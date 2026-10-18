import type { Pool, PoolClient } from 'pg';
import type {
  PaymentStateAdmission,
  PaymentStateAdmissionResult,
  PaymentStatePolicy,
  PaymentStateRecord,
  PaymentStateStore,
  PaymentStateTransition,
} from '../payment-state-store.js';
import {
  createPaymentStatePolicy,
  paymentEventKey,
} from '../payment-state-store.js';
import {
  admitPaymentState,
  transitionPaymentState,
  type PaymentStateChange,
} from '../payment-state-machine.js';
import {
  PAYMENT_STATE_MIGRATION,
  PAYMENT_STATE_SCHEMA_VERSION,
} from './migrations.js';

/** PostgreSQL deployment options. The application retains ownership of its pool. */
export interface PostgresPaymentStateStoreOptions extends Partial<PaymentStatePolicy> {
  pool: Pool;
  /** One namespace per server identity/payment policy; shared by all its workers. */
  namespace: string;
}

/**
 * Durable, cross-worker payment state. Short transactions serialize admission
 * within one namespace; provider I/O and handlers never run while holding locks.
 * Call initialize() before attaching this store. Failed/ambiguous transactions
 * propagate without replaying a command or falling back to memory.
 *
 * @example
 * import { Pool } from 'pg';
 * import { PostgresPaymentStateStore } from '@contextvm/sdk/payments/postgres';
 * const pool = new Pool({ connectionString });
 * const store = new PostgresPaymentStateStore({ pool, namespace: serverPubkey });
 * await store.initialize();
 * withServerPayments(transport, { processors, pricedCapabilities, stateStore: store });
 * // The application closes its pool after every worker has stopped using it.
 */
export class PostgresPaymentStateStore implements PaymentStateStore {
  public readonly policy: PaymentStatePolicy;
  private readonly pool: Pool;
  private readonly namespace: string;
  private initialized = false;

  public constructor(options: PostgresPaymentStateStoreOptions) {
    if (!options.namespace || options.namespace.length > 200)
      throw new TypeError('Payment namespace must contain 1–200 characters');
    this.pool = options.pool;
    this.namespace = options.namespace;
    this.policy = createPaymentStatePolicy(options);
  }

  /** Applies the versioned schema and rejects inconsistent worker policies. */
  public async initialize(): Promise<void> {
    await this.transaction(async (client) => {
      await client.query('SELECT pg_advisory_xact_lock(193608, 8)');
      const schema = await client.query<{ existing: string | null }>(
        "SELECT to_regclass('contextvm_payment_namespaces')::text AS existing",
      );
      const existing = schema.rows[0];
      if (!existing) throw new Error('Payment schema lookup failed');
      if (!existing.existing) await client.query(PAYMENT_STATE_MIGRATION);
      else {
        const clock = await client.query<{ present: boolean }>(
          `SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE
           attrelid = 'contextvm_payment_namespaces'::regclass AND attname = 'clock_ms' AND NOT attisdropped) AS present`,
        );
        if (!clock.rows[0]?.present)
          await client.query(
            'ALTER TABLE contextvm_payment_namespaces ADD COLUMN clock_ms bigint NOT NULL DEFAULT 0',
          );
      }
    }, false);
    // Release DDL locks before taking a namespace row lock. Repeating schema
    // DDL while holding that row can deadlock with a worker writing records.
    await this.transaction(async (client) => {
      await client.query(
        'INSERT INTO contextvm_payment_namespaces(namespace, version, policy) VALUES ($1, $2, $3::jsonb) ON CONFLICT DO NOTHING',
        [
          this.namespace,
          PAYMENT_STATE_SCHEMA_VERSION,
          JSON.stringify(this.policy),
        ],
      );
      await this.lockNamespace(client);
    }, false);
    this.initialized = true;
  }

  /** Atomic claim-or-reserve, replay admission, capacity check, and retention. */
  public async admit(
    input: PaymentStateAdmission,
  ): Promise<PaymentStateAdmissionResult> {
    input = structuredClone(input);
    return this.transaction(async (client) => {
      const { now, generation } = await this.lockNamespace(client);
      await client.query(
        `DELETE FROM contextvm_payment_records WHERE namespace = $1 AND retain_until_ms <= $2
         AND NOT (phase IN ('reserved', 'issuing', 'pending') AND lease_expires_ms > $2)`,
        [this.namespace, now],
      );
      const keys = [input.key];
      if (input.mode === 'explicit_gating')
        keys.push(paymentEventKey(input.clientPubkey, input.requestEventId));
      const { rows } = await client.query<{
        key: string;
        record: PaymentStateRecord;
      }>(
        `SELECT key, record FROM contextvm_payment_records WHERE namespace = $1 AND
           (key = ANY($2::text[]) OR (record->>'clientPubkey' = $3 AND record->'eventIds' ? $4))`,
        [this.namespace, keys, input.clientPubkey, input.requestEventId],
      );
      const counts = await client.query<{ total: string; pending: string }>(
        `SELECT count(*) AS total,
         count(*) FILTER (WHERE phase IN ('reserved', 'issuing', 'pending') AND lease_expires_ms > $2) AS pending
         FROM contextvm_payment_records WHERE namespace = $1`,
        [this.namespace, now],
      );
      const usage = counts.rows[0];
      if (!usage) throw new Error('Payment capacity lookup failed');
      const change = admitPaymentState(
        input,
        {
          record: rows.find((row) => row.key === input.key)?.record,
          execution: rows.find((row) => row.key !== input.key)?.record,
          total: Number(usage.total),
          pending: Number(usage.pending),
        },
        this.policy,
        now,
        generation,
      );
      await this.write(client, change);
      return change.result;
    });
  }

  /** Persists only transitions belonging to the current live attempt. */
  public async transition(input: PaymentStateTransition): Promise<boolean> {
    input = structuredClone(input);
    return this.transaction(async (client) => {
      const { now } = await this.lockNamespace(client);
      const { rows } = await client.query<{ record: PaymentStateRecord }>(
        'SELECT record FROM contextvm_payment_records WHERE namespace = $1 AND key = $2',
        [this.namespace, input.key],
      );
      const change = transitionPaymentState(
        input,
        rows[0]?.record,
        now,
        this.policy,
      );
      await this.write(client, change);
      return change.result;
    });
  }

  /** Reads persisted recovery state; an unavailable database is an error. */
  public async read(key: string): Promise<PaymentStateRecord | undefined> {
    return this.transaction(async (client) => {
      await this.lockNamespace(client);
      const { rows } = await client.query<{ record: PaymentStateRecord }>(
        'SELECT record FROM contextvm_payment_records WHERE namespace = $1 AND key = $2',
        [this.namespace, key],
      );
      return rows[0]?.record;
    });
  }

  private async lockNamespace(
    client: PoolClient,
  ): Promise<{ now: number; generation: string }> {
    const { rows } = await client.query<{
      version: number;
      policy: PaymentStatePolicy;
    }>(
      'SELECT version, policy FROM contextvm_payment_namespaces WHERE namespace = $1 FOR UPDATE',
      [this.namespace],
    );
    const namespace = rows[0];
    if (
      !namespace ||
      namespace.version !== PAYMENT_STATE_SCHEMA_VERSION ||
      Object.entries(this.policy).some(
        ([key, value]) =>
          namespace.policy[key as keyof PaymentStatePolicy] !== value,
      )
    )
      throw new Error('Payment namespace schema or policy mismatch');
    const clock = await client.query<{ now: string; generation: string }>(
      `UPDATE contextvm_payment_namespaces SET clock_ms = GREATEST(clock_ms,
       floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint)
       WHERE namespace = $1 RETURNING clock_ms AS now, gen_random_uuid()::text AS generation`,
      [this.namespace],
    );
    const now = Number(clock.rows[0]?.now);
    if (!Number.isSafeInteger(now))
      throw new Error('Payment database clock lookup failed');
    const generation = clock.rows[0]?.generation;
    if (!generation) throw new Error('Payment generation allocation failed');
    return { now, generation };
  }

  private async write<T>(
    client: PoolClient,
    change: PaymentStateChange<T>,
  ): Promise<void> {
    for (const key of change.remove)
      await client.query(
        'DELETE FROM contextvm_payment_records WHERE namespace = $1 AND key = $2',
        [this.namespace, key],
      );
    for (const record of change.put)
      await client.query(
        `INSERT INTO contextvm_payment_records(namespace, key, phase, lease_expires_ms, retain_until_ms, record)
       VALUES ($1, $2, $3, $4, $5, $6::jsonb) ON CONFLICT(namespace, key) DO UPDATE SET
       phase = EXCLUDED.phase, lease_expires_ms = EXCLUDED.lease_expires_ms,
       retain_until_ms = EXCLUDED.retain_until_ms, record = EXCLUDED.record`,
        [
          this.namespace,
          record.key,
          record.phase,
          record.leaseExpiresAtMs,
          record.retainUntilMs,
          JSON.stringify(record),
        ],
      );
  }

  private async transaction<T>(
    run: (client: PoolClient) => Promise<T>,
    requireInitialized = true,
  ): Promise<T> {
    if (requireInitialized && !this.initialized)
      throw new Error('Initialize the PostgreSQL payment store first');
    const client = await this.pool.connect();
    let failed = false;
    try {
      await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');
      const result = await run(client);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      failed = true;
      try {
        await client.query('ROLLBACK');
      } catch {
        /* Preserve the original, potentially ambiguous error. */
      }
      throw error;
    } finally {
      client.release(failed);
    }
  }
}
