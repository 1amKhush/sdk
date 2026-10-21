import { withTimeout } from '../../core/utils/utils.js';
import { describe, expect, test } from 'bun:test';
import { Pool, type PoolClient } from 'pg';
import { PostgresPaymentStateStore } from './postgres-payment-state-store.js';
import {
  admission,
  bind,
  paymentStoreContract,
} from '../../__mocks__/payment-store-contract.js';
import type { PaymentStateRecord } from '../payment-state-store.js';

const connectionString = process.env.PAYMENT_TEST_DATABASE_URL;
const databaseTest = connectionString ? test : test.skip;
const databaseDescribe = connectionString ? describe : describe.skip;

function createPool(): Pool {
  return new Pool({
    connectionString,
    max: 2,
    connectionTimeoutMillis: 3000,
    query_timeout: 5000,
  });
}

if (connectionString)
  paymentStoreContract('PostgreSQL atomic payment store', async (policy) => {
    const namespace = `contract-${crypto.randomUUID()}`;
    const pool = createPool();
    const otherPool = createPool();
    const store = new PostgresPaymentStateStore({ pool, namespace, ...policy });
    const peer = new PostgresPaymentStateStore({
      pool: otherPool,
      namespace,
      ...policy,
    });
    try {
      await store.initialize();
      await peer.initialize();
    } catch (error) {
      await Promise.all([pool.end(), otherPool.end()]);
      throw error;
    }
    return {
      store,
      peer,
      advance: async (ms) => {
        await Bun.sleep(ms);
      },
      close: async () => {
        await Promise.all([pool.end(), otherPool.end()]);
      },
    };
  });

async function withDatabase(
  run: (
    store: PostgresPaymentStateStore,
    pool: Pool,
    namespace: string,
  ) => Promise<void>,
): Promise<void> {
  const pool = createPool();
  const namespace = `integration-${crypto.randomUUID()}`;
  const store = new PostgresPaymentStateStore({ pool, namespace });
  try {
    await store.initialize();
    await run(store, pool, namespace);
  } finally {
    await pool.end();
  }
}

function worker(
  action: string,
  namespace: string,
): {
  ready: Promise<PaymentStateRecord | { status: string }>;
  exited: Promise<number | null>;
  kill: () => void;
} {
  let receive!: (value: PaymentStateRecord | { status: string }) => void;
  let exit!: (code: number | null) => void;
  const ready = new Promise<PaymentStateRecord | { status: string }>(
    (resolve) => {
      receive = resolve;
    },
  );
  const exited = new Promise<number | null>((resolve) => {
    exit = resolve;
  });
  const child = Bun.spawn(
    [
      process.execPath,
      'src/__mocks__/payment-state-worker.ts',
      action,
      namespace,
    ],
    {
      env: { ...process.env, PAYMENT_TEST_DATABASE_URL: connectionString },
      stdin: 'ignore',
      stdout: 'ignore',
      stderr: 'inherit',
      // IPC avoids unread pipe lifetimes interfering with Bun's exit notification.
      ipc: (message) =>
        receive(message as PaymentStateRecord | { status: string }),
      onExit: (_child, code) => {
        exit(code);
      },
      timeout: 10000,
      killSignal: 'SIGKILL',
    },
  );
  return {
    ready: withTimeout(ready, 10000, 'Payment worker did not report readiness'),
    exited: withTimeout(exited, 10000, 'Payment worker did not exit'),
    kill: () => {
      child.kill('SIGKILL');
    },
  };
}

databaseDescribe('PostgreSQL recovery and failure boundaries', () => {
  databaseTest(
    'the admission clock floor survives reconnects and prevents old-event readmission',
    async () =>
      withDatabase(async (store, pool, namespace) => {
        const createdAt = Math.floor(Date.now() / 1000);
        const floor = Date.now() + store.policy.replayWindowMs + 1000;
        await pool.query(
          'UPDATE contextvm_payment_namespaces SET clock_ms = $2 WHERE namespace = $1',
          [namespace, floor],
        );
        const restarted = new PostgresPaymentStateStore({ pool, namespace });
        await restarted.initialize();
        const input = admission('transparent');
        expect(
          (
            await restarted.admit({
              ...input,
              event: {
                id: input.requestEventId,
                author: input.clientPubkey,
                createdAt,
              },
            })
          ).status,
        ).toBe('stale');
      }),
  );

  databaseTest(
    'two separate worker processes have one grant consumer',
    async () =>
      withDatabase(async (store, _pool, namespace) => {
        const input = admission();
        const record = await bind(store, input);
        await store.transition({
          key: input.key,
          generation: record.generation,
          type: 'grant',
          ttlMs: 10000,
        });
        const children = [
          worker('claim', namespace),
          worker('claim', namespace),
        ];
        const outputs = await Promise.all(
          children.map(async (child) => {
            const result = await child.ready;
            expect(await child.exited).toBe(0);
            if (!('status' in result))
              throw new Error('Claim worker returned no admission status');
            return result;
          }),
        );
        expect(outputs.map((result) => result.status).sort()).toEqual([
          'claimed',
          'duplicate',
        ]);
      }),
  );

  databaseTest(
    'killing the invoice worker and reconnecting preserves its binding and replay tombstone',
    async () =>
      withDatabase(async (store, _pool, namespace) => {
        const child = worker('invoice', namespace);
        try {
          const record = await child.ready;
          if (!('key' in record))
            throw new Error('Invoice worker returned no payment record');
          child.kill();
          await child.exited;
          const pool = createPool();
          try {
            const restarted = new PostgresPaymentStateStore({
              pool,
              namespace,
            });
            await restarted.initialize();
            expect((await restarted.read(record.key))?.invoice).toEqual(
              record.invoice,
            );
            expect(
              (await restarted.admit(admission('transparent'))).status,
            ).toBe('duplicate');
            expect((await store.read(record.key))?.generation).toBe(
              record.generation,
            );
          } finally {
            await pool.end();
          }
        } finally {
          child.kill();
          await child.exited;
        }
      }),
  );

  databaseTest(
    'a new store recovers a paid grant and preserves the consumed result',
    async () =>
      withDatabase(async (store, _pool, namespace) => {
        const input = admission();
        const record = await bind(store, input);
        await store.transition({
          key: input.key,
          generation: record.generation,
          type: 'grant',
          ttlMs: 10000,
        });
        const pool = createPool();
        try {
          const restarted = new PostgresPaymentStateStore({ pool, namespace });
          await restarted.initialize();
          const retry = { ...input, requestEventId: 'retry-after-restart' };
          expect((await restarted.admit(retry)).status).toBe('claimed');
          expect((await store.admit(retry)).status).toBe('duplicate');
        } finally {
          await pool.end();
        }
      }),
  );

  databaseTest('refuses mismatched policies and schema versions', async () =>
    withDatabase(async (_store, pool, namespace) => {
      const mismatch = new PostgresPaymentStateStore({
        pool,
        namespace,
        maxPendingPayments: 1,
      });
      await expect(mismatch.initialize()).rejects.toThrow('mismatch');
      await pool.query(
        'UPDATE contextvm_payment_namespaces SET version = 999 WHERE namespace = $1',
        [namespace],
      );
      const future = new PostgresPaymentStateStore({ pool, namespace });
      await expect(future.initialize()).rejects.toThrow('mismatch');
    }),
  );

  databaseTest(
    'an unavailable database propagates instead of using empty memory state',
    async () => {
      const pool = createPool();
      const store = new PostgresPaymentStateStore({
        pool,
        namespace: `closed-${crypto.randomUUID()}`,
      });
      await store.initialize();
      await pool.end();
      await expect(store.admit(admission())).rejects.toThrow();
      await expect(store.read(admission().key)).rejects.toThrow();
    },
  );

  databaseTest(
    'lost commit acknowledgement leaves admission reserved and does not replay it',
    async () =>
      withDatabase(async (store, pool, namespace) => {
        const fault = acknowledgementFault(pool);
        const uncertain = new PostgresPaymentStateStore({
          pool: fault.pool,
          namespace,
        });
        await uncertain.initialize();
        fault.arm();
        await expect(uncertain.admit(admission())).rejects.toThrow(
          'commit acknowledgement lost',
        );
        expect((await store.admit(admission())).status).toBe('pending');
      }),
  );

  databaseTest(
    'lost grant acknowledgement cannot clear a committed paid grant',
    async () =>
      withDatabase(async (store, pool, namespace) => {
        const input = admission();
        const record = await bind(store, input);
        const fault = acknowledgementFault(pool);
        const uncertain = new PostgresPaymentStateStore({
          pool: fault.pool,
          namespace,
        });
        await uncertain.initialize();
        fault.arm();
        const fence = { key: input.key, generation: record.generation };
        await expect(
          uncertain.transition({ ...fence, type: 'grant', ttlMs: 10000 }),
        ).rejects.toThrow('commit acknowledgement lost');
        expect(await store.transition({ ...fence, type: 'expire' })).toBe(
          false,
        );
        expect(
          (await store.admit({ ...input, requestEventId: 'retry' })).status,
        ).toBe('claimed');
      }),
  );
});

function acknowledgementFault(pool: Pool): { pool: Pool; arm: () => void } {
  let armed = false;
  return {
    arm: () => {
      armed = true;
    },
    pool: {
      async connect(): Promise<PoolClient> {
        const client = await pool.connect();
        return {
          query: async (query: string, values?: unknown[]) => {
            const result = await client.query(query, values);
            if (query === 'COMMIT' && armed) {
              armed = false;
              throw new Error('commit acknowledgement lost');
            }
            return result;
          },
          release: (failed?: boolean) => client.release(failed),
        } as PoolClient;
      },
    } as Pool,
  };
}

databaseTest(
  'schema 1 upgrade preserves unresolved invoices and fences old workers',
  async () =>
    withDatabase(async (store, pool, namespace) => {
      const input = admission();
      const record = await bind(store, input);
      await pool.query(
        'UPDATE contextvm_payment_namespaces SET version = 1 WHERE namespace = $1',
        [namespace],
      );
      await pool.query(
        "UPDATE contextvm_payment_records SET retain_until_ms = 1, record = jsonb_set(record, '{retainUntilMs}', '1'::jsonb) WHERE namespace = $1",
        [namespace],
      );
      const restarted = new PostgresPaymentStateStore({ pool, namespace });
      await restarted.initialize();
      const restored = await restarted.read(record.key);
      expect(restored?.invoice).toEqual(record.invoice);
      expect(restored?.retainUntilMs).toBeNull();
      const version = await pool.query(
        'SELECT version FROM contextvm_payment_namespaces WHERE namespace = $1',
        [namespace],
      );
      expect(version.rows[0].version).toBe(2);
    }),
);

databaseTest(
  'lost result commit acknowledgement preserves the payload and one publication claim after reconnect',
  async () =>
    withDatabase(async (store, pool, namespace) => {
      const { PaymentResultRecovery } = await import('../payment-recovery.js');
      const input = admission('transparent');
      const record = await bind(store, input);
      const fence = { key: record.key, generation: record.generation };
      await store.transition({ ...fence, type: 'settled', ttlMs: 10000 });
      await store.transition({ ...fence, type: 'consume' });
      const fault = acknowledgementFault(pool);
      const uncertain = new PostgresPaymentStateStore({
        pool: fault.pool,
        namespace,
      });
      await uncertain.initialize();
      const recovery = new PaymentResultRecovery(uncertain, {
        publicationLeaseMs: 10000,
      });
      const response = {
        jsonrpc: '2.0' as const,
        id: input.requestEventId,
        result: { value: 7 },
      };
      const snapshot = {
        route: { clientPubkey: input.clientPubkey, originalRequestId: 42 },
        expiresAtMs: Number.MAX_SAFE_INTEGER,
      };
      fault.arm();
      await expect(
        recovery.retain(input.requestEventId, response, snapshot),
      ).rejects.toThrow('commit acknowledgement lost');
      const restarted = new PaymentResultRecovery(store, {
        publicationLeaseMs: 10000,
      });
      const claims = await Promise.allSettled([
        restarted.prepare(input.requestEventId, response, snapshot),
        restarted.prepare(input.requestEventId, response, snapshot),
      ]);
      expect(
        claims.filter((result) => result.status === 'fulfilled'),
      ).toHaveLength(1);
      const winner = claims.find((result) => result.status === 'fulfilled');
      if (winner?.status !== 'fulfilled')
        throw new Error('No publication claimant');
      await winner.value.framePublished(1);
      await winner.value.complete();
      expect((await store.read(input.key))?.delivery?.status).toBe('published');
    }),
);
