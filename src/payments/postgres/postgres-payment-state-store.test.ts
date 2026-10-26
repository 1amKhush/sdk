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
): ReturnType<typeof Bun.spawn> {
  return Bun.spawn(
    [
      process.execPath,
      'src/__mocks__/payment-state-worker.ts',
      action,
      namespace,
    ],
    {
      env: { ...process.env, PAYMENT_TEST_DATABASE_URL: connectionString },
      stdout: 'pipe',
      stderr: 'pipe',
    },
  );
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
            const text = await new Response(
              child.stdout as ReadableStream<Uint8Array>,
            ).text();
            const error = await new Response(
              child.stderr as ReadableStream<Uint8Array>,
            ).text();
            expect(await child.exited, error).toBe(0);
            return JSON.parse(text) as { status: string };
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
          const reader = (
            child.stdout as ReadableStream<Uint8Array>
          ).getReader();
          const chunk = await reader.read();
          const record = JSON.parse(
            new TextDecoder().decode(chunk.value),
          ) as PaymentStateRecord;
          reader.releaseLock();
          child.kill('SIGKILL');
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
