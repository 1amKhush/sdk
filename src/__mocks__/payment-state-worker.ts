import { Pool } from 'pg';
import { PostgresPaymentStateStore } from '../payments/postgres/index.js';
import {
  paymentAuthorizationKey,
  paymentEventKey,
} from '../payments/payment-state-store.js';

const [action, namespace] = process.argv.slice(2);
const pool = new Pool({
  connectionString: process.env.PAYMENT_TEST_DATABASE_URL,
  max: 1,
});
const input = {
  key:
    action === 'claim'
      ? paymentAuthorizationKey('client', 'hash')
      : paymentEventKey('client', 'event-1'),
  mode:
    action === 'claim'
      ? ('explicit_gating' as const)
      : ('transparent' as const),
  clientPubkey: 'client',
  invocationHash: 'hash',
  requestEventId: action === 'claim' ? 'worker-retry' : 'event-1',
  leaseMs: 10000,
};
const store = new PostgresPaymentStateStore({ pool, namespace });
await store.initialize();
if (action === 'claim') {
  console.log(JSON.stringify(await store.admit(input)));
  await pool.end();
} else if (action === 'invoice') {
  const result = await store.admit(input);
  if (result.status !== 'reserved')
    throw new Error('Worker admission was refused');
  const record = result.record;
  const fence = { key: record.key, generation: record.generation };
  await store.transition({ ...fence, type: 'issuing' });
  await store.transition({
    ...fence,
    type: 'bind',
    invoice: { pmi: 'fake', amount: 10, pay_req: 'persisted-worker-invoice' },
    leaseMs: 10000,
  });
  console.log(JSON.stringify(await store.read(record.key)));
  setInterval(() => {}, 1000);
} else {
  throw new Error('Unknown payment test worker action');
}
