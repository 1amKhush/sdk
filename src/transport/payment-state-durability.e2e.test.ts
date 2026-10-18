import { expect, test } from 'bun:test';
import { Pool } from 'pg';
import { Client } from '@contextvm/mcp-sdk/client';
import { McpServer } from '@contextvm/mcp-sdk/server/mcp';
import { generateSecretKey, getPublicKey } from 'nostr-tools/pure';
import { bytesToHex } from 'nostr-tools/utils';
import { z } from 'zod';
import type { NostrEvent } from 'nostr-tools';
import { withTimeout } from '../core/utils/utils.js';
import { MockRelayHub } from '../__mocks__/mock-relay-handler.js';
import { EncryptionMode, GiftWrapMode } from '../core/interfaces.js';
import { PrivateKeySigner } from '../signer/private-key-signer.js';
import { NostrClientTransport } from './nostr-client-transport.js';
import { NostrServerTransport } from './nostr-server-transport.js';
import { withClientPayments, withServerPayments } from '../payments/index.js';
import { PostgresPaymentStateStore } from '../payments/postgres/index.js';
import type {
  PaymentProcessor,
  PaymentStateRecord,
} from '../payments/index.js';

const connectionString = process.env.PAYMENT_TEST_DATABASE_URL;
const databaseTest = connectionString ? test : test.skip;
const modes = [
  {
    name: 'cleartext',
    encryptionMode: EncryptionMode.DISABLED,
    giftWrapMode: GiftWrapMode.OPTIONAL,
  },
  {
    name: 'persistent encryption',
    encryptionMode: EncryptionMode.REQUIRED,
    giftWrapMode: GiftWrapMode.PERSISTENT,
  },
  {
    name: 'ephemeral encryption',
    encryptionMode: EncryptionMode.REQUIRED,
    giftWrapMode: GiftWrapMode.EPHEMERAL,
  },
];

for (const mode of modes)
  databaseTest(
    `durable transparent payments: two workers and replay after TTL/restart over ${mode.name}`,
    async () => {
      const pool = new Pool({
        connectionString,
        max: 4,
        connectionTimeoutMillis: 3000,
        query_timeout: 5000,
      });
      const namespace = `e2e-${crypto.randomUUID()}`;
      const hub = new MockRelayHub();
      const serverKey = generateSecretKey();
      const clientKey = generateSecretKey();
      const clientPubkey = getPublicKey(clientKey);
      let invoices = 0;
      let approvals = 0;
      let verifications = 0;
      let executions = 0;
      let admittedRecord: PaymentStateRecord | undefined;
      const processor: PaymentProcessor = {
        pmi: 'fake',
        currencyUnit: 'sats',
        async createPaymentRequired(input) {
          invoices++;
          return {
            amount: input.amount,
            pmi: 'fake',
            pay_req: `invoice-${invoices}`,
            ttl: 2,
          };
        },
        async verifyPayment() {
          verifications++;
          return {};
        },
      };
      const servers: McpServer[] = [];
      const createWorker = async (
        onReplay?: () => void,
      ): Promise<NostrServerTransport> => {
        const store = new PostgresPaymentStateStore({ pool, namespace });
        await store.initialize();
        const admit = store.admit.bind(store);
        store.admit = async (input) => {
          const result = await admit(input);
          if (result.status === 'reserved') admittedRecord = result.record;
          if (result.status === 'duplicate') onReplay?.();
          return result;
        };
        const server = new McpServer({
          name: 'durable-worker',
          version: '1.0.0',
        });
        server.registerTool(
          'paid',
          { inputSchema: { value: z.number() } },
          async ({ value }) => {
            executions++;
            return { content: [{ type: 'text', text: String(value) }] };
          },
        );
        servers.push(server);
        const transport = withServerPayments(
          new NostrServerTransport({
            signer: new PrivateKeySigner(bytesToHex(serverKey)),
            relayHandler: hub.createRelayHandler(),
            encryptionMode: mode.encryptionMode,
            giftWrapMode: mode.giftWrapMode,
          }),
          {
            stateStore: store,
            paymentTtlMs: 2000,
            processors: [processor],
            pricedCapabilities: [
              {
                method: 'tools/call',
                name: 'paid',
                amount: 5,
                currencyUnit: 'sats',
              },
            ],
          },
        );
        await server.connect(transport);
        return transport;
      };
      const clientEvents: NostrEvent[] = [];
      const clientRelay = hub.createRelayHandler();
      const publish = clientRelay.publish.bind(clientRelay);
      clientRelay.publish = async (event, options) => {
        clientEvents.push(event);
        await publish(event, options);
      };
      const client = new Client({
        name: 'durable-payment-client',
        version: '1.0.0',
      });
      try {
        await createWorker();
        await createWorker();
        const base = new NostrClientTransport({
          signer: new PrivateKeySigner(bytesToHex(clientKey)),
          relayHandler: clientRelay,
          serverPubkey: getPublicKey(serverKey),
          encryptionMode: mode.encryptionMode,
          giftWrapMode: mode.giftWrapMode,
        });
        await client.connect(
          withClientPayments(base, {
            handlers: [
              {
                pmi: 'fake',
                async handle() {
                  approvals++;
                },
              },
            ],
          }),
        );
        const result = await client.callTool(
          { name: 'paid', arguments: { value: 7 } },
          undefined,
          { timeout: 5000 },
        );
        expect(result.content).toEqual([{ type: 'text', text: '7' }]);
        expect([invoices, approvals, verifications, executions]).toEqual([
          1, 1, 1, 1,
        ]);
        expect(admittedRecord?.clientPubkey).toBe(clientPubkey);
        await Promise.all(servers.splice(0).map((server) => server.close()));
        await Bun.sleep(
          Math.max(0, admittedRecord!.leaseExpiresAtMs - Date.now() + 25),
        );
        let replayed!: () => void;
        const replay = new Promise<void>((resolve) => {
          replayed = resolve;
        });
        const restartedTransport = await createWorker(replayed);
        // Deliver a relay replay directly into the real verification/decryption pipeline,
        // independently of the new subscription's narrower `since` filter.
        for (const event of clientEvents)
          await restartedTransport['processIncomingEvent'](event);
        await withTimeout(replay, 3000, 'Retained request was not replayed');
        expect([invoices, approvals, verifications, executions]).toEqual([
          1, 1, 1, 1,
        ]);
      } finally {
        await client.close();
        await Promise.all(servers.map((server) => server.close()));
        await pool.end();
      }
    },
  );
