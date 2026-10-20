import { PaymentResultRecovery } from './payment-recovery.js';
import { ObservedPaymentStateStore } from './observed-payment-state-store.js';
import type { NostrServerTransport } from '../transport/nostr-server-transport.js';
import type { PaymentInteractionPolicy } from './types.js';
import type { ServerPaymentsOptions } from './server-payments.js';
import { createCapTagsFromPricedCapabilities } from './cap-tags.js';
import { createPmiTagsFromProcessors } from './pmi-tags.js';
import { createServerPaymentsMiddleware } from './server-payments.js';
import { createExplicitGatingMiddleware } from './server-explicit-gating.js';
import { serverPaymentStore } from './server-payment-state.js';
import { validateServerPaymentOptions } from './server-payment-validation.js';
import { buildProcessorsByPmi } from './server-payments-utils.js';
import { createLogger } from '../core/utils/logger.js';
import { NOSTR_TAGS } from '../core/constants.js';

/**
 * Attaches CEP-8 payments gating to a NostrServerTransport.
 *
 * By default the server uses the `optional` policy: it advertises
 * `explicit_gating` support and mirrors each client's requested lifecycle, so a
 * client that requests `explicit_gating` is gated while transparent clients keep
 * the notification-based flow. Pass `paymentInteraction: 'transparent'` for a
 * transparent-only server.
 */
// Transports that already have the payments lifecycle registered. The dedup
// closures live in per-call middleware instances, so a second registration
// silently double-charges every priced request (two invoices, two forwards).
// ponytail: WeakSet keyed by transport; promote to an instance flag if a
// legitimate multi-registration use case appears.
const paymentsRegistered = new WeakSet<object>();

export function withServerPayments(
  transport: NostrServerTransport,
  options: ServerPaymentsOptions,
): NostrServerTransport {
  if (paymentsRegistered.has(transport)) {
    throw new Error(
      'withServerPayments already called on this transport; registering the payments lifecycle twice double-charges priced requests',
    );
  }
  validateServerPaymentOptions(options);
  const backend = serverPaymentStore(options);
  const stateStore =
    options.onPaymentOutcome || options.paymentJournal
      ? new ObservedPaymentStateStore(backend, options)
      : backend;
  transport.setPaymentResultRecovery(
    new PaymentResultRecovery(stateStore, {
      ...options,
      publicationLeaseMs: options.paymentTtlMs ?? 300_000,
    }),
  );
  options = { ...options, stateStore };
  paymentsRegistered.add(transport);

  // Build the PMI → processor map once and share it across both middlewares.
  const processorsByPmi = buildProcessorsByPmi(
    options.processors,
    createLogger('server-payments'),
  );

  const policy: PaymentInteractionPolicy =
    options.paymentInteraction ?? 'optional';
  const supportsExplicitGating = policy === 'optional';

  // CEP-8 discovery tags: advertise supported PMIs + reference pricing on
  // announcement/list events. When explicit gating is supported, also advertise
  // it as an available opt-in mode (availability, not effective session mode).
  const extraTags: string[][] = createPmiTagsFromProcessors(options.processors);

  if (supportsExplicitGating) {
    extraTags.push([NOSTR_TAGS.PAYMENT_INTERACTION, 'explicit_gating']);
  }

  transport.setAnnouncementExtraTags(extraTags);
  transport.setAnnouncementPricingTags(
    createCapTagsFromPricedCapabilities(options.pricedCapabilities),
  );

  // Expose the configured policy to the transport coordinator so it can accept
  // or reject per-session `payment_interaction` requests.
  transport.setSupportedPaymentInteraction(policy);

  // Transport-owned shutdown signal (aborted at the top of close()). Both
  // middlewares stop polling and skip forward/grant once it fires. Not chained
  // through `onclose`, which consumers may reassign after registration.
  const abortSignal = transport.closeSignal;
  if (options.disposeProcessorsOnClose) {
    transport.addCloseHandler(async () => {
      const results = await Promise.allSettled(
        [...new Set(options.processors)].map((p) =>
          Promise.resolve().then(() => p.dispose?.()),
        ),
      );
      if (results.some((result) => result.status === 'rejected'))
        throw new Error('Payment processor disposal failed');
    });
  }

  transport.addInboundMiddleware(
    createServerPaymentsMiddleware({
      sender: transport,
      onPaymentPending: (id) =>
        transport.capturePaymentRouteSnapshot(
          id,
          Number.MAX_SAFE_INTEGER - Date.now(),
        ),
      options,
      processorsByPmi,
      // Snapshot the correlation route when an invoice goes out so the paid
      // response survives route-pop (duplicate cleanup) and session eviction.
      onInvoiceIssued: ({ requestEventId }) =>
        transport.capturePaymentRouteSnapshot(
          requestEventId,
          Number.MAX_SAFE_INTEGER - Date.now(),
        ),
      onPaidDuplicate: (record) =>
        transport.retryPaymentResult(
          record.clientPubkey,
          record.requestEventId,
        ),
      abortSignal,
    }),
  );

  // The transparent middleware self-gates on the per-session effective mode, so
  // it is safe to register the explicit-gating middleware alongside it. Each
  // request is routed to exactly one lifecycle based on the negotiated mode.
  if (supportsExplicitGating) {
    const authorizationStore = stateStore;
    transport.addInboundMiddleware(
      createExplicitGatingMiddleware({
        options,
        authorizationStore,
        onPaidDuplicate: (record) =>
          transport.retryPaymentResult(
            record.clientPubkey,
            record.requestEventId,
          ),
        onPaymentPending: (id) =>
          transport.capturePaymentRouteSnapshot(
            id,
            Number.MAX_SAFE_INTEGER - Date.now(),
          ),
        sendResponse: async (clientPubkey, response, requestEventId) => {
          await transport.sendTargetedResponse(
            clientPubkey,
            response,
            requestEventId,
          );
        },
        processorsByPmi,
        onInvoiceIssued: ({ requestEventId }) =>
          transport.capturePaymentRouteSnapshot(
            requestEventId,
            Number.MAX_SAFE_INTEGER - Date.now(),
          ),
        abortSignal,
      }),
    );
  }
  return transport;
}
