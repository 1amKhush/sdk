import { z } from 'zod';

export const paymentAmountSchema = z
  .number()
  .finite()
  .nonnegative()
  .max(Number.MAX_SAFE_INTEGER);
export const paymentPmiSchema = z.string().regex(/^[a-z0-9-]+$/);

/** Runtime offer schema; unknown extension metadata is preserved. */
export const paymentOptionSchema = z.object({
  amount: paymentAmountSchema,
  pmi: paymentPmiSchema,
  pay_req: z
    .string()
    .min(1)
    .refine((value) => value.trim().length > 0),
  description: z.string().optional(),
  ttl: z
    .number()
    .finite()
    .positive()
    .max(Number.MAX_SAFE_INTEGER / 1000)
    .optional(),
  _meta: z.record(z.string(), z.unknown()).optional(),
});
