/** Initial durable payment schema; initialization runs under a migration lock. */
export const PAYMENT_STATE_SCHEMA_VERSION = 2;

/** PostgreSQL schema shared by all payment namespaces. */
export const PAYMENT_STATE_MIGRATION = `
CREATE TABLE IF NOT EXISTS contextvm_payment_namespaces (
  namespace text PRIMARY KEY,
  version integer NOT NULL,
  policy jsonb NOT NULL,
  clock_ms bigint NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS contextvm_payment_records (
  namespace text NOT NULL REFERENCES contextvm_payment_namespaces(namespace),
  key text NOT NULL,
  phase text NOT NULL,
  lease_expires_ms bigint NOT NULL,
  retain_until_ms bigint,
  record jsonb NOT NULL,
  PRIMARY KEY (namespace, key)
);
CREATE INDEX IF NOT EXISTS contextvm_payment_retention
  ON contextvm_payment_records(namespace, retain_until_ms);
`;
