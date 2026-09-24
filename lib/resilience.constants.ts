export const RETRY_METADATA = 'resilience:retry';
export const TIMEOUT_METADATA = 'resilience:timeout';
export const CIRCUIT_BREAKER_METADATA = 'resilience:circuit-breaker';
export const BULKHEAD_METADATA = 'resilience:bulkhead';
export const FALLBACK_METADATA = 'resilience:fallback';
export const PRESET_METADATA = 'resilience:preset';

export const RESILIENCE_METADATA_KEYS = [
  RETRY_METADATA,
  TIMEOUT_METADATA,
  CIRCUIT_BREAKER_METADATA,
  BULKHEAD_METADATA,
  FALLBACK_METADATA,
  PRESET_METADATA,
] as const;
