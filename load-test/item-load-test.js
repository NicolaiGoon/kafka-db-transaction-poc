import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

// Targets one endpoint per run (see run-comparison.ps1, which runs it once per implementation).
const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const ENDPOINT = __ENV.ENDPOINT || '/items/pooled';
const VUS = parseInt(__ENV.VUS || '20', 10);
const DURATION = __ENV.DURATION || '30s';
// Fraction of requests sent with an invalid payload (missing the NOT NULL "name"). These force a
// DB insert failure, so the chained DB+Kafka transaction must roll back, leaving NO row and NO
// event. Used to prove rollback: db count == kafka count == 201s, regardless of how many fail.
const INVALID_RATIO = parseFloat(__ENV.INVALID_RATIO || '0');

const itemsCreated = new Counter('items_created'); // HTTP 201
const itemsFailed = new Counter('items_failed');   // anything else (rollback or bulkhead rejection)

export const options = {
  scenarios: {
    load: {
      executor: 'constant-vus',
      vus: VUS,
      duration: DURATION,
    },
  },
};

export default function () {
  const invalid = Math.random() < INVALID_RATIO;
  const body = invalid
    ? JSON.stringify({ payload: 'rollback-me' }) // no "name" -> DB rejects -> both sides roll back
    : JSON.stringify({ name: `item-${__VU}-${__ITER}`, payload: 'load' });

  const res = http.post(`${BASE_URL}${ENDPOINT}`, body, {
    headers: { 'Content-Type': 'application/json' },
  });

  if (res.status === 201) {
    itemsCreated.add(1);
  } else {
    itemsFailed.add(1);
  }

  check(res, {
    'valid request created (201)': (r) => invalid || r.status === 201,
    'invalid request rejected (not 201)': (r) => !invalid || r.status !== 201,
  });
}

// Emit a single JSON line on stdout so the runner can parse the throughput numbers.
export function handleSummary(data) {
  const m = data.metrics;
  const v = (name, field, def = 0) =>
    m[name] && m[name].values[field] !== undefined ? m[name].values[field] : def;

  const summary = {
    endpoint: ENDPOINT,
    vus: VUS,
    duration_s: data.state.testRunDurationMs / 1000,
    http_reqs: v('http_reqs', 'count'),
    http_reqs_per_s: v('http_reqs', 'rate'),
    items_created: v('items_created', 'count'),
    items_failed: v('items_failed', 'count'),
    p95_ms: v('http_req_duration', 'p(95)'),
    avg_ms: v('http_req_duration', 'avg'),
  };
  return { stdout: JSON.stringify(summary) + '\n' };
}
