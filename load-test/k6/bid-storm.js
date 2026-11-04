// Tempestade de lances nos "últimos segundos" de um leilão.
//
//   docker run --rm -i --network host -v "$PWD/load-test/k6:/scripts" grafana/k6:1.3.0 run /scripts/bid-storm.js
//   BASE_URL=http://<alb-dns> RATE=2000 DURATION=60s ... (variáveis opcionais, via -e)
//
// Cada usuário virtual cobre o mínimo que conhece (+0 a 2 incrementos), como faria um cliente real
// reagindo ao preço; 5% dos lances são reenviados com a mesma Idempotency-Key.
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';
import { randomIntBetween, uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const RATE = Number(__ENV.RATE || 1000);
const DURATION = __ENV.DURATION || '30s';
const USERS = Number(__ENV.USERS || 50000);

const accepted = new Counter('bids_accepted');
const rejected = new Counter('bids_rejected');
const replayed = new Counter('bids_replayed');
// 503 = a task recusou o excesso (load shedding) em vez de enfileirar: capacidade excedida, não erro.
const shed = new Counter('bids_shed');

export const options = {
  scenarios: {
    storm: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: Math.max(50, RATE / 5),
      maxVUs: RATE * 2,
    },
  },
  thresholds: {
    'http_req_failed{expected:false}': ['rate==0'],
    'http_req_duration{name:bid}': ['p(95)<250', 'p(99)<500'],
    checks: ['rate>0.999'],
  },
};

export function setup() {
  const res = http.post(`${BASE}/auctions`, JSON.stringify({
    title: 'k6 storm', startingPrice: '100.00', minIncrement: '1.00',
    durationSeconds: 3600, antiSnipingWindowSeconds: 0, antiSnipingExtendSeconds: 0,
  }), { headers: { 'Content-Type': 'application/json' } });
  return { auctionId: res.json('id') };
}

let known = 100.0;

export default function (data) {
  const user = `u${randomIntBetween(1, USERS)}`;
  const key = uuidv4();
  const amount = (known + randomIntBetween(0, 2)).toFixed(2);
  const params = {
    headers: { 'Content-Type': 'application/json', 'X-User-Id': user, 'Idempotency-Key': key },
    tags: { name: 'bid' },
    // 409/422/429 são respostas de negócio esperadas; 503 é descarte por excesso de carga (contado à parte).
    responseCallback: http.expectedStatuses(200, 201, 409, 422, 429, 503),
  };
  const body = JSON.stringify({ amount });
  const res = http.post(`${BASE}/auctions/${data.auctionId}/bids`, body, params);

  check(res, { 'sem erro de servidor': (r) => r.status < 500 || r.status === 503 });
  if (res.status === 503) { shed.add(1); return; }
  if (res.status === 201) accepted.add(1);
  else if (res.status === 200) replayed.add(1);
  else rejected.add(1);

  const min = res.status < 500 ? res.json('minimumNextBid') : null;
  if (min) known = Number(min);

  if (Math.random() < 0.05) {
    const retry = http.post(`${BASE}/auctions/${data.auctionId}/bids`, body, params);
    if (retry.status === 503) { shed.add(1); return; }
    check(retry, { 'retry devolve o mesmo resultado': (r) => r.status === res.status || (res.status === 201 && r.status === 200) });
    if (retry.status === 200) replayed.add(1);
  }
}

export function teardown(data) {
  const a = http.get(`${BASE}/auctions/${data.auctionId}`).json();
  console.log(`leilão ${data.auctionId}: seq=${a.seq} preço=${a.currentPrice} líder=${a.leaderId}`);
}
