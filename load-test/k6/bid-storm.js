// Tempestade de lances nos "últimos segundos" de um leilão.
//
//   docker run --rm -i --network host -v "$PWD/load-test/k6:/scripts" grafana/k6:1.3.0 run /scripts/bid-storm.js
//   BASE_URL=http://<alb-dns> RATE=2000 DURATION=60s ... (variáveis opcionais, via -e)
//   AUTH=cognito TOKEN_USERS=200 contra a AWS (lances com JWT do Cognito)
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
// AUTH=cognito (AWS): o setup cadastra TOKEN_USERS usuários e cada lance usa o access token de um deles.
// Com o rate limit por usuário (10 lances/s), a taxa útil é no máximo ~TOKEN_USERS × 10/s.
const AUTH = __ENV.AUTH || 'header';
const TOKEN_USERS = Number(__ENV.TOKEN_USERS || 200);

const accepted = new Counter('bids_accepted');
const rejected = new Counter('bids_rejected');
const replayed = new Counter('bids_replayed');
// 503 = a task recusou o excesso (load shedding) em vez de enfileirar: capacidade excedida, não erro.
const shed = new Counter('bids_shed');

export const options = {
  setupTimeout: '5m',
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

function cognitoUsers() {
  const suffix = uuidv4().slice(0, 8);
  const tokens = [];
  for (let start = 0; start < TOKEN_USERS; start += 10) {
    const creds = [];
    for (let i = start; i < Math.min(start + 10, TOKEN_USERS); i++) {
      creds.push(JSON.stringify({ username: `k6-${suffix}-${i}`, password: `k6-${suffix}-pw1` }));
    }
    const json = { headers: { 'Content-Type': 'application/json' } };
    http.batch(creds.map((c) => ['POST', `${BASE}/auth/signup`, c, json]));
    const logins = http.batch(creds.map((c) => ['POST', `${BASE}/auth/login`, c, json]));
    logins.forEach((r) => { if (r.status === 200) tokens.push(r.json('accessToken')); });
  }
  if (tokens.length === 0) throw new Error('nenhum usuário do Cognito conseguiu logar');
  console.log(`${tokens.length} usuários do Cognito prontos`);
  return tokens;
}

function identity(data, user) {
  if (AUTH !== 'cognito') return { 'X-User-Id': user };
  return { Authorization: `Bearer ${data.tokens[randomIntBetween(0, data.tokens.length - 1)]}` };
}

export function setup() {
  const tokens = AUTH === 'cognito' ? cognitoUsers() : [];
  const creator = AUTH === 'cognito' ? { Authorization: `Bearer ${tokens[0]}` } : {};
  const res = http.post(`${BASE}/auctions`, JSON.stringify({
    title: 'k6 storm', startingPrice: '100.00', minIncrement: '1.00',
    durationSeconds: 3600, antiSnipingWindowSeconds: 0, antiSnipingExtendSeconds: 0,
  }), { headers: { 'Content-Type': 'application/json', ...creator } });
  return { auctionId: res.json('id'), tokens };
}

let known = 100.0;

export default function (data) {
  const user = `u${randomIntBetween(1, USERS)}`;
  const key = uuidv4();
  const amount = (known + randomIntBetween(0, 2)).toFixed(2);
  const params = {
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key, ...identity(data, user) },
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
