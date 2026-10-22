// 무역 항구 게임 — Netlify Function (v2) + Neon Postgres
// 환경변수: DATABASE_URL (Neon 연결 문자열), TEACHER_PIN (교사 비밀번호, 필수)
import { Pool, neonConfig } from '@neondatabase/serverless';
import ws from 'ws';
import { randomInt, createHash } from 'node:crypto';

neonConfig.webSocketConstructor = ws;
export const config = { path: '/api/*' };

/* ================= 게임 데이터 ================= */
const RAW = {
  wood:   { name: '목재',   emoji: '🪵' },
  iron:   { name: '철광석', emoji: '⛏️' },
  cotton: { name: '면화',   emoji: '☁️' },
  food:   { name: '식량',   emoji: '🌾' },
  oil:    { name: '석유',   emoji: '🛢️' },
  usd:    { name: '달러',   emoji: '💵' },
};
const RAW_KEYS = Object.keys(RAW);
const MAX_GROUP_COUNT = 10;
const MIN_GROUP_COUNT = 5;
// 기본형 10개 모둠은 각 특산 재화를 두 모둠씩, 축소형 5개 모둠은 한 모둠씩 맡는다.
const SPECIALTY_KEYS = ['wood', 'iron', 'cotton', 'food', 'oil'];
const GROUP_MATERIAL = ['wood', 'wood', 'iron', 'iron', 'cotton', 'cotton', 'food', 'food', 'oil', 'oil'];
const groupMaterialsForCount = (count) => count === 5 ? SPECIALTY_KEYS : GROUP_MATERIAL;
// 시작 모둠 수는 기존 10개 또는 축소형 5개만 허용한다.
// 모둠마다 특산 재화 8개와 다른 재화 2개·1개를 무작위로 지급한다.
const allowedGroupCount = (value) => Number(value) === 5 ? 5 : 10;

const LEVELS = {
  1: { name: '기본 가공품', points: 30 },
  2: { name: '심화 가공품', points: 100 },
  3: { name: '최종 완성품', points: 200 },
};
// 기본·심화 가공품은 재료 2종, 최종 완성품은 재료 3종으로 제작한다.
// 심화 가공품에는 기본 가공품이, 최종 완성품에는 심화 가공품이 반드시 포함된다.
const PRODUCTS = {
  // 기본 가공품: 원재료 2종 중심, 각 $30~$50
  cart:      { name: '건설 자재 묶음',       emoji: '🏗️', level: 1, points: 30, recipe: { wood: 1, iron: 1 } },
  clothes:   { name: '생활용품 세트',        emoji: '👕', level: 1, points: 30, recipe: { cotton: 1, food: 1 } },
  paper:     { name: '종이·포장재 묶음',      emoji: '📦', level: 1, points: 30, recipe: { wood: 1, cotton: 1 } },
  farmkit:   { name: '농업 도구 세트',        emoji: '🌱', level: 1, points: 30, recipe: { iron: 1, food: 1 } },
  synthetics:{ name: '합성 섬유',             emoji: '🧵', level: 1, points: 50, recipe: { oil: 1, cotton: 1 } },
  supplykit: { name: '생활 기반 꾸러미',       emoji: '🧰', level: 1, points: 50, recipe: { wood: 1, iron: 1, food: 1 } },
  // 심화 가공품: 기본 가공품 1개 + 원재료 1종, 각 $100
  smart:     { name: '스마트 기기',          emoji: '📱', level: 2, points: 100, recipe: { cart: 1, oil: 1 } },
  fashion:   { name: '고급 의류',            emoji: '🧥', level: 2, points: 100, recipe: { clothes: 1, cotton: 1 } },
  medkit:    { name: '응급 구호 키트',        emoji: '🩹', level: 2, points: 100, recipe: { farmkit: 1, oil: 1 } },
  // 최종 완성품: 심화 가공품 2개 + 기본 재화 1종, 각 $200
  city:      { name: '첨단 도시 모형',        emoji: '🚀', level: 3, points: 200, recipe: { smart: 1, medkit: 1, wood: 1 } },
  civ:       { name: '지속 가능한 미래 세트',  emoji: '👑', level: 3, points: 200, recipe: { fashion: 1, medkit: 1, food: 1 } },
};
const ITEM_KEYS = [...RAW_KEYS, ...Object.keys(PRODUCTS)];
const itemInfo = (k) => RAW[k] || PRODUCTS[k] || { name: k, emoji: '📦' };

const LIMITS = { minProposal: 10, minReply: 5, maxText: 200, maxQty: 5, maxPending: 4 };

/* ================= 공통 도구 ================= */
class HttpError extends Error {
  constructor(status, msg) { super(msg); this.status = status; }
}
const fail = (status, msg) => { throw new HttpError(status, msg); };
const json = (obj, status = 200) =>
  new Response(JSON.stringify(obj), {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' },
  });
const len = (s) => Array.from(s).length;
const clampInt = (v, lo, hi, dflt) => {
  const n = Math.round(Number(v));
  return Number.isFinite(n) ? Math.min(hi, Math.max(lo, n)) : dflt;
};
const randomCode = () => String(randomInt(1000, 10000));
function shuffle(a) {
  const arr = [...a];
  for (let i = arr.length - 1; i > 0; i--) {
    const j = randomInt(0, i + 1);
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr;
}
async function tx(client, fn) {
  await client.query('BEGIN');
  try {
    const r = await fn();
    await client.query('COMMIT');
    return r;
  } catch (e) {
    await client.query('ROLLBACK').catch(() => {});
    throw e;
  }
}

/* ================= DB 스키마 (자동 생성) ================= */
const SCHEMA_SQL = `
create table if not exists tg_game (
  id int primary key check (id = 1),
  phase text not null default 'lobby',
  ends_at timestamptz,
  production int not null default 4,
  autarky_seconds int not null default 120,
  trade_seconds int not null default 720,
  paused boolean not null default false,
  paused_remaining int not null default 0,
  announcement text not null default '',
  announcement_at timestamptz,
  group_count int not null default 10
);
alter table tg_game add column if not exists paused boolean not null default false;
alter table tg_game add column if not exists paused_remaining int not null default 0;
alter table tg_game add column if not exists announcement text not null default '';
alter table tg_game add column if not exists announcement_at timestamptz;
alter table tg_game add column if not exists group_count int not null default 10;
insert into tg_game (id) values (1) on conflict do nothing;
create table if not exists tg_groups (
  id int primary key,
  name text not null,
  material text not null,
  code text not null,
  mission text,
  mission_count int not null default 0,
  mission_bonus int not null default 0,
  mission_start_lv1 int not null default 0,
  mission_start_lv2 int not null default 0,
  mission_start_lv3 int not null default 0,
  mission_start_partners int not null default 0,
  mission_start_accepts int not null default 0,
  shop_exchange_count int not null default 0,
  last_seen timestamptz
);
alter table tg_groups add column if not exists mission_count int not null default 0;
alter table tg_groups add column if not exists mission_bonus int not null default 0;
alter table tg_groups add column if not exists mission_start_lv1 int not null default 0;
alter table tg_groups add column if not exists mission_start_lv2 int not null default 0;
alter table tg_groups add column if not exists mission_start_lv3 int not null default 0;
alter table tg_groups add column if not exists mission_start_partners int not null default 0;
alter table tg_groups add column if not exists mission_start_accepts int not null default 0;
alter table tg_groups add column if not exists shop_exchange_count int not null default 0;
create table if not exists tg_inventory (
  group_id int not null references tg_groups(id) on delete cascade,
  item text not null,
  qty int not null default 0 check (qty >= 0),
  primary key (group_id, item)
);
create table if not exists tg_crafts (
  id serial primary key,
  group_id int not null references tg_groups(id) on delete cascade,
  product text not null,
  phase text not null,
  created_at timestamptz not null default now()
);
create table if not exists tg_trades (
  id serial primary key,
  from_group int not null,
  to_group int not null,
  give_item text not null,
  give_qty int not null,
  want_item text not null,
  want_qty int not null,
  message text not null,
  reply text,
  status text not null default 'pending',
  created_at timestamptz not null default now(),
  resolved_at timestamptz
);
`;

const readySchemas = new Set();
async function ensureSchema(client, schemaKey = 'public') {
  if (readySchemas.has(schemaKey)) return;
  try {
    await client.query(SCHEMA_SQL);
  } catch (e) {
    // 처음 접속이 동시에 몰려 테이블 생성이 겹친 경우 한 번 더 시도
    await new Promise((r) => setTimeout(r, 300));
    await client.query(SCHEMA_SQL);
  }
  // 초기화 시 순차적인 수십 회 DB 왕복을 피하고 두 번의 묶음 쿼리로 처리한다.
  // 기존 모둠 입장 코드와 재고는 보존하며, 누락된 행만 생성한다.
  const groupValues = [];
  const groupParams = [];
  for (let i = 1; i <= MAX_GROUP_COUNT; i++) {
    const offset = groupParams.length;
    groupValues.push(`($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4})`);
    groupParams.push(i, `${i}모둠`, GROUP_MATERIAL[i - 1], randomCode());
  }
  await client.query(
    `insert into tg_groups (id, name, material, code) values ${groupValues.join(', ')}
     on conflict (id) do update set name=excluded.name`,
    groupParams
  );
  await client.query(
    `insert into tg_inventory (group_id, item, qty)
     select g.id, i.item, 0
     from generate_series(1, $1::int) as g(id)
     cross join unnest($2::text[]) as i(item)
     on conflict do nothing`,
    [MAX_GROUP_COUNT, ITEM_KEYS]
  );
  await client.query('update tg_game set group_count=10 where id=1 and group_count not in (5, 10)');
  readySchemas.add(schemaKey);
}

/* ================= 인증 ================= */
const TEACHER_PIN = '1234';
function checkPin(p) { return String(p) === TEACHER_PIN; }
function requireTeacher(req) {
  let p = req.headers.get('x-teacher-pin') || '';
  try { p = decodeURIComponent(p); } catch { /* 그대로 사용 */ }
  if (!checkPin(p)) fail(401, '교사 비밀번호가 맞지 않아요.');
}
async function authGroup(client, req) {
  const gid = Number(req.headers.get('x-group'));
  const code = String(req.headers.get('x-code') ?? '').trim();
  if (!Number.isInteger(gid)) fail(401, '먼저 모둠으로 입장해 주세요.');
  const { rows } = await client.query(
    'select g.id, g.name, g.material from tg_groups g, tg_game gm where g.id=$1 and g.code=$2 and g.id <= gm.group_count and gm.id=1',
    [gid, code]
  );
  if (!rows[0]) fail(401, '입장 정보가 맞지 않아요. 다시 입장해 주세요.');
  return rows[0];
}

/* ================= 데이터 읽기·점수 계산 ================= */
async function getGame(client) {
  const { rows } = await client.query(
    'select phase, ends_at, production, autarky_seconds, trade_seconds, paused, paused_remaining, announcement, announcement_at, group_count, now() as now from tg_game where id=1'
  );
  return rows[0];
}
async function loadAll(client) {
  const game = await getGame(client);
  const groups = (await client.query('select * from tg_groups where id <= (select group_count from tg_game where id=1) order by id')).rows;
  const inv = (await client.query('select group_id, item, qty from tg_inventory where group_id <= (select group_count from tg_game where id=1)')).rows;
  const crafts = (
    await client.query('select group_id, product, phase, count(*)::int as n from tg_crafts group by 1,2,3')
  ).rows;
  const trades = (await client.query('select * from tg_trades where from_group <= (select group_count from tg_game where id=1) and to_group <= (select group_count from tg_game where id=1) order by id')).rows;
  return { game, groups, inv, crafts, trades };
}

function computeStats(all) {
  const accepted = all.trades.filter((t) => t.status === 'accepted');
  const out = {};
  for (const g of all.groups) {
    const inventory = Object.fromEntries(ITEM_KEYS.map((k) => [k, 0]));
    for (const r of all.inv) if (r.group_id === g.id) inventory[r.item] = r.qty;

    const crafts = {};
    const lv = { 1: 0, 2: 0, 3: 0 };
    let craftScore = 0, autarkyScore = 0, tradeScore = 0;
    for (const r of all.crafts) {
      if (r.group_id !== g.id) continue;
      const p = PRODUCTS[r.product];
      if (!p) continue;
      const pts = (p.points ?? LEVELS[p.level].points) * r.n;
      crafts[r.product] = (crafts[r.product] || 0) + r.n;
      lv[p.level] += r.n;
      craftScore += pts;
      if (r.phase === 'autarky') autarkyScore += pts; else tradeScore += pts;
    }

    const mine = accepted.filter((t) => t.from_group === g.id || t.to_group === g.id);
    const partners = new Set(mine.map((t) => (t.from_group === g.id ? t.to_group : t.from_group)));
    const accepts = accepted.filter((t) => t.to_group === g.id).length;
    const mission = null;
    const bonus = 0;

    out[g.id] = {
      id: g.id, name: g.name, material: g.material, inventory, crafts,
      craftScore, autarkyScore, tradeScore, bonus, total: craftScore + bonus, dollars: inventory.usd || 0, finalTotal: craftScore + bonus + (inventory.usd || 0),
      tradeCount: mine.length, mission, missionCount: g.mission_count || 0, shopExchangeCount: g.shop_exchange_count || 0, levelCounts: lv, partnersCount: partners.size, acceptCount: accepts,
    };
  }
  return out;
}

const tradeOut = (t) => ({
  id: t.id, from: t.from_group, to: t.to_group,
  giveItem: t.give_item, giveQty: t.give_qty, wantItem: t.want_item, wantQty: t.want_qty,
  message: t.message, reply: t.reply, status: t.status, at: t.created_at,
});

function publicConfig(groups) {
  return {
    raw: RAW, products: PRODUCTS, levels: LEVELS, limits: LIMITS,
    groups: groups.map((g) => ({ id: g.id, name: g.name, material: g.material })),
  };
}

async function state(client, req) {
  const pinHeader = req.headers.get('x-teacher-pin');
  let teacher = false;
  let g = null;
  if (pinHeader !== null) { requireTeacher(req); teacher = true; }
  else {
    g = await authGroup(client, req);
    await client.query('update tg_groups set last_seen=now() where id=$1', [g.id]);
  }

  const all = await loadAll(client);
  const stats = computeStats(all);
  // 진행 중 순위는 제작 포인트만으로 계산한다. 게임 종료 시에는 남은 달러를 더해 최종 순위를 계산한다.
  const ranked = Object.values(stats).sort((a, b) => b.craftScore - a.craftScore || a.id - b.id);
  const finalRanked = Object.values(stats).sort((a, b) => b.finalTotal - a.finalTotal || a.id - b.id);
  const sumOf = (f) => Object.values(stats).reduce((s, x) => s + x[f], 0);
  const compare = { autarky: sumOf('autarkyScore'), trade: sumOf('tradeScore'), bonus: sumOf('bonus') };
  const gm = all.game;

  const base = {
    serverNow: gm.now,
    game: {
      phase: gm.phase, endsAt: gm.ends_at, production: gm.production,
      autarkySeconds: gm.autarky_seconds, tradeSeconds: gm.trade_seconds, groupCount: gm.group_count,
      paused: gm.paused, pausedRemaining: gm.paused_remaining, announcement: gm.announcement || '', announcementAt: gm.announcement_at,
    },
    config: publicConfig(all.groups),
    compare,
  };

  if (teacher) {
    const now = new Date(gm.now).getTime();
    const trades = [...all.trades].reverse().map(tradeOut);
    const count = (s) => all.trades.filter((t) => t.status === s).length;
    return json({
      ...base,
      role: 'teacher',
      groups: all.groups.map((row) => ({
        ...stats[row.id],
        code: row.code,
        online: !!row.last_seen && now - new Date(row.last_seen).getTime() < 12000,
      })),
      trades,
      summary: { accepted: count('accepted'), pending: count('pending'), rejected: count('rejected') },
      results: gm.phase === 'finished' ? finalRanked.map((s, i) => ({ rank: i + 1, id: s.id, name: s.name, material: s.material, craftScore: s.craftScore, dollars: s.dollars, total: s.finalTotal })) : null,
    });
  }

  const me = stats[g.id];
  const tradesAsc = all.trades;
  const out = {
    ...base,
    role: 'group',
    me,
    board: ranked.map((s) => ({ id: s.id, name: s.name, material: s.material, total: s.total })),
    incoming: tradesAsc.filter((t) => t.to_group === g.id).reverse().map(tradeOut),
    outgoing: tradesAsc.filter((t) => t.from_group === g.id).reverse().map(tradeOut),
    // 학생 무역 탭에서 다른 모둠의 재고도 확인할 수 있도록 공개 재고만 전달한다(입장 코드 등 비공개 정보는 제외).
    marketGroups: all.groups.map((group) => ({
      id: group.id, name: group.name, material: group.material,
      inventory: Object.fromEntries(ITEM_KEYS.map((key) => [key, stats[group.id]?.inventory[key] ?? 0])),
    })),
    feed: tradesAsc.filter((t) => t.status === 'accepted').reverse().map((t) => ({
      id: t.id, from: t.from_group, to: t.to_group,
      giveItem: t.give_item, giveQty: t.give_qty, wantItem: t.want_item, wantQty: t.want_qty,
    })),
  };
  if (gm.phase === 'finished') {
    out.results = finalRanked.map((s, i) => ({
      rank: i + 1, id: s.id, name: s.name, material: s.material,
      craftScore: s.craftScore, dollars: s.dollars, total: s.finalTotal,
    }));
  }
  return json(out);
}

/* ================= 모둠 행동 ================= */
async function craft(client, g, b) {
  const key = String(b.product);
  const p = PRODUCTS[key];
  if (!p) fail(400, '만들 물품을 골라 주세요.');
  const game = await getGame(client);
  if (!['autarky', 'trade'].includes(game.phase) || game.paused) fail(403, game.paused ? '선생님이 게임을 잠시 멈췄어요.' : '지금은 물품을 만들 수 없어요.');

  return tx(client, async () => {
    const { rows } = await client.query(
      'select item, qty from tg_inventory where group_id=$1 for update', [g.id]
    );
    const have = (k) => rows.find((r) => r.item === k)?.qty ?? 0;
    const missing = Object.entries(p.recipe)
      .filter(([k, n]) => have(k) < n)
      .map(([k]) => itemInfo(k).name);
    if (missing.length) fail(400, `재료가 부족해요: ${missing.join(', ')}`);
    for (const [k, n] of Object.entries(p.recipe)) {
      await client.query('update tg_inventory set qty=qty-$3 where group_id=$1 and item=$2', [g.id, k, n]);
    }
    await client.query('insert into tg_crafts (group_id, product, phase) values ($1,$2,$3)', [g.id, key, game.phase]);
    await client.query('update tg_inventory set qty=qty+1 where group_id=$1 and item=$2', [g.id, key]);
    const points = p.points ?? LEVELS[p.level].points;
    // 물품 제작은 포인트만 적립한다. 달러는 시작 지급 및 무역으로만 보유·사용한다.
    return json({ ok: true, product: key, points, dollarsEarned: 0 });
  });
}

async function propose(client, g, b) {
  const to = Number(b.to);
  const giveItem = String(b.giveItem);
  const wantItem = String(b.wantItem);
  const giveQty = Number(b.giveQty);
  const wantQty = Number(b.wantQty);
  const message = String(b.message ?? '').trim();

  if (!Number.isInteger(to) || to < 1 || to > (await getGame(client)).group_count || to === g.id) fail(400, '제안을 보낼 다른 모둠을 골라 주세요.');
  if (!RAW[giveItem] || !RAW[wantItem]) fail(400, '교환할 재화를 골라 주세요. 가공품은 무역할 수 없어요.');
  if (giveItem === wantItem) fail(400, '서로 다른 재화끼리 바꿔야 해요.');
  const dollarTrade = giveItem === 'usd' || wantItem === 'usd';
  if (!dollarTrade) {
    if (giveItem === 'usd' || wantItem === 'usd' || giveQty !== 1 || wantQty !== 1) fail(400, '물물교환은 기본 재화 1개와 다른 기본 재화 1개를 바꾸는 1대1 방식이에요.');
  } else {
    if (giveItem === 'usd' && (giveQty < 30 || giveQty > 300 || giveQty % 30 !== 0 || wantQty !== 1)) fail(400, '달러로 교환할 때는 30달러 단위로, 기본 재화 1개씩 제안해 주세요.');
    if (wantItem === 'usd' && (wantQty < 30 || wantQty > 300 || wantQty % 30 !== 0 || giveQty !== 1)) fail(400, '달러로 교환할 때는 기본 재화 1개당 30달러 단위로 제안해 주세요.');
  }
  if (len(message) > LIMITS.maxText) fail(400, `무역 제안문은 ${LIMITS.maxText}글자까지 쓸 수 있어요.`);

  const game = await getGame(client);
  if (game.phase !== 'trade' || game.paused) fail(403, game.paused ? '선생님이 게임을 잠시 멈췄어요.' : '지금은 무역 시간이 아니에요.');

  return tx(client, async () => {
    const inv = await client.query('select qty from tg_inventory where group_id=$1 and item=$2', [g.id, giveItem]);
    if ((inv.rows[0]?.qty ?? 0) < giveQty) fail(400, `줄 재료가 부족해요 (${RAW[giveItem].name}). 가진 만큼만 제안할 수 있어요.`);
    const pend = await client.query(
      "select count(*)::int as n from tg_trades where from_group=$1 and status='pending'", [g.id]
    );
    if (pend.rows[0].n >= LIMITS.maxPending) fail(400, '답을 기다리는 제안이 너무 많아요. 답장을 기다리거나 취소해 보세요.');
    const r = await client.query(
      `insert into tg_trades (from_group, to_group, give_item, give_qty, want_item, want_qty, message)
       values ($1,$2,$3,$4,$5,$6,$7) returning id`,
      [g.id, to, giveItem, giveQty, wantItem, wantQty, message]
    );
    return json({ ok: true, id: r.rows[0].id });
  });
}

async function shopExchange(client, g, b) {
  const mode = String(b.mode || 'resource');
  if (mode !== 'resource') fail(400, '국내 생산 상점에서는 달러로 재화를 살 수 없어요. 기본 재화끼리만 교환할 수 있어요.');
  const wantItem = String(b.wantItem || '');
  if (!RAW[wantItem] || wantItem === 'usd') fail(400, '받을 기본 재화를 골라 주세요. 가공품과 달러는 받을 수 없어요.');

  return tx(client, async () => {
    const { rows } = await client.query('select phase, paused from tg_game where id=1 for update');
    const game = rows[0];
    if (game.phase !== 'trade' || game.paused) fail(403, game.paused ? '선생님이 게임을 잠시 멈췄어요.' : '상점은 무역 시간에만 이용할 수 있어요.');
    const groupRows = await client.query('select shop_exchange_count from tg_groups where id=$1 for update', [g.id]);
    const used = groupRows.rows[0]?.shop_exchange_count ?? 0;
    if (used >= 5) fail(400, '이번 게임에서 상점 교환을 모두 사용했어요. 모둠별 교환은 게임당 5번까지 가능해요.');

    const giveItem = String(b.giveItem1 || '');
    if (!RAW[giveItem] || giveItem === 'usd' || giveItem === wantItem) fail(400, '서로 다른 기본 재화를 선택해 주세요. 국내 생산 상점에서는 달러를 사용할 수 없어요.');
    const inv = await client.query('select item, qty from tg_inventory where group_id=$1 and item = any($2::text[]) for update', [g.id, [giveItem, wantItem]]);
    const quantities = Object.fromEntries(inv.rows.map((row) => [row.item, row.qty]));
    if ((quantities[giveItem] ?? 0) < 2) fail(400, `${RAW[giveItem].name}이(가) 부족해요. 현재 ${quantities[giveItem] ?? 0}개 있어요.`);
    await client.query('update tg_inventory set qty=qty-2 where group_id=$1 and item=$2', [g.id, giveItem]);
    await client.query('update tg_inventory set qty=qty+1 where group_id=$1 and item=$2', [g.id, wantItem]);
    const updated = await client.query('update tg_groups set shop_exchange_count=shop_exchange_count+1 where id=$1 returning shop_exchange_count', [g.id]);
    return json({ ok: true, mode, wantItem, exchangesUsed: updated.rows[0].shop_exchange_count, exchangesRemaining: Math.max(0, 5 - updated.rows[0].shop_exchange_count) });
  });
}

async function respond(client, g, b) {
  const id = Number(b.id);
  const accept = b.accept === true;
  const reply = String(b.reply ?? '').trim();
  if (!Number.isInteger(id)) fail(400, '제안을 찾을 수 없어요.');
  if (len(reply) > LIMITS.maxText) fail(400, `답장은 ${LIMITS.maxText}글자까지 쓸 수 있어요.`);
  const game = await getGame(client);
  if (game.phase !== 'trade' || game.paused) fail(403, game.paused ? '선생님이 게임을 잠시 멈췄어요.' : '지금은 무역 시간이 아니에요.');

  const result = await tx(client, async () => {
    const tr = await client.query('select * from tg_trades where id=$1 for update', [id]);
    const t = tr.rows[0];
    if (!t || t.to_group !== g.id) fail(404, '제안을 찾을 수 없어요.');
    if (t.status !== 'pending') fail(409, '이미 처리된 제안이에요.');

    if (!accept) {
      // 거절 횟수 제한은 두지 않는다. 자원이 부족하거나 조건이 맞지 않으면 언제든 거절할 수 있다.
      await client.query(
        "update tg_trades set status='rejected', reply=$2, resolved_at=now() where id=$1",
        [id, reply || null]
      );
      return { status: 'rejected' };
    }

    // 두 모둠의 창고를 항상 같은 순서로 잠가서 서로 막히는 일을 방지
    const inv = await client.query(
      'select group_id, item, qty from tg_inventory where group_id = any($1::int[]) order by group_id, item for update',
      [[t.from_group, t.to_group]]
    );
    const q = (gid, item) => inv.rows.find((r) => r.group_id === gid && r.item === item)?.qty ?? 0;

    if (q(t.to_group, t.want_item) < t.want_qty) {
      fail(400, `우리 모둠의 ${RAW[t.want_item].name}이 부족해서 수락할 수 없어요.`);
    }
    if (q(t.from_group, t.give_item) < t.give_qty) {
      await client.query(
        "update tg_trades set status='cancelled', reply='상대 모둠의 재료가 부족해져서 자동으로 취소됐어요.', resolved_at=now() where id=$1",
        [id]
      );
      return { error: '상대 모둠의 재료가 부족해져서 제안이 취소됐어요.' };
    }

    const move = (gid, item, delta) =>
      client.query('update tg_inventory set qty=qty+$3 where group_id=$1 and item=$2', [gid, item, delta]);
    await move(t.from_group, t.give_item, -t.give_qty);
    await move(t.from_group, t.want_item, t.want_qty);
    await move(t.to_group, t.want_item, -t.want_qty);
    await move(t.to_group, t.give_item, t.give_qty);
    await client.query(
      "update tg_trades set status='accepted', reply=$2, resolved_at=now() where id=$1",
      [id, reply]
    );
    return { status: 'accepted' };
  });

  if (result.error) return json({ error: result.error }, 409);
  return json({ ok: true, status: result.status });
}

async function cancel(client, g, b) {
  const id = Number(b.id);
  if (!Number.isInteger(id)) fail(400, '제안을 찾을 수 없어요.');
  const r = await client.query(
    "update tg_trades set status='cancelled', resolved_at=now() where id=$1 and from_group=$2 and status='pending' returning id",
    [id, g.id]
  );
  if (!r.rows[0]) fail(409, '취소할 수 없는 제안이에요.');
  return json({ ok: true });
}

/* ================= 교사 행동 ================= */
async function wipe(client) {
  await client.query('delete from tg_trades');
  await client.query('delete from tg_crafts');
  await client.query('update tg_inventory set qty=0');
  await client.query('update tg_groups set mission=null, mission_count=0, mission_bonus=0, mission_start_lv1=0, mission_start_lv2=0, mission_start_lv3=0, mission_start_partners=0, mission_start_accepts=0, shop_exchange_count=0');
}
// 게임 시작 시 기존 방식으로 자원을 지급한다.
// 각 모둠은 특산 재화 8개, 다른 핵심 재화 2개와 1개(서로 다른 재화)를 무작위로 받으며 달러 $150을 받는다.
// 5개 모둠은 5종의 특산 재화를 하나씩, 10개 모둠은 각 특산 재화를 두 모둠씩 맡는다.
async function grantInitialResources(client) {
  const groups = (await client.query(
    'select id, material from tg_groups where id <= (select group_count from tg_game where id=1) order by id'
  )).rows;
  for (const group of groups) {
    await client.query(
      'update tg_inventory set qty=qty+$3 where group_id=$1 and item=$2',
      [group.id, group.material, 8]
    );
    await client.query(
      'update tg_inventory set qty=qty+150 where group_id=$1 and item=$2',
      [group.id, 'usd']
    );
    const bonusItems = shuffle(SPECIALTY_KEYS.filter((item) => item !== group.material)).slice(0, 2);
    await client.query(
      'update tg_inventory set qty=qty+$3 where group_id=$1 and item=$2',
      [group.id, bonusItems[0], 2]
    );
    await client.query(
      'update tg_inventory set qty=qty+$3 where group_id=$1 and item=$2',
      [group.id, bonusItems[1], 1]
    );
  }
}

async function teacherAction(client, action, b) {
  switch (action) {
    case 'start': {
      const production = 20;
      const autarky = clampInt(b.autarkySeconds, 0, 1800, 120);
      const trade = clampInt(b.tradeSeconds, 60, 3600, 720);
      return tx(client, async () => {
        const currentGame = await getGame(client);
        if (currentGame.phase === 'finished') await wipe(client);
        else { await client.query('delete from tg_trades'); await client.query('delete from tg_crafts'); await client.query('update tg_groups set mission=null'); }
        const groupCount = allowedGroupCount(b.groupCount);
        await client.query('update tg_game set group_count=$1 where id=1', [groupCount]);
        // 새 게임마다 재고를 초기화해 이전 게임의 자원이 누적되지 않게 한다.
        await client.query('update tg_inventory set qty=0');
        await client.query('delete from tg_trades where from_group > $1 or to_group > $1', [groupCount]);
        await client.query('delete from tg_crafts where group_id > $1', [groupCount]);
        await client.query('update tg_inventory set qty=0 where group_id > $1', [groupCount]);
        const materials = groupMaterialsForCount(groupCount);
        for (let i = 0; i < groupCount; i++) {
          await client.query('update tg_groups set material=$2, mission=null, mission_count=0, mission_bonus=0, mission_start_lv1=0, mission_start_lv2=0, mission_start_lv3=0, mission_start_partners=0, mission_start_accepts=0, shop_exchange_count=0 where id=$1', [i + 1, materials[i]]);
        }
        await grantInitialResources(client);
        await client.query(
          "update tg_game set phase='autarky', ends_at = now() + ($1::int * interval '1 second'), production=$2, autarky_seconds=$1, trade_seconds=$3, paused=false, paused_remaining=0, announcement='', announcement_at=null where id=1",
          [autarky, production, trade]
        );
        return json({ ok: true });
      });
    }
    case 'open_trade': {
      return tx(client, async () => {
        const { rows } = await client.query('select phase, trade_seconds, paused from tg_game where id=1 for update');
        if (rows[0].phase !== 'autarky') fail(409, '자급자족 단계에서만 무역을 열 수 있어요.');
        if (rows[0].paused) fail(409, '게임을 재개한 뒤 무역을 열어 주세요.');
        await client.query(
          "update tg_game set phase='trade', ends_at = now() + (trade_seconds * interval '1 second'), paused_remaining=0 where id=1"
        );
        return json({ ok: true });
      });
    }
    case 'extend': {
      const sec = clampInt(b.seconds, 10, 600, 60);
      const game = await getGame(client);
      if (!['autarky', 'trade'].includes(game.phase)) fail(409, '진행 중일 때만 시간을 늘릴 수 있어요.');
      if (game.paused) {
        await client.query('update tg_game set paused_remaining=paused_remaining+$1 where id=1', [sec]);
      } else {
        await client.query(
          "update tg_game set ends_at = greatest(coalesce(ends_at, now()), now()) + ($1::int * interval '1 second') where id=1",
          [sec]
        );
      }
      return json({ ok: true });
    }
    case 'pause': {
      return tx(client, async () => {
        const { rows } = await client.query('select phase, paused, ends_at from tg_game where id=1 for update');
        const game = rows[0];
        if (!['autarky', 'trade'].includes(game.phase)) fail(409, '게임 진행 중에만 멈출 수 있어요.');
        if (game.paused) return json({ ok: true, paused: true });
        const rem = game.ends_at ? Math.max(0, Math.ceil((new Date(game.ends_at).getTime() - Date.now()) / 1000)) : 0;
        await client.query('update tg_game set paused=true, paused_remaining=$1, ends_at=null where id=1', [rem]);
        return json({ ok: true, paused: true });
      });
    }
    case 'resume': {
      return tx(client, async () => {
        const { rows } = await client.query('select phase, paused, paused_remaining from tg_game where id=1 for update');
        const game = rows[0];
        if (!['autarky', 'trade'].includes(game.phase)) fail(409, '게임 진행 중에만 재개할 수 있어요.');
        if (!game.paused) return json({ ok: true, paused: false });
        await client.query("update tg_game set paused=false, ends_at=now() + (paused_remaining * interval '1 second'), paused_remaining=0 where id=1");
        return json({ ok: true, paused: false });
      });
    }
    case 'grant_resource': {
      const groupId = clampInt(b.groupId, 1, MAX_GROUP_COUNT, 0);
      const item = String(b.item || '');
      const qty = clampInt(b.qty, 1, 999, 0);
      const game = await getGame(client);
      if (!['lobby', 'autarky', 'trade'].includes(game.phase)) fail(409, '게임이 끝난 뒤에는 자원을 지급할 수 없어요. 새 게임을 준비해 주세요.');
      if (groupId < 1 || groupId > game.group_count) fail(400, '현재 설정된 모둠 번호를 확인해 주세요.');
      if (!RAW[item]) fail(400, '지급할 자원을 골라 주세요.');
      await client.query('update tg_inventory set qty=qty+$3 where group_id=$1 and item=$2', [groupId, item, qty]);
      return json({ ok: true });
    }
    case 'set_group_count': {
      const groupCount = allowedGroupCount(b.groupCount);
      return tx(client, async () => {
        const game = await getGame(client);
        if (game.phase !== 'lobby') fail(409, '모둠 개수는 게임 시작 전에만 바꿀 수 있어요.');
        await client.query('update tg_game set group_count=$1 where id=1', [groupCount]);
        await client.query('delete from tg_trades where from_group > $1 or to_group > $1', [groupCount]);
        await client.query('delete from tg_crafts where group_id > $1', [groupCount]);
        await client.query('update tg_inventory set qty=0 where group_id > $1', [groupCount]);
        const materials = groupMaterialsForCount(groupCount);
        for (let i = 0; i < groupCount; i++) {
          await client.query('update tg_groups set material=$2 where id=$1', [i + 1, materials[i]]);
        }
        return json({ ok: true, groupCount });
      });
    }
    case 'announce': {
      const message = String(b.message ?? '').trim().slice(0, 240);
      await client.query('update tg_game set announcement=$1, announcement_at=now() where id=1', [message]);
      return json({ ok: true });
    }
    case 'finish': {
      const game = await getGame(client);
      if (!['autarky', 'trade'].includes(game.phase)) fail(409, '진행 중일 때만 끝낼 수 있어요.');
      await client.query("update tg_game set phase='finished', ends_at=null, paused=false, paused_remaining=0 where id=1");
      return json({ ok: true });
    }
    case 'reset': {
      return tx(client, async () => {
        await wipe(client);
        for (let i = 1; i <= MAX_GROUP_COUNT; i++) {
          await client.query('update tg_groups set code=$2, last_seen=null where id=$1', [i, randomCode()]);
        }
        await client.query("update tg_game set phase='lobby', group_count=10, ends_at=null, paused=false, paused_remaining=0, announcement='', announcement_at=null where id=1");
        for (let i = 0; i < MAX_GROUP_COUNT; i++) {
          await client.query('update tg_groups set material=$2 where id=$1', [i + 1, GROUP_MATERIAL[i]]);
        }
        return json({ ok: true });
      });
    }
    default:
      return fail(404, '알 수 없는 요청이에요.');
  }
}

/* ================= 학급별 독립 게임 공간 ================= */
function normalizeRoomCode(value) {
  let decoded = String(value ?? '');
  try { decoded = decodeURIComponent(decoded); } catch { /* 이미 디코딩된 문자열이면 그대로 사용 */ }
  const room = decoded.trim().normalize('NFKC').toLowerCase();
  if (!room) return 'public'; // 이전 버전에서 로그인한 사용자는 기존 게임 공간을 유지한다.
  if (room === '기본학급' || room === 'default') return 'public';
  if (Array.from(room).length > 24 || !/^[\p{L}\p{N} _-]+$/u.test(room)) {
    fail(400, '학급 코드는 한글·영문·숫자·공백·하이픈·밑줄로 24자 이내로 입력해 주세요.');
  }
  return 'tg_room_' + createHash('sha256').update(room).digest('hex').slice(0, 24);
}
async function selectRoom(client, req) {
  const key = normalizeRoomCode(req.headers.get('x-room-code'));
  if (key !== 'public') {
    // 스키마 이름은 서버에서 해시로 생성하므로 SQL 식별자 삽입 위험이 없다.
    await client.query(`create schema if not exists "${key}"`);
    await client.query(`set search_path to "${key}"`);
  } else {
    await client.query('set search_path to public');
  }
  return key;
}

/* ================= 로그인 & 라우팅 ================= */
async function login(client, b) {
  if (b.pin !== undefined) {
    if (!checkPin(String(b.pin))) fail(401, '교사 비밀번호가 맞지 않아요.');
    return json({ ok: true, role: 'teacher' });
  }
  const gid = Number(b.group);
  if (!Number.isInteger(gid)) fail(400, '모둠 번호를 골라 주세요.');
  const { rows } = await client.query(
    'select g.id, g.name, g.material from tg_groups g, tg_game gm where g.id=$1 and g.code=$2 and g.id <= gm.group_count and gm.id=1',
    [gid, String(b.code ?? '').trim()]
  );
  if (!rows[0]) fail(401, '모둠 번호나 입장 코드가 맞지 않아요.');
  return json({ ok: true, role: 'group', group: rows[0] });
}

async function readBody(req) {
  try { return (await req.json()) ?? {}; } catch { return fail(400, '잘못된 요청이에요.'); }
}

async function dispatch(client, req, route) {
  const method = req.method;
  if (route === 'state' && method === 'GET') return state(client, req);
  if (method !== 'POST') fail(405, '지원하지 않는 요청이에요.');

  const body = await readBody(req);
  if (route === 'login') return login(client, body);
  if (route.startsWith('teacher/')) {
    requireTeacher(req);
    return teacherAction(client, route.slice('teacher/'.length), body);
  }
  const g = await authGroup(client, req);
  switch (route) {
    case 'craft':   return craft(client, g, body);
    case 'propose': return propose(client, g, body);
    case 'shop_exchange': return shopExchange(client, g, body);
    case 'respond': return respond(client, g, body);
    case 'cancel':  return cancel(client, g, body);
    default:        return fail(404, '알 수 없는 요청이에요.');
  }
}

export default async (req) => {
  const route = new URL(req.url).pathname.replace(/^\/api\/?/, '').replace(/\/$/, '');

  // 교사 PIN 확인은 DB를 전혀 사용하지 않는다. DB 연결/스키마 초기화를 기다리지 않고 즉시 응답한다.
  // clone()을 사용하므로 일반 로그인 처리에 사용할 원본 요청 본문은 그대로 남는다.
  if (route === 'login' && req.method === 'POST') {
    try {
      const earlyBody = await req.clone().json();
      if (earlyBody && earlyBody.pin !== undefined) {
        if (!checkPin(String(earlyBody.pin))) return json({ error: '교사 비밀번호가 맞지 않아요.' }, 401);
        return json({ ok: true, role: 'teacher' });
      }
    } catch {
      // 본문 오류는 일반 요청 처리 경로에서 일관되게 안내한다.
    }
  }

  if (!process.env.DATABASE_URL) {
    return json({ error: 'DATABASE_URL 환경변수가 설정되지 않았어요.' }, 500);
  }
  const pool = new Pool({ connectionString: process.env.DATABASE_URL });
  let client;
  try {
    client = await pool.connect();
    const roomKey = await selectRoom(client, req);
    await ensureSchema(client, roomKey);
    if (route === 'public-config' && req.method === 'GET') {
      const game = await getGame(client);
      return json({ groupCount: game.group_count });
    }
    return await dispatch(client, req, route);
  } catch (e) {
    if (e instanceof HttpError) return json({ error: e.message }, e.status);
    console.error(e);
    return json({ error: '서버에 문제가 생겼어요. 잠시 뒤에 다시 해 보세요.' }, 500);
  } finally {
    if (client) client.release();
    await pool.end().catch(() => {});
  }
};
