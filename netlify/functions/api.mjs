// 무역 항구 게임 — Netlify Function (v2) + Neon Postgres
// 환경변수: DATABASE_URL (Neon 연결 문자열), TEACHER_PIN (교사 비밀번호, 필수)
import { Pool, neonConfig } from '@neondatabase/serverless';
import ws from 'ws';
import { randomInt } from 'node:crypto';

neonConfig.webSocketConstructor = ws;
export const config = { path: '/api/*' };

/* ================= 게임 데이터 ================= */
const RAW = {
  wood:   { name: '목재',   emoji: '🪵' },
  iron:   { name: '철광석', emoji: '⛏️' },
  cotton: { name: '면화',   emoji: '☁️' },
  food:   { name: '식량',   emoji: '🌾' },
  oil:    { name: '석유',   emoji: '🛢️' },
  gold:   { name: '금',     emoji: '🪙' },
};
const RAW_KEYS = Object.keys(RAW);
const GROUP_COUNT = 6;
const GROUP_MATERIAL = ['wood', 'iron', 'cotton', 'food', 'oil', 'gold']; // 1~6모둠의 특산품

const LEVELS = {
  1: { name: '기본 가공품', points: 30 },
  2: { name: '심화 가공품', points: 70 },
  3: { name: '최종 완성품', points: 150 },
};
const PRODUCTS = {
  cart:    { name: '건물 건설 카트',   emoji: '🏗️', level: 1, recipe: { wood: 1, iron: 1 } },
  clothes: { name: '의류 세트',        emoji: '👕', level: 1, recipe: { cotton: 1, food: 1 } },
  fuel:    { name: '연료 통',          emoji: '⛽', level: 1, recipe: { oil: 1, iron: 1 } },
  smart:   { name: '스마트 기기',      emoji: '📱', level: 2, recipe: { iron: 1, gold: 1, oil: 1 } },
  fashion: { name: '고급 원단/패션',   emoji: '🧥', level: 2, recipe: { cotton: 1, gold: 1, food: 1 } },
  energy:  { name: '가공 에너지 카드', emoji: '🔋', level: 2, recipe: { oil: 1, wood: 1, food: 1 } },
  city:    { name: '우주선 / 첨단 도시', emoji: '🚀', level: 3, recipe: { iron: 1, gold: 1, oil: 1, wood: 1 } },
  civ:     { name: '호화 문명 세트',   emoji: '👑', level: 3, recipe: { cotton: 1, gold: 1, food: 1, wood: 1 } },
};

const MISSIONS = {
  partners3: { title: '무역왕',       emoji: '🌏', desc: '서로 다른 3개 모둠과 무역 성공하기',        bonus: 40, target: 3 },
  accept2:   { title: '친절한 상인',  emoji: '💌', desc: '다른 모둠의 제안을 2번 수락하기',           bonus: 30, target: 2 },
  lv3:       { title: '도전자',       emoji: '🚀', desc: '난이도 3 최종 완성품 1개 만들기',           bonus: 50, target: 1 },
  allLevels: { title: '전 단계 정복', emoji: '🏅', desc: '난이도 1·2·3 물품을 각각 1개 이상 만들기', bonus: 60, target: 3 },
  lv2x2:     { title: '기술자',       emoji: '🔧', desc: '난이도 2 심화 가공품 2개 만들기',           bonus: 40, target: 2 },
  lv1x3:     { title: '기초 튼튼',    emoji: '🧱', desc: '난이도 1 기본 가공품 3개 만들기',           bonus: 30, target: 3 },
};

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
  trade_seconds int not null default 720
);
insert into tg_game (id) values (1) on conflict do nothing;
create table if not exists tg_groups (
  id int primary key,
  name text not null,
  material text not null,
  code text not null,
  mission text,
  last_seen timestamptz
);
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

let schemaReady = false;
async function ensureSchema(client) {
  if (schemaReady) return;
  try {
    await client.query(SCHEMA_SQL);
  } catch (e) {
    // 처음 접속이 동시에 몰려 테이블 생성이 겹친 경우 한 번 더 시도
    await new Promise((r) => setTimeout(r, 300));
    await client.query(SCHEMA_SQL);
  }
  const { rows } = await client.query('select count(*)::int as n from tg_groups');
  if (rows[0].n < GROUP_COUNT) {
    for (let i = 1; i <= GROUP_COUNT; i++) {
      await client.query(
        'insert into tg_groups (id, name, material, code) values ($1,$2,$3,$4) on conflict (id) do nothing',
        [i, `${i}모둠`, GROUP_MATERIAL[i - 1], randomCode()]
      );
      for (const k of RAW_KEYS) {
        await client.query(
          'insert into tg_inventory (group_id, item, qty) values ($1,$2,0) on conflict do nothing',
          [i, k]
        );
      }
    }
  }
  schemaReady = true;
}

/* ================= 인증 ================= */
const teacherPin = () => process.env.TEACHER_PIN || '';
function checkPin(p) {
  if (!teacherPin()) fail(500, 'Netlify 환경변수 TEACHER_PIN(교사 비밀번호)이 설정되지 않았어요.');
  return p === teacherPin();
}
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
    'select id, name, material from tg_groups where id=$1 and code=$2',
    [gid, code]
  );
  if (!rows[0]) fail(401, '입장 정보가 맞지 않아요. 다시 입장해 주세요.');
  return rows[0];
}

/* ================= 데이터 읽기·점수 계산 ================= */
async function getGame(client) {
  const { rows } = await client.query(
    'select phase, ends_at, production, autarky_seconds, trade_seconds, now() as now from tg_game where id=1'
  );
  return rows[0];
}
async function loadAll(client) {
  const game = await getGame(client);
  const groups = (await client.query('select * from tg_groups order by id')).rows;
  const inv = (await client.query('select group_id, item, qty from tg_inventory')).rows;
  const crafts = (
    await client.query('select group_id, product, phase, count(*)::int as n from tg_crafts group by 1,2,3')
  ).rows;
  const trades = (await client.query('select * from tg_trades order by id')).rows;
  return { game, groups, inv, crafts, trades };
}

function missionInfo(key, ctx) {
  const m = MISSIONS[key];
  if (!m) return null;
  let progress = 0;
  switch (key) {
    case 'partners3': progress = ctx.partners; break;
    case 'accept2':   progress = ctx.accepts; break;
    case 'lv3':       progress = ctx.lv[3]; break;
    case 'allLevels': progress = [1, 2, 3].filter((l) => ctx.lv[l] > 0).length; break;
    case 'lv2x2':     progress = ctx.lv[2]; break;
    case 'lv1x3':     progress = ctx.lv[1]; break;
  }
  return {
    key, title: m.title, emoji: m.emoji, desc: m.desc, bonus: m.bonus, target: m.target,
    progress: Math.min(progress, m.target), done: progress >= m.target,
  };
}

function computeStats(all) {
  const accepted = all.trades.filter((t) => t.status === 'accepted');
  const out = {};
  for (const g of all.groups) {
    const inventory = Object.fromEntries(RAW_KEYS.map((k) => [k, 0]));
    for (const r of all.inv) if (r.group_id === g.id) inventory[r.item] = r.qty;

    const crafts = {};
    const lv = { 1: 0, 2: 0, 3: 0 };
    let craftScore = 0, autarkyScore = 0, tradeScore = 0;
    for (const r of all.crafts) {
      if (r.group_id !== g.id) continue;
      const p = PRODUCTS[r.product];
      if (!p) continue;
      const pts = LEVELS[p.level].points * r.n;
      crafts[r.product] = (crafts[r.product] || 0) + r.n;
      lv[p.level] += r.n;
      craftScore += pts;
      if (r.phase === 'autarky') autarkyScore += pts; else tradeScore += pts;
    }

    const mine = accepted.filter((t) => t.from_group === g.id || t.to_group === g.id);
    const partners = new Set(mine.map((t) => (t.from_group === g.id ? t.to_group : t.from_group)));
    const accepts = accepted.filter((t) => t.to_group === g.id).length;
    const mission = g.mission ? missionInfo(g.mission, { lv, partners: partners.size, accepts }) : null;
    const bonus = mission && mission.done ? mission.bonus : 0;

    out[g.id] = {
      id: g.id, name: g.name, material: g.material, inventory, crafts,
      craftScore, autarkyScore, tradeScore, bonus, total: craftScore + bonus,
      tradeCount: mine.length, mission,
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
    raw: RAW, products: PRODUCTS, levels: LEVELS, missions: MISSIONS, limits: LIMITS,
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
  const ranked = Object.values(stats).sort((a, b) => b.total - a.total || a.id - b.id);
  const sumOf = (f) => Object.values(stats).reduce((s, x) => s + x[f], 0);
  const compare = { autarky: sumOf('autarkyScore'), trade: sumOf('tradeScore'), bonus: sumOf('bonus') };
  const gm = all.game;

  const base = {
    serverNow: gm.now,
    game: {
      phase: gm.phase, endsAt: gm.ends_at, production: gm.production,
      autarkySeconds: gm.autarky_seconds, tradeSeconds: gm.trade_seconds,
    },
    config: publicConfig(all.groups),
    compare,
  };

  if (teacher) {
    const now = new Date(gm.now).getTime();
    const trades = [...all.trades].reverse().slice(0, 80).map(tradeOut);
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
    });
  }

  const me = stats[g.id];
  const tradesAsc = all.trades;
  const out = {
    ...base,
    role: 'group',
    me,
    board: ranked.map((s) => ({ id: s.id, name: s.name, material: s.material, total: s.total })),
    incoming: tradesAsc.filter((t) => t.to_group === g.id && t.status === 'pending').reverse().map(tradeOut),
    outgoing: tradesAsc.filter((t) => t.from_group === g.id).reverse().slice(0, 10).map(tradeOut),
    feed: tradesAsc.filter((t) => t.status === 'accepted').reverse().slice(0, 8).map((t) => ({
      id: t.id, from: t.from_group, to: t.to_group,
      giveItem: t.give_item, giveQty: t.give_qty, wantItem: t.want_item, wantQty: t.want_qty,
    })),
  };
  if (gm.phase === 'finished') {
    out.results = ranked.map((s, i) => ({
      rank: i + 1, id: s.id, name: s.name, material: s.material,
      craftScore: s.craftScore, bonus: s.bonus, total: s.total,
      mission: s.mission && { title: s.mission.title, emoji: s.mission.emoji, desc: s.mission.desc, done: s.mission.done },
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
  if (!['autarky', 'trade'].includes(game.phase)) fail(403, '지금은 물품을 만들 수 없어요.');

  return tx(client, async () => {
    const { rows } = await client.query(
      'select item, qty from tg_inventory where group_id=$1 for update', [g.id]
    );
    const have = (k) => rows.find((r) => r.item === k)?.qty ?? 0;
    const missing = Object.entries(p.recipe)
      .filter(([k, n]) => have(k) < n)
      .map(([k]) => RAW[k].name);
    if (missing.length) fail(400, `재료가 부족해요: ${missing.join(', ')}`);
    for (const [k, n] of Object.entries(p.recipe)) {
      await client.query('update tg_inventory set qty=qty-$3 where group_id=$1 and item=$2', [g.id, k, n]);
    }
    await client.query('insert into tg_crafts (group_id, product, phase) values ($1,$2,$3)', [g.id, key, game.phase]);
    return json({ ok: true, product: key, points: LEVELS[p.level].points });
  });
}

async function propose(client, g, b) {
  const to = Number(b.to);
  const giveItem = String(b.giveItem);
  const wantItem = String(b.wantItem);
  const giveQty = Number(b.giveQty);
  const wantQty = Number(b.wantQty);
  const message = String(b.message ?? '').trim();

  if (!Number.isInteger(to) || to < 1 || to > GROUP_COUNT || to === g.id) fail(400, '제안을 보낼 다른 모둠을 골라 주세요.');
  if (!RAW[giveItem] || !RAW[wantItem]) fail(400, '바꿀 물품을 골라 주세요.');
  if (giveItem === wantItem) fail(400, '서로 다른 물품끼리 바꿔야 해요.');
  for (const q of [giveQty, wantQty]) {
    if (!Number.isInteger(q) || q < 1 || q > LIMITS.maxQty) fail(400, `물품 개수는 1~${LIMITS.maxQty}개로 정해 주세요.`);
  }
  if (len(message) < LIMITS.minProposal) fail(400, `무역 제안문을 ${LIMITS.minProposal}글자 이상 써 주세요.`);
  if (len(message) > LIMITS.maxText) fail(400, `무역 제안문은 ${LIMITS.maxText}글자까지 쓸 수 있어요.`);

  const game = await getGame(client);
  if (game.phase !== 'trade') fail(403, '지금은 무역 시간이 아니에요.');

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

async function respond(client, g, b) {
  const id = Number(b.id);
  const accept = b.accept === true;
  const reply = String(b.reply ?? '').trim();
  if (!Number.isInteger(id)) fail(400, '제안을 찾을 수 없어요.');
  if (accept && len(reply) < LIMITS.minReply) fail(400, `수락 답장을 ${LIMITS.minReply}글자 이상 써 주세요.`);
  if (len(reply) > LIMITS.maxText) fail(400, `답장은 ${LIMITS.maxText}글자까지 쓸 수 있어요.`);
  const game = await getGame(client);
  if (game.phase !== 'trade') fail(403, '지금은 무역 시간이 아니에요.');

  const result = await tx(client, async () => {
    const tr = await client.query('select * from tg_trades where id=$1 for update', [id]);
    const t = tr.rows[0];
    if (!t || t.to_group !== g.id) fail(404, '제안을 찾을 수 없어요.');
    if (t.status !== 'pending') fail(409, '이미 처리된 제안이에요.');

    if (!accept) {
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
  await client.query('update tg_groups set mission=null');
}
const grantProduction = (client, n) =>
  client.query(
    'update tg_inventory i set qty = i.qty + $1 from tg_groups g where g.id = i.group_id and g.material = i.item',
    [n]
  );

async function teacherAction(client, action, b) {
  switch (action) {
    case 'start': {
      const production = clampInt(b.production, 1, 10, 4);
      const autarky = clampInt(b.autarkySeconds, 0, 1800, 120);
      const trade = clampInt(b.tradeSeconds, 60, 3600, 720);
      return tx(client, async () => {
        await wipe(client);
        const keys = shuffle(Object.keys(MISSIONS));
        for (let i = 0; i < GROUP_COUNT; i++) {
          await client.query('update tg_groups set mission=$2 where id=$1', [i + 1, keys[i % keys.length]]);
        }
        await grantProduction(client, production);
        await client.query(
          "update tg_game set phase='autarky', ends_at = now() + ($1::int * interval '1 second'), production=$2, autarky_seconds=$1, trade_seconds=$3 where id=1",
          [autarky, production, trade]
        );
        return json({ ok: true });
      });
    }
    case 'open_trade': {
      return tx(client, async () => {
        const { rows } = await client.query('select phase, production, trade_seconds from tg_game where id=1 for update');
        if (rows[0].phase !== 'autarky') fail(409, '자급자족 단계에서만 무역을 열 수 있어요.');
        await grantProduction(client, rows[0].production);
        await client.query(
          "update tg_game set phase='trade', ends_at = now() + ($1::int * interval '1 second') where id=1",
          [rows[0].trade_seconds]
        );
        return json({ ok: true });
      });
    }
    case 'extend': {
      const sec = clampInt(b.seconds, 10, 600, 60);
      const game = await getGame(client);
      if (!['autarky', 'trade'].includes(game.phase)) fail(409, '진행 중일 때만 시간을 늘릴 수 있어요.');
      await client.query(
        "update tg_game set ends_at = greatest(coalesce(ends_at, now()), now()) + ($1::int * interval '1 second') where id=1",
        [sec]
      );
      return json({ ok: true });
    }
    case 'finish': {
      const game = await getGame(client);
      if (!['autarky', 'trade'].includes(game.phase)) fail(409, '진행 중일 때만 끝낼 수 있어요.');
      await client.query("update tg_game set phase='finished', ends_at=null where id=1");
      return json({ ok: true });
    }
    case 'reset': {
      return tx(client, async () => {
        await wipe(client);
        for (let i = 1; i <= GROUP_COUNT; i++) {
          await client.query('update tg_groups set code=$2, last_seen=null where id=$1', [i, randomCode()]);
        }
        await client.query("update tg_game set phase='lobby', ends_at=null where id=1");
        return json({ ok: true });
      });
    }
    default:
      return fail(404, '알 수 없는 요청이에요.');
  }
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
    'select id, name, material from tg_groups where id=$1 and code=$2',
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
    case 'respond': return respond(client, g, body);
    case 'cancel':  return cancel(client, g, body);
    default:        return fail(404, '알 수 없는 요청이에요.');
  }
}

export default async (req) => {
  if (!process.env.DATABASE_URL) {
    return json({ error: 'DATABASE_URL 환경변수가 설정되지 않았어요.' }, 500);
  }
  const route = new URL(req.url).pathname.replace(/^\/api\/?/, '').replace(/\/$/, '');
  const pool = new Pool({ connectionString: process.env.DATABASE_URL });
  let client;
  try {
    client = await pool.connect();
    await ensureSchema(client);
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
