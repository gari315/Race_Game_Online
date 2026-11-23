'use strict';
/*
 * F1 レースゲーム オンライン対戦サーバー
 * Node.js + ws による WebSocket サーバー。
 * - ルームコードはひらがな一文字、最大5人/部屋
 * - レース中の状態は中継のみ（サーバーは最新の state を保持しない）
 * - ゴールの着順はサーバーが確定する
 */
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const MAX_MEMBERS = 5;
const HIRAGANA = [...'あいうえおかきくけこさしすせそたちつてとなにぬねのはひふへほまみむめもやゆよらりるれろわをん'];
const DEFAULT_SETTINGS = { course: 0, laps: 3, ai: true };
const STATE_FIELDS = ['x', 'z', 'heading', 'vx', 'vz', 'steer', 'progress', 'lap'];

/* ---------------- HTTP（ヘルスチェック用） ---------------- */
const server = http.createServer((req, res) => {
  if (req.method === 'GET' && req.url === '/') {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('F1 online server OK');
  } else {
    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Not Found');
  }
});

const wss = new WebSocketServer({ server, maxPayload: 64 * 1024 });

/* ---------------- 部屋の状態 ---------------- */
// rooms: code -> {
//   code, hostId, settings:{course,laps,ai}, phase:'lobby'|'racing',
//   members:[{id,name,ws,spectator}]  // 参加順＝グリッド順
//   racers:[{id,name,status:'racing'|'finished'|'gone',place}]  // そのレースの参加者
//   finishCount, firstGoal, raceOver
// }
const rooms = new Map();

let idCounter = 0;
function newPlayerId() {
  idCounter += 1;
  return 'p' + idCounter.toString(36) + Math.random().toString(36).slice(2, 6);
}

function send(ws, obj) {
  if (ws && ws.readyState === ws.OPEN) {
    try { ws.send(JSON.stringify(obj)); } catch (e) { /* 切断中の送信失敗は無視 */ }
  }
}

function broadcast(room, obj, exceptId) {
  for (const m of room.members) {
    if (exceptId && m.id === exceptId) continue;
    send(m.ws, obj);
  }
}

function roomSnapshot(room) {
  return {
    t: 'room',
    code: room.code,
    hostId: room.hostId,
    members: room.members.map(m => ({ id: m.id, name: m.name, spectator: m.spectator })),
    settings: { course: room.settings.course, laps: room.settings.laps, ai: room.settings.ai },
    phase: room.phase,
  };
}

function broadcastRoom(room) {
  broadcast(room, roomSnapshot(room));
}

function sendError(ws, msg) {
  send(ws, { t: 'error', msg });
}

function isFiniteNumber(v) {
  return typeof v === 'number' && Number.isFinite(v);
}

function cleanName(name) {
  if (typeof name !== 'string') return 'プレイヤー';
  const n = name.trim().slice(0, 12);
  return n.length > 0 ? n : 'プレイヤー';
}

function isValidCode(code) {
  return typeof code === 'string' && [...code].length === 1 && HIRAGANA.includes(code);
}

/* ---------------- レース終了判定 ---------------- */
function checkRaceOver(room) {
  if (room.phase !== 'racing' || room.raceOver) return;
  if (!room.racers || room.racers.length === 0) return;
  const allDone = room.racers.every(r => r.status === 'finished' || r.status === 'gone');
  if (!allDone) return;
  room.raceOver = true;
  const results = room.racers
    .slice()
    .sort((a, b) => {
      if (a.place == null && b.place == null) return 0;
      if (a.place == null) return 1;
      if (b.place == null) return -1;
      return a.place - b.place;
    })
    .map(r => ({ id: r.id, name: r.name, place: r.place }));
  broadcast(room, { t: 'raceOver', results });
}

/* ---------------- 各メッセージの処理 ---------------- */
function handleCreate(ws, msg) {
  if (ws.roomCode) { sendError(ws, '既に部屋に入っています'); return; }
  let code = msg.code;
  if (code === undefined || code === null || code === '') {
    code = HIRAGANA.find(c => !rooms.has(c));
    if (!code) { sendError(ws, '空いているルームコードがありません'); return; }
  } else {
    if (!isValidCode(code)) { sendError(ws, 'ルームコードはひらがな一文字で指定してください'); return; }
    if (rooms.has(code)) { sendError(ws, 'そのルームコードは既に使われています'); return; }
  }
  const room = {
    code,
    hostId: ws.playerId,
    settings: { course: DEFAULT_SETTINGS.course, laps: DEFAULT_SETTINGS.laps, ai: DEFAULT_SETTINGS.ai },
    phase: 'lobby',
    members: [],
    racers: [],
    finishCount: 0,
    firstGoal: false,
    raceOver: false,
  };
  room.members.push({ id: ws.playerId, name: cleanName(msg.name), ws, spectator: false });
  rooms.set(code, room);
  ws.roomCode = code;
  broadcastRoom(room);
}

function handleJoin(ws, msg) {
  if (ws.roomCode) { sendError(ws, '既に部屋に入っています'); return; }
  const room = typeof msg.code === 'string' ? rooms.get(msg.code) : null;
  if (!room) { sendError(ws, '部屋が見つかりません'); return; }
  if (room.members.length >= MAX_MEMBERS) { sendError(ws, '部屋が満員です（最大5人）'); return; }
  // レース中に参加した人は観戦者として入る（次のレースから通常参加）
  const member = {
    id: ws.playerId,
    name: cleanName(msg.name),
    ws,
    spectator: room.phase === 'racing',
  };
  room.members.push(member);
  ws.roomCode = room.code;
  broadcastRoom(room);
}

function handleSettings(ws, room, member, msg) {
  if (room.hostId !== member.id) { sendError(ws, '設定の変更はホストのみ可能です'); return; }
  const s = room.settings;
  if (isFiniteNumber(msg.course)) s.course = Math.max(0, Math.min(99, Math.trunc(msg.course)));
  if (isFiniteNumber(msg.laps)) s.laps = Math.max(1, Math.min(99, Math.trunc(msg.laps)));
  if (typeof msg.ai === 'boolean') s.ai = msg.ai;
  broadcast(room, { t: 'settings', course: s.course, laps: s.laps, ai: s.ai });
}

function handleStart(ws, room, member) {
  if (room.hostId !== member.id) { sendError(ws, 'レースの開始はホストのみ可能です'); return; }
  if (room.phase !== 'lobby') return;
  room.phase = 'racing';
  room.finishCount = 0;
  room.firstGoal = false;
  room.raceOver = false;
  // 観戦者でないメンバーが参加順（＝グリッド順）でレーサーになる
  room.racers = room.members
    .filter(m => !m.spectator)
    .map(m => ({ id: m.id, name: m.name, status: 'racing', place: null }));
  broadcast(room, {
    t: 'start',
    startAt: Date.now() + 3000,
    settings: { course: room.settings.course, laps: room.settings.laps, ai: room.settings.ai },
    members: room.racers.map(r => ({ id: r.id, name: r.name })),
  });
}

function handleState(ws, room, member, msg) {
  if (room.phase !== 'racing') return;
  // レース参加中（走行中）のプレイヤー以外の中継はしない
  const racer = room.racers.find(r => r.id === member.id);
  if (!racer || racer.status !== 'racing') return;
  const out = { t: 'state', id: member.id };
  for (const f of STATE_FIELDS) {
    if (!isFiniteNumber(msg[f])) return; // 有限数でないフィールドがあれば中継しない
    out[f] = msg[f];
  }
  broadcast(room, out, member.id);
}

function handleHit(ws, room, member, msg) {
  const out = Object.assign({}, msg, { t: 'hit', from: member.id });
  const target = typeof msg.target === 'string'
    ? room.members.find(m => m.id === msg.target)
    : null;
  if (target && target.id !== member.id) {
    send(target.ws, out); // 対象本人がいれば本人へ
  } else {
    broadcast(room, out, member.id); // それ以外は本人以外全員へ
  }
}

function handleFinish(ws, room, member) {
  if (room.phase !== 'racing') return;
  const racer = room.racers.find(r => r.id === member.id);
  if (!racer || racer.status !== 'racing') return; // 二重申告・観戦者は無視
  racer.status = 'finished';
  room.finishCount += 1;
  racer.place = room.finishCount;
  broadcast(room, { t: 'finished', id: member.id, place: racer.place });
  if (racer.place === 1) {
    room.firstGoal = true;
    broadcast(room, { t: 'firstGoal' });
  }
  checkRaceOver(room);
}

function handleRetire(ws, room, member) {
  if (room.phase !== 'racing') return;
  const racer = room.racers.find(r => r.id === member.id);
  if (!racer || racer.status !== 'racing') return;
  racer.status = 'gone';
  member.spectator = true; // 観戦者化（車は消える）
  broadcast(room, { t: 'gone', id: member.id, reason: 'retire' });
  broadcastRoom(room);
  checkRaceOver(room);
}

function handleNext(ws, room, member) {
  if (room.phase !== 'racing') return;
  // 1位がゴールした後（またはレース終了後）のみ受理する
  if (!room.firstGoal && !room.raceOver) return;
  room.phase = 'lobby';
  room.racers = [];
  room.finishCount = 0;
  room.firstGoal = false;
  room.raceOver = false;
  for (const m of room.members) m.spectator = false; // 全員の状態をリセット
  broadcast(room, {
    t: 'toLobby',
    settings: { course: room.settings.course, laps: room.settings.laps, ai: room.settings.ai },
  });
  broadcastRoom(room);
}

function handleLeave(ws) {
  const code = ws.roomCode;
  if (!code) return;
  ws.roomCode = null;
  const room = rooms.get(code);
  if (!room) return;
  const idx = room.members.findIndex(m => m.id === ws.playerId);
  if (idx === -1) return;
  const member = room.members[idx];
  room.members.splice(idx, 1);
  if (room.phase === 'racing') {
    const racer = room.racers.find(r => r.id === member.id);
    if (racer && racer.status === 'racing') racer.status = 'gone';
  }
  if (room.members.length === 0) {
    rooms.delete(code); // 部屋が空になったら削除
    return;
  }
  broadcast(room, { t: 'gone', id: member.id, reason: 'left' });
  if (room.hostId === member.id) {
    room.hostId = room.members[0].id; // 参加順で次の現役メンバーがホストに
  }
  broadcastRoom(room);
  checkRaceOver(room);
}

/* ---------------- 接続処理 ---------------- */
wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.playerId = newPlayerId();
  ws.roomCode = null;
  ws.on('pong', () => { ws.isAlive = true; });

  send(ws, { t: 'welcome', id: ws.playerId });

  ws.on('message', (data) => {
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch (e) {
      return; // 不正なJSONは無視
    }
    if (!msg || typeof msg !== 'object' || typeof msg.t !== 'string') return;

    if (msg.t === 'create') { handleCreate(ws, msg); return; }
    if (msg.t === 'join') { handleJoin(ws, msg); return; }

    const room = ws.roomCode ? rooms.get(ws.roomCode) : null;
    if (!room) return;
    const member = room.members.find(m => m.id === ws.playerId);
    if (!member) { ws.roomCode = null; return; }

    switch (msg.t) {
      case 'settings': handleSettings(ws, room, member, msg); break;
      case 'start': handleStart(ws, room, member); break;
      case 'state': handleState(ws, room, member, msg); break;
      case 'hit': handleHit(ws, room, member, msg); break;
      case 'finish': handleFinish(ws, room, member); break;
      case 'retire': handleRetire(ws, room, member); break;
      case 'next': handleNext(ws, room, member); break;
      case 'leave': handleLeave(ws); break;
      default: break; // 未知のメッセージは無視
    }
  });

  ws.on('close', () => { handleLeave(ws); }); // 切断も leave と同じ処理
  ws.on('error', () => { /* close イベント側で処理する */ });
});

/* ---------------- 死活監視（30秒ごとに ping） ---------------- */
const pingTimer = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      ws.terminate(); // 応答のない接続は切断（close 経由で退出処理が走る）
      return;
    }
    ws.isAlive = false;
    try { ws.ping(); } catch (e) { /* 無視 */ }
  });
}, 30000);
wss.on('close', () => clearInterval(pingTimer));

server.listen(PORT, () => {
  console.log('F1 online server listening on port ' + PORT);
});
