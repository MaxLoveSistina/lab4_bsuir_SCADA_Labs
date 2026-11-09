// Мини-SCADA, вариант 8. OPC UA (arOPC :4842) -> веб-интерфейс :8080 -> PostgreSQL
const http = require("http"), fs = require("fs"), path = require("path");
const { Pool } = require("pg");
const { OPCUAClient, AttributeIds, DataType, NodeClass, BrowseDirection,
        MessageSecurityMode, SecurityPolicy } = require("node-opcua-client");

const OPC_URL = process.env.OPC_URL || "opc.tcp://127.0.0.1:4842";
const PORT = 8080;
const pool = new Pool({ host: process.env.DB_HOST || "127.0.0.1", port: 5432, database: "lab4", user: "lab4", password: "lab4local" });

const FLOATS = ["Pressure", "Level", "PumpStartSetpoint", "PumpStopSetpoint", "AlarmPressureLimit"];
const BOOLS = ["AutoMode", "PumpCmd", "PumpRunning", "PumpBlocked", "AlarmHighPressure"];
const TAGS = [...FLOATS, ...BOOLS];
const WRITABLE = ["AutoMode", "PumpCmd"];
const DEADBAND = { Pressure: 1, Level: 0.1 }; // 0,1 % шкалы (1000 кПа и 100 %)
const LOW_LEVEL = 20;

const S = { connected: false, dbOk: false, values: {}, good: {}, events: [] };
let nodes = {}, sess = null, seq = 0;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function q(sql, params) {
  try { const r = await pool.query(sql, params); S.dbOk = true; q.warned = false; return r; }
  catch (e) { if (!q.warned) { console.log("БД:", e.message); q.warned = true; } S.dbOk = false; return null; }
}

// ---------- события и аварии ----------
function addEvent(key, text, active) {
  const ev = { id: ++seq, ts: Date.now(), key, text, active, acked: false, tsAck: null, tsClear: active ? null : Date.now() };
  if (!active) ev.acked = true; // информационные события квитирования не требуют
  S.events.unshift(ev); S.events.length = Math.min(S.events.length, 100);
  q("INSERT INTO events(id,ts,key,text,ts_ack,ts_clear) VALUES($1,to_timestamp($2/1000.0),$3,$4,NULL,$5)",
    [ev.id, ev.ts, key, text, active ? null : new Date(ev.ts)]);
}
function setAlarm(key, text, on) {
  const a = S.events.find((e) => e.key === key && e.active);
  if (on && !a) addEvent(key, text, true);
  if (!on && a) {
    a.active = false; a.tsClear = Date.now();
    q("UPDATE events SET ts_clear=to_timestamp($2/1000.0) WHERE id=$1", [a.id, a.tsClear]);
  }
}
function ack(id) {
  for (const e of S.events) if ((id === "all" || e.id === id) && !e.acked) {
    e.acked = true; e.tsAck = Date.now();
    q("UPDATE events SET ts_ack=to_timestamp($2/1000.0) WHERE id=$1", [e.id, e.tsAck]);
  }
}
function checkAlarms() {
  const v = S.values;
  setAlarm("HighPressure", "Авария: давление выше 700 кПа", v.AlarmHighPressure === true);
  setAlarm("LowLevel", "Предупреждение: уровень ниже 20 %, насос заблокирован", typeof v.Level === "number" && v.Level < LOW_LEVEL);
}

// ---------- архив ----------
const last = {};
function archive() {
  const now = Date.now();
  for (const t of TAGS) {
    const val = S.values[t];
    if (val === undefined || !S.good[t]) continue;
    const num = typeof val === "boolean" ? (val ? 1 : 0) : val;
    const l = last[t];
    let write = false;
    if (t in DEADBAND) write = !l || (now - l.ts >= 1000 && Math.abs(num - l.v) > DEADBAND[t]) || now - l.ts >= 30000;
    else if (BOOLS.includes(t)) write = !l || l.v !== num;
    if (write) {
      last[t] = { ts: now, v: num };
      q("INSERT INTO history(ts,tag,value) VALUES(to_timestamp($1/1000.0),$2,$3)", [now, t, num]);
    }
  }
}

// ---------- OPC UA ----------
async function findNodes(s) {
  const found = {}, tree = [];
  const match = (n) => TAGS.find((t) => n === t || n.endsWith("." + t) || n.endsWith("/" + t));
  async function walk(id, depth) {
    if (depth > 10) return;
    const r = await s.browse({ nodeId: id, browseDirection: BrowseDirection.Forward, referenceTypeId: "HierarchicalReferences",
                               includeSubtypes: true, nodeClassMask: 0, resultMask: 63 });
    for (const ref of r.references || []) {
      const n = ref.browseName.name || "", t = match(n) || match(String(ref.displayName && ref.displayName.text || ""));
      if (tree.length < 60) tree.push("  ".repeat(depth) + n + "  [" + ref.nodeId.toString() + "]");
      if (t && ref.nodeClass === NodeClass.Variable && !found[t]) found[t] = ref.nodeId;
      else if (n !== "Server" && ref.nodeClass !== NodeClass.Method) await walk(ref.nodeId, depth + 1);
    }
  }
  await walk("ObjectsFolder", 0);
  found.__tree = tree;
  return found;
}
// arOPC иногда отдаёт Float с обратным порядком байтов (60 превращается в 4e-41). Разворачиваем обратно.
let warned = false;
function fixFloat(tag, v) {
  if (typeof v !== "number" || v === 0 || !(Math.abs(v) < 1e-20)) return v;
  const b = Buffer.alloc(4); b.writeFloatBE(v, 0); b.reverse();
  if (!warned) { console.log("ВНИМАНИЕ: arOPC отдаёт Float с обратным порядком байтов, применена программная коррекция."); warned = true; }
  return b.readFloatBE(0);
}
async function poll(s) {
  const names = Object.keys(nodes);
  const res = await s.read(names.map((n) => ({ nodeId: nodes[n], attributeId: AttributeIds.Value })));
  res.forEach((dv, i) => {
    const ok = dv.statusCode.value === 0;
    S.good[names[i]] = ok;
    if (ok && dv.value) S.values[names[i]] = FLOATS.includes(names[i]) ? fixFloat(names[i], dv.value.value) : dv.value.value;
  });
  if (Date.now() - (poll.dbg || 0) > 5000) {
    poll.dbg = Date.now();
    res.forEach((dv, i) => { if (["Level", "Pressure", "PumpStartSetpoint", "AutoMode"].includes(names[i]))
      console.log("RAW", names[i], "=", JSON.stringify(dv.value && dv.value.value), "тип:", dv.value && DataType[dv.value.dataType], "статус:", dv.statusCode.name); });
  }
  checkAlarms(); archive();
}
async function opcLoop() {
  for (;;) {
    let client;
    try {
      client = OPCUAClient.create({ applicationName: "MiniSCADA", endpointMustExist: false,
        securityMode: MessageSecurityMode.None, securityPolicy: SecurityPolicy.None, connectionStrategy: { maxRetry: 0 } });
      await client.connect(OPC_URL);
      sess = await client.createSession();
      nodes = await findNodes(sess);
      const tree = nodes.__tree; delete nodes.__tree;
      if (!Object.keys(nodes).length) throw new Error("Теги не найдены. Дерево сервера:\n" + tree.join("\n"));
      const missing = TAGS.filter((t) => !nodes[t]);
      console.log("OPC UA подключен. Найдено тегов:", Object.keys(nodes).length, missing.length ? "Не найдены: " + missing : "");
      S.connected = true; addEvent("Comm", "Связь с arOPC установлена", false);
      for (;;) { await poll(sess); await sleep(1000); }
    } catch (e) { console.log("OPC UA:", e.message); }
    if (S.connected) addEvent("Comm", "Связь с arOPC потеряна", false);
    S.connected = false; S.good = {}; sess = null;
    try { await client.disconnect(); } catch {}
    await sleep(3000);
  }
}
async function writeTag(tag, value) {
  if (!WRITABLE.includes(tag)) throw new Error("Запись запрещена");
  if (!sess || !nodes[tag]) throw new Error("Нет связи с arOPC");
  const sc = await sess.write({ nodeId: nodes[tag], attributeId: AttributeIds.Value,
    value: { value: { dataType: DataType.Boolean, value: !!value } } });
  if (sc.value !== 0) throw new Error("arOPC отклонил запись: " + sc.name);
}

// ---------- HTTP ----------
const body = (req) => new Promise((r) => { let b = ""; req.on("data", (c) => (b += c)); req.on("end", () => { try { r(JSON.parse(b || "{}")); } catch { r({}); } }); });
const json = (res, code, obj) => { res.writeHead(code, { "Content-Type": "application/json; charset=utf-8" }); res.end(JSON.stringify(obj)); };

http.createServer(async (req, res) => {
  const u = new URL(req.url, "http://x");
  try {
    if (u.pathname === "/api/state") return json(res, 200, S);
    if (u.pathname === "/api/write" && req.method === "POST") {
      const b = await body(req);
      try { await writeTag(b.tag, b.value); json(res, 200, { ok: true }); } catch (e) { json(res, 400, { error: e.message }); }
      return;
    }
    if (u.pathname === "/api/ack" && req.method === "POST") { ack((await body(req)).id); return json(res, 200, { ok: true }); }
    if (u.pathname === "/api/history") {
      const r = await q("SELECT tag, extract(epoch from ts)*1000 AS t, value FROM history WHERE ts BETWEEN $1 AND $2 ORDER BY ts LIMIT 50000",
        [new Date(u.searchParams.get("from")), new Date(u.searchParams.get("to"))]);
      const series = {};
      for (const row of r ? r.rows : []) (series[row.tag] = series[row.tag] || []).push([Number(row.t), row.value]);
      return json(res, 200, { series, dbOk: S.dbOk });
    }
    const file = path.join(__dirname, "public", u.pathname === "/" ? "index.html" : u.pathname);
    if (file.startsWith(path.join(__dirname, "public")) && fs.existsSync(file)) {
      res.writeHead(200, { "Content-Type": file.endsWith(".html") ? "text/html; charset=utf-8" : "text/plain" });
      return res.end(fs.readFileSync(file));
    }
    json(res, 404, { error: "not found" });
  } catch (e) { json(res, 500, { error: e.message }); }
}).listen(PORT, process.env.HTTP_HOST || "127.0.0.1", () => console.log("SCADA: http://127.0.0.1:" + PORT));

// ---------- старт ----------
(async () => {
  await q("CREATE TABLE IF NOT EXISTS history(ts timestamptz NOT NULL, tag text NOT NULL, value double precision)");
  await q("CREATE INDEX IF NOT EXISTS history_idx ON history(tag, ts)");
  await q("CREATE TABLE IF NOT EXISTS events(id int PRIMARY KEY, ts timestamptz, key text, text text, ts_ack timestamptz, ts_clear timestamptz)");
  const m = await q("SELECT coalesce(max(id),0) AS m FROM events");
  if (m) seq = m.rows[0].m;
  const old = await q("SELECT id, extract(epoch from ts)*1000 AS ts, key, text, extract(epoch from ts_ack)*1000 AS a, extract(epoch from ts_clear)*1000 AS c FROM events ORDER BY id DESC LIMIT 50");
  if (old) S.events = old.rows.map((r) => ({ id: r.id, ts: Number(r.ts), key: r.key, text: r.text, active: false,
    acked: true, tsAck: r.a && Number(r.a), tsClear: r.c ? Number(r.c) : Number(r.ts) }));
  opcLoop();
})();
