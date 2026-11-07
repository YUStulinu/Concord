import "./style.css";
import { diff, leaderOf, logGrid, splitBrain, termColour, type ClusterEvent, type Seen, type Status } from "./cluster";

// The servers to watch: ?nodes=127.0.0.1:8001,127.0.0.1:8002 (default: the demo cluster).
const params = new URLSearchParams(location.search);
const ADDRS = (params.get("nodes") ?? "127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003,127.0.0.1:8004,127.0.0.1:8005")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const $ = <T extends HTMLElement>(id: string) => document.getElementById(id) as T;
let seen: Seen[] = ADDRS.map((addr) => ({ addr, status: null }));
let firstPoll = true;
const events: ClusterEvent[] = [];
let loading = false;
let written = 0;
let failed = 0;

function el(tag: string, cls?: string, text?: string): HTMLElement {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text !== undefined) e.textContent = text;
  return e;
}

async function call(addr: string, path: string, body?: unknown, timeout = 600): Promise<Response> {
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), timeout);
  try {
    return await fetch(`http://${addr}${path}`, {
      method: body === undefined ? "GET" : "POST",
      body: body === undefined ? undefined : JSON.stringify(body),
      headers: body === undefined ? undefined : { "Content-Type": "application/json" },
      signal: ctl.signal,
    });
  } finally {
    clearTimeout(t);
  }
}

async function poll() {
  const next: Seen[] = await Promise.all(
    ADDRS.map(async (addr) => {
      try {
        const r = await call(addr, "/v1/status", undefined, 400);
        return { addr, status: (await r.json()) as Status };
      } catch {
        return { addr, status: null };
      }
    }),
  );
  // The first poll has nothing to compare with: servers are not "back", they were never seen.
  if (!firstPoll) events.unshift(...diff(seen, next, Date.now()).reverse());
  firstPoll = false;
  events.splice(60);
  seen = next;
  render();
}

function idsExcept(id: number): number[] {
  return seen.map((s) => s.status?.id).filter((x): x is number => x !== undefined && x !== id);
}

async function isolate(target: Seen) {
  const id = target.status?.id;
  if (id === undefined) return;
  // Both sides drop the links: the target blocks everyone, everyone blocks the target.
  await Promise.allSettled(
    seen.map((s) => {
      const sid = s.status?.id;
      if (sid === undefined) return Promise.resolve();
      const blocked = sid === id ? idsExcept(id) : [id];
      return call(s.addr, "/v1/admin/faults", { blocked, drop: 0, delay_ms: 0, jitter_ms: 0 });
    }),
  );
}

async function healAll() {
  await Promise.allSettled(
    seen.flatMap((s) => [call(s.addr, "/v1/admin/faults", { blocked: [], drop: 0, delay_ms: 0, jitter_ms: 0 }), call(s.addr, "/v1/admin/clock", { rate: 1 })]),
  );
}

async function writeLoop() {
  let n = 0;
  while (loading) {
    const leader = leaderOf(seen);
    const target = seen.find((s) => s.status && leader && s.status.id === leader.id) ?? seen[0];
    try {
      const r = await call(target.addr, "/v1/kv", { op: "put", key: `k${n % 10}`, value: `dashboard-${n}` }, 1500);
      if (r.ok) written++;
      else failed++;
    } catch {
      failed++;
    }
    n++;
    await new Promise((res) => setTimeout(res, 40));
  }
}

function serverCard(s: Seen): HTMLElement {
  const card = el("article", "server");
  const st = s.status;
  if (!st) {
    card.classList.add("down");
    card.append(el("h3", undefined, s.addr), el("p", "role down", "not answering"));
    return card;
  }
  card.classList.add(st.role);
  const head = el("div", "head");
  head.append(el("h3", undefined, `server ${st.id}`), el("span", `role ${st.role}`, st.role));
  card.append(head);
  const dl = el("dl");
  const row = (k: string, v: string) => dl.append(el("dt", undefined, k), el("dd", undefined, v));
  row("term", String(st.term));
  row("log", `${st.last_index}${st.snap_index ? ` (from ${st.snap_index + 1})` : ""}`);
  row("committed", String(st.commit));
  row("applied", String(st.applied));
  row("keys", String(st.keys));
  row("fsyncs", String(st.syncs));
  card.append(dl);
  const f = st.faults;
  const faults: string[] = [];
  if (f.blocked?.length) faults.push(`cut off from ${f.blocked.join(", ")}`);
  if (f.drop) faults.push(`drops ${(f.drop * 100).toFixed(0)}%`);
  if (f.delay_ms || f.jitter_ms) faults.push(`delay ${f.delay_ms}+${f.jitter_ms} ms`);
  if (faults.length) card.append(el("p", "faults", faults.join(" · ")));
  if (st.peers?.length) {
    const p = el("p", "peers");
    p.textContent = "replicated: " + st.peers.map((x) => `${x.id}→${x.match}`).join("  ");
    card.append(p);
  }
  const buttons = el("div", "buttons");
  const b1 = el("button", undefined, "Isolate") as HTMLButtonElement;
  b1.onclick = () => void isolate(s);
  const b2 = el("button", undefined, "Crash") as HTMLButtonElement;
  b2.onclick = () => void call(s.addr, "/v1/admin/crash", {}).catch(() => {});
  const clock = el("select") as HTMLSelectElement;
  for (const r of [0.25, 0.5, 1, 2, 4]) {
    const o = document.createElement("option");
    o.value = String(r);
    o.textContent = `clock ${r}×`;
    if (r === 1) o.selected = true;
    clock.append(o);
  }
  clock.onchange = () => void call(s.addr, "/v1/admin/clock", { rate: Number(clock.value) });
  buttons.append(b1, b2, clock);
  card.append(buttons);
  return card;
}

function render() {
  const leader = leaderOf(seen);
  const up = seen.filter((s) => s.status).length;
  let line = `${up} of ${seen.length} servers answering · `;
  line += leader ? `leader: server ${leader.id}, term ${leader.term}, commit ${leader.commit}` : "no leader";
  if (splitBrain(seen)) line += " · two servers think they lead (the old one is cut off and will step down)";
  if (loading || written) line += ` · writes: ${written} ok, ${failed} failed`;
  $("headline").textContent = line;

  $("servers").replaceChildren(...seen.map(serverCard));

  const g = logGrid(seen, 40);
  const table = el("div", "grid");
  table.style.gridTemplateColumns = `90px repeat(${g.to - g.from + 1}, minmax(10px, 1fr))`;
  table.append(el("div", "label", "index"));
  for (let i = g.from; i <= g.to; i++) table.append(el("div", "idx", i % 5 === 0 ? String(i) : ""));
  g.rows.forEach((row, r) => {
    table.append(el("div", "label", seen[r].status ? `server ${seen[r].status!.id}` : seen[r].addr));
    for (const c of row) {
      const d = el("div", `cell ${c.kind}`);
      if (c.kind === "entry") {
        d.style.background = termColour(c.term);
        d.title = `term ${c.term}${c.conf ? ", membership change" : ""}${c.committed ? "" : ", not committed"}`;
        if (!c.committed) d.classList.add("uncommitted");
        if (c.conf) d.classList.add("conf");
      }
      table.append(d);
    }
  });
  $("logs").replaceChildren(table);

  $("events").replaceChildren(
    ...events.map((e) => {
      const li = el("li");
      li.append(el("time", undefined, new Date(e.at).toLocaleTimeString()), document.createTextNode(e.text));
      return li;
    }),
  );
}

$("heal").onclick = () => void healAll();
$("load").onclick = () => {
  loading = !loading;
  $("load").textContent = loading ? "Stop writes" : "Start writes";
  if (loading) void writeLoop();
};
setInterval(() => void poll(), 300);
void poll();
