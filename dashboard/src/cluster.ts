// What the dashboard computes from the servers' /v1/status replies.

export interface EntryInfo {
  index: number;
  term: number;
  conf?: boolean;
}

export interface Status {
  id: number;
  role: string;
  term: number;
  vote: number;
  lead: number;
  commit: number;
  applied: number;
  last_index: number;
  snap_index: number;
  members: { ID: number; Raft: string; HTTP: string }[] | null;
  peers?: { id: number; match: number; next: number }[];
  tail: EntryInfo[] | null;
  faults: { blocked: number[] | null; drop: number; delay_ms: number; jitter_ms: number };
  keys: number;
  syncs: number;
  elections_won: number;
  time_ms: number;
}

/** A server as last seen: its status, or null when it did not answer. */
export interface Seen {
  addr: string;
  status: Status | null;
}

/** A stable colour for a term, so the same term looks the same on every server. */
export function termColour(term: number): string {
  const hue = (term * 137.508) % 360; // golden angle: neighbouring terms differ clearly
  return `hsl(${hue.toFixed(0)} 65% 55%)`;
}

export type Cell = { kind: "entry"; term: number; conf: boolean; committed: boolean } | { kind: "compacted" } | { kind: "empty" } | { kind: "unknown" };

/**
 * The last `width` log positions of every server, aligned by index, so a divergence shows as
 * different colours in one column.
 */
export function logGrid(servers: Seen[], width: number): { from: number; to: number; rows: Cell[][] } {
  const lasts = servers.filter((s) => s.status).map((s) => s.status!.last_index);
  const to = lasts.length ? Math.max(...lasts) : 0;
  const from = Math.max(1, to - width + 1);
  const rows = servers.map((s) => {
    const row: Cell[] = [];
    const st = s.status;
    const byIndex = new Map((st?.tail ?? []).map((e) => [e.index, e]));
    for (let i = from; i <= to; i++) {
      if (!st) row.push({ kind: "unknown" });
      else if (i <= st.snap_index) row.push({ kind: "compacted" });
      else if (i > st.last_index) row.push({ kind: "empty" });
      else {
        const e = byIndex.get(i);
        row.push(e ? { kind: "entry", term: e.term, conf: !!e.conf, committed: i <= st.commit } : { kind: "unknown" });
      }
    }
    return row;
  });
  return { from, to, rows };
}

export interface ClusterEvent {
  at: number;
  text: string;
}

/** Changes worth telling between two polls: new leaders and terms, servers going and coming. */
export function diff(prev: Seen[], next: Seen[], at: number): ClusterEvent[] {
  const out: ClusterEvent[] = [];
  const byAddr = new Map(prev.map((s) => [s.addr, s.status]));
  for (const s of next) {
    const before = byAddr.get(s.addr);
    const now = s.status;
    if (before && !now) out.push({ at, text: `server ${before.id} stopped answering` });
    if (before === null && now) out.push({ at, text: `server ${now.id} is back (term ${now.term}, log up to ${now.last_index})` });
    if (!before || !now) continue;
    if (now.role === "leader" && before.role !== "leader") out.push({ at, text: `server ${now.id} became leader in term ${now.term}` });
    if (before.role === "leader" && now.role !== "leader") out.push({ at, text: `server ${now.id} stepped down (now ${now.role}, term ${now.term})` });
    if (now.snap_index > before.snap_index) out.push({ at, text: `server ${now.id} compacted its log up to ${now.snap_index}` });
    const before_m = (before.members ?? []).map((m) => m.ID).sort().join(",");
    const now_m = (now.members ?? []).map((m) => m.ID).sort().join(",");
    if (before_m !== now_m) out.push({ at, text: `server ${now.id} now sees members {${now_m}}` });
  }
  return out;
}

/** The leader with the highest term among those answering, if any. */
export function leaderOf(servers: Seen[]): Status | undefined {
  return servers
    .map((s) => s.status)
    .filter((s): s is Status => !!s && s.role === "leader")
    .sort((a, b) => b.term - a.term)[0];
}

/** Servers that disagree about who leads: a sign of a partition. */
export function splitBrain(servers: Seen[]): boolean {
  const leaders = servers.map((s) => s.status).filter((s) => s && s.role === "leader");
  return leaders.length > 1;
}
