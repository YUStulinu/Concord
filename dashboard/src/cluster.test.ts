import { describe, expect, it } from "vitest";
import { diff, leaderOf, logGrid, splitBrain, termColour, type Seen, type Status } from "./cluster";

function st(id: number, over: Partial<Status> = {}): Status {
  return {
    id, role: "follower", term: 2, vote: 0, lead: 1, commit: 5, applied: 5, last_index: 6, snap_index: 0,
    members: [{ ID: 1, Raft: "", HTTP: "" }, { ID: 2, Raft: "", HTTP: "" }, { ID: 3, Raft: "", HTTP: "" }],
    tail: [1, 2, 3, 4, 5, 6].map((i) => ({ index: i, term: i <= 3 ? 1 : 2, conf: i <= 3 })),
    faults: { blocked: null, drop: 0, delay_ms: 0, jitter_ms: 0 }, keys: 0, syncs: 0, elections_won: 0, time_ms: 0,
    ...over,
  };
}

describe("logGrid", () => {
  it("aligns servers by index and marks what is committed, compacted, missing", () => {
    const servers: Seen[] = [
      { addr: "a", status: st(1, { role: "leader" }) },
      { addr: "b", status: st(2, { last_index: 4, commit: 4, tail: st(2).tail!.slice(0, 4) }) },
      { addr: "c", status: st(3, { snap_index: 3, tail: st(3).tail!.slice(3) }) },
      { addr: "d", status: null },
    ];
    const g = logGrid(servers, 4);
    expect([g.from, g.to]).toEqual([3, 6]);
    expect(g.rows[0].map((c) => c.kind)).toEqual(["entry", "entry", "entry", "entry"]);
    expect(g.rows[0][3]).toEqual({ kind: "entry", term: 2, conf: false, committed: false });
    expect(g.rows[1].map((c) => c.kind)).toEqual(["entry", "entry", "empty", "empty"]);
    expect(g.rows[2][0].kind).toBe("compacted");
    expect(g.rows[3].every((c) => c.kind === "unknown")).toBe(true);
  });
});

describe("events", () => {
  it("notices leaders, crashes, compaction and membership", () => {
    const before: Seen[] = [{ addr: "a", status: st(1, { role: "leader" }) }, { addr: "b", status: st(2) }, { addr: "c", status: null }];
    const after: Seen[] = [
      { addr: "a", status: null },
      { addr: "b", status: st(2, { role: "leader", term: 3, snap_index: 4, members: [{ ID: 2, Raft: "", HTTP: "" }] }) },
      { addr: "c", status: st(3) },
    ];
    const texts = diff(before, after, 0).map((e) => e.text);
    expect(texts).toContain("server 1 stopped answering");
    expect(texts).toContain("server 2 became leader in term 3");
    expect(texts).toContain("server 2 compacted its log up to 4");
    expect(texts).toContain("server 2 now sees members {2}");
    expect(texts.some((t) => t.startsWith("server 3 is back"))).toBe(true);
  });

  it("finds the current leader and two-leader moments", () => {
    const s: Seen[] = [{ addr: "a", status: st(1, { role: "leader", term: 2 }) }, { addr: "b", status: st(2, { role: "leader", term: 3 }) }];
    expect(leaderOf(s)?.id).toBe(2);
    expect(splitBrain(s)).toBe(true);
    expect(splitBrain([s[0]])).toBe(false);
  });

  it("gives each term its own colour", () => {
    expect(termColour(3)).toBe(termColour(3));
    expect(termColour(3)).not.toBe(termColour(4));
  });
});
