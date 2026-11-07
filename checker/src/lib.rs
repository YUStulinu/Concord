//! Linearizability checking for key-value histories.
//!
//! A history is a list of operations with the time each was called and returned. It is
//! linearizable if every operation can be given a single instant between its call and its
//! return such that, in that order, the operations make sense one after another for a single
//! copy of the store (Herlihy and Wing, 1990).
//!
//! The search is Wing and Gong's, with Lowe's memoisation of (operations linearized so far,
//! state): repeatedly take an operation that could go next -- one called before any
//! not-yet-linearized operation returned -- apply it to the model, and backtrack when stuck.
//! Keys are independent objects, so each key's operations are checked on their own
//! (linearizability is compositional), which keeps the search small.
//!
//! An operation whose outcome is unknown (a timeout) has no return time: it may have taken effect
//! at any point after its call, or never. The model gives it two alternatives, "applied" and
//! "not applied", tried in turn.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct Outcome {
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub found: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct Op {
    pub process: u64,
    pub op: String,
    pub key: String,
    #[serde(default)]
    pub value: Option<String>,
    /// cas only: the value expected; null means "the key is absent". Missing for other ops.
    #[serde(default, deserialize_with = "present")]
    pub expect: Option<Option<String>>,
    pub call: u64,
    /// None: the outcome is unknown.
    pub ret: Option<u64>,
    #[serde(default)]
    pub ok: Option<bool>,
    #[serde(default)]
    pub result: Option<Outcome>,
}

fn present<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Option<String>>, D::Error> {
    Ok(Some(Option::deserialize(d)?))
}

type State = Option<String>;

/// The register model: the state after `op` (alternative `alt`), or None if the op's recorded
/// result is impossible in `state`.
fn step(state: &State, op: &Op, alt: usize) -> Option<State> {
    let known = op.ret.is_some();
    if !known && alt == 1 {
        return Some(state.clone()); // it never happened
    }
    let res = op.result.as_ref();
    match op.op.as_str() {
        "get" => {
            let r = res?;
            let seen = if r.found { r.value.clone() } else { None };
            (seen == *state).then(|| state.clone())
        }
        "put" => Some(op.value.clone()),
        "delete" => {
            if known
                && let Some(r) = res
                && r.found != state.is_some()
            {
                return None;
            }
            Some(None)
        }
        "cas" => {
            let expect = op.expect.clone().unwrap_or(None);
            let matches = *state == expect;
            if !known {
                return Some(if matches { op.value.clone() } else { state.clone() });
            }
            match op.ok {
                Some(true) => matches.then(|| op.value.clone()),
                _ => {
                    // A failed cas reports what it found.
                    if matches {
                        return None;
                    }
                    if let Some(r) = res {
                        let seen = if r.found { r.value.clone() } else { None };
                        if seen != *state {
                            return None;
                        }
                    }
                    Some(state.clone())
                }
            }
        }
        _ => None,
    }
}

fn alternatives(op: &Op) -> usize {
    if op.ret.is_some() { 1 } else { 2 }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub enum Verdict {
    Linearizable,
    /// No order works. `stuck_at`: the candidates when the search got deepest.
    NotLinearizable {
        linearized: usize,
        stuck_at: Vec<Op>,
    },
    /// Gave up after the step budget.
    Unknown,
}

#[derive(Debug, Clone, Serialize)]
pub struct KeyReport {
    pub key: String,
    pub ops: usize,
    pub verdict: Verdict,
    pub steps: u64,
}

#[derive(Clone, Copy)]
struct Event {
    op: usize,
    call: bool,
    time: u64,
}

/// Checks one key's operations.
pub fn check_key(ops: &[Op], budget: u64) -> (Verdict, u64) {
    let n = ops.len();
    if n == 0 {
        return (Verdict::Linearizable, 0);
    }
    // Events in time order; unknown returns go at the end. At equal times calls come first:
    // an operation's call must precede its own return, and two operations whose call and
    // return share a timestamp are treated as overlapping.
    let mut events: Vec<Event> = Vec::with_capacity(2 * n);
    for (i, op) in ops.iter().enumerate() {
        events.push(Event { op: i, call: true, time: op.call });
        events.push(Event { op: i, call: false, time: op.ret.unwrap_or(u64::MAX) });
    }
    events.sort_by_key(|e| (e.time, !e.call));
    let m = events.len();
    // A doubly linked list over events, with a head sentinel at index m.
    const END: usize = usize::MAX;
    let head = m;
    let mut next: Vec<usize> = (1..=m).collect();
    next[m - 1] = END;
    next.push(0);
    let mut prev: Vec<usize> = vec![0; m + 1];
    for (i, p) in prev.iter_mut().enumerate().take(m) {
        *p = if i == 0 { head } else { i - 1 };
    }
    let mut call_of = vec![0; n];
    let mut ret_of = vec![0; n];
    for (i, e) in events.iter().enumerate() {
        if e.call {
            call_of[e.op] = i;
        } else {
            ret_of[e.op] = i;
        }
    }
    fn unlink(next: &mut [usize], prev: &mut [usize], i: usize) {
        let (p, nx) = (prev[i], next[i]);
        next[p] = nx;
        if nx != usize::MAX {
            prev[nx] = p;
        }
    }
    fn relink(next: &mut [usize], prev: &mut [usize], i: usize) {
        let (p, nx) = (prev[i], next[i]);
        next[p] = i;
        if nx != usize::MAX {
            prev[nx] = i;
        }
    }

    let words = n.div_ceil(64);
    let mut done = vec![0u64; words];
    let mut cache: HashSet<(Vec<u64>, State)> = HashSet::new();
    let mut state: State = None;
    // (op, state before, alternative used)
    let mut stack: Vec<(usize, State, usize)> = Vec::new();
    let mut cur = next[head];
    let mut start_alt = 0usize;
    let mut steps = 0u64;
    let mut deepest = 0usize;
    let mut stuck: Vec<usize> = Vec::new();

    while next[head] != END {
        steps += 1;
        if steps > budget {
            return (Verdict::Unknown, steps);
        }
        let e = events[cur];
        if e.call {
            let op = e.op;
            let mut advanced = false;
            for alt in start_alt..alternatives(&ops[op]) {
                if let Some(ns) = step(&state, &ops[op], alt) {
                    done[op / 64] |= 1 << (op % 64);
                    if cache.insert((done.clone(), ns.clone())) {
                        stack.push((op, std::mem::replace(&mut state, ns), alt));
                        unlink(&mut next, &mut prev, call_of[op]);
                        unlink(&mut next, &mut prev, ret_of[op]);
                        cur = next[head];
                        advanced = true;
                        break;
                    }
                    done[op / 64] &= !(1 << (op % 64));
                }
            }
            start_alt = 0;
            if !advanced {
                cur = next[cur];
            }
        } else {
            // A return of an operation not yet placed: something earlier must change.
            if stack.len() >= deepest {
                deepest = stack.len();
                stuck.clear();
                let mut i = next[head];
                while i != END && stuck.len() < 6 {
                    if events[i].call {
                        stuck.push(events[i].op);
                    }
                    i = next[i];
                }
            }
            let Some((op, before, alt)) = stack.pop() else {
                let stuck_at = stuck.iter().map(|&i| ops[i].clone()).collect();
                return (Verdict::NotLinearizable { linearized: deepest, stuck_at }, steps);
            };
            done[op / 64] &= !(1 << (op % 64));
            state = before;
            relink(&mut next, &mut prev, ret_of[op]);
            relink(&mut next, &mut prev, call_of[op]);
            cur = call_of[op];
            start_alt = alt + 1;
        }
    }
    (Verdict::Linearizable, steps)
}

/// Checks a whole history, key by key, in parallel.
pub fn check(ops: &[Op], budget: u64) -> Vec<KeyReport> {
    let mut by_key: HashMap<&str, Vec<Op>> = HashMap::new();
    for op in ops {
        by_key.entry(op.key.as_str()).or_default().push(op.clone());
    }
    let mut keys: Vec<(String, Vec<Op>)> = by_key.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
    keys.sort_by(|a, b| a.0.cmp(&b.0));
    std::thread::scope(|s| {
        let handles: Vec<_> = keys
            .iter()
            .map(|(k, v)| {
                s.spawn(move || {
                    let (verdict, steps) = check_key(v, budget);
                    KeyReport { key: k.clone(), ops: v.len(), verdict, steps }
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn op(
        process: u64,
        kind: &str,
        value: Option<&str>,
        call: u64,
        ret: Option<u64>,
        result: Option<(bool, Option<&str>)>,
    ) -> Op {
        Op {
            process,
            op: kind.into(),
            key: "k".into(),
            value: value.map(String::from),
            expect: None,
            call,
            ret,
            ok: Some(true),
            result: result.map(|(found, v)| Outcome { found, value: v.map(String::from) }),
        }
    }

    fn get(p: u64, call: u64, ret: u64, seen: Option<&str>) -> Op {
        op(p, "get", None, call, Some(ret), Some((seen.is_some(), seen)))
    }

    fn put(p: u64, v: &str, call: u64, ret: Option<u64>) -> Op {
        op(p, "put", Some(v), call, ret, Some((false, None)))
    }

    fn verdict(ops: &[Op]) -> Verdict {
        check_key(ops, 1_000_000).0
    }

    fn bad(v: Verdict) -> bool {
        matches!(v, Verdict::NotLinearizable { .. })
    }

    #[test]
    fn sequential_histories() {
        assert_eq!(verdict(&[put(1, "a", 0, Some(1)), get(1, 2, 3, Some("a"))]), Verdict::Linearizable);
        assert!(bad(verdict(&[put(1, "a", 0, Some(1)), get(1, 2, 3, Some("b"))])));
        assert_eq!(verdict(&[get(1, 0, 1, None)]), Verdict::Linearizable);
    }

    #[test]
    fn concurrency_allows_either_order() {
        // The read overlaps the write: it may see the old or the new value.
        let w = put(1, "a", 0, Some(10));
        assert_eq!(verdict(&[w.clone(), get(2, 5, 6, None)]), Verdict::Linearizable);
        assert_eq!(verdict(&[w.clone(), get(2, 5, 6, Some("a"))]), Verdict::Linearizable);
        // A stale read: after the write returned, the old value is gone.
        assert!(bad(verdict(&[w, get(2, 11, 12, None)])));
        // Reads that disagree about the order of two writes.
        let h = [
            put(1, "a", 0, Some(10)),
            put(2, "b", 0, Some(10)),
            get(3, 11, 12, Some("a")),
            get(4, 13, 14, Some("b")),
            get(5, 15, 16, Some("a")),
        ];
        assert!(bad(verdict(&h)));
    }

    #[test]
    fn unknown_outcomes_may_or_may_not_have_happened() {
        let lost = put(1, "a", 0, None);
        assert_eq!(verdict(&[lost.clone(), get(2, 5, 6, None)]), Verdict::Linearizable);
        assert_eq!(verdict(&[lost.clone(), get(2, 5, 6, Some("a"))]), Verdict::Linearizable);
        // But not both: once seen, it cannot vanish.
        assert!(bad(verdict(&[lost, get(2, 5, 6, Some("a")), get(3, 7, 8, None)])));
    }

    #[test]
    fn compare_and_set() {
        let mut cas = op(2, "cas", Some("b"), 2, Some(3), Some((true, Some("a"))));
        cas.expect = Some(Some("a".into()));
        let h = [put(1, "a", 0, Some(1)), cas.clone(), get(1, 4, 5, Some("b"))];
        assert_eq!(verdict(&h), Verdict::Linearizable);
        // A cas that claims success though the value was something else.
        let mut wrong = cas.clone();
        wrong.expect = Some(Some("z".into()));
        assert!(bad(verdict(&[put(1, "a", 0, Some(1)), wrong])));
        // A failed cas must report the value it found.
        let mut failed = cas;
        failed.ok = Some(false);
        failed.expect = Some(Some("z".into()));
        assert_eq!(verdict(&[put(1, "a", 0, Some(1)), failed.clone()]), Verdict::Linearizable);
        failed.result = Some(Outcome { found: true, value: Some("q".into()) });
        assert!(bad(verdict(&[put(1, "a", 0, Some(1)), failed])));
    }

    /// Random histories from a real sequential execution, spread out in time so operations
    /// overlap, are always linearizable; making one read see a value never written breaks them.
    #[test]
    fn random_histories() {
        let mut rng = fastrand::Rng::with_seed(9);
        for round in 0..300 {
            let mut state: Option<String> = None;
            let mut ops = Vec::new();
            let mut t = 100u64;
            for i in 0..40 {
                // Each op's true instant is t; its interval reaches a little either side.
                t += 10;
                let call = t - rng.u64(0..25);
                let ret = t + rng.u64(0..25);
                let p = rng.u64(1..6);
                match rng.u8(0..3) {
                    0 => {
                        let v = format!("v{i}");
                        state = Some(v.clone());
                        ops.push(put(p, &v, call, Some(ret)));
                    }
                    1 => ops.push(get(p, call, ret, state.as_deref())),
                    _ => {
                        let o = op(p, "delete", None, call, Some(ret), Some((state.is_some(), None)));
                        state = None;
                        ops.push(o);
                    }
                }
            }
            assert_eq!(verdict(&ops), Verdict::Linearizable, "round {round}");
            if let Some(g) = ops.iter_mut().find(|o| o.op == "get" && o.result.as_ref().is_some_and(|r| r.found)) {
                g.result = Some(Outcome { found: true, value: Some("never".into()) });
                assert!(bad(verdict(&ops)), "round {round}");
            }
        }
    }

    #[test]
    fn reads_json_histories() {
        let text = r#"[{"process":1,"op":"cas","key":"k0","value":"1-2","expect":null,"call":5,"ret":9,"ok":true,"result":{"value":"","found":false}},
                      {"process":2,"op":"get","key":"k0","call":10,"ret":11,"ok":true,"result":{"value":"1-2","found":true}},
                      {"process":3,"op":"put","key":"k1","value":"x","call":1,"ret":null,"ok":null,"result":null}]"#;
        let ops: Vec<Op> = serde_json::from_str(text).unwrap();
        assert_eq!(ops[0].expect, Some(None));
        assert_eq!(ops[1].expect, None);
        let reports = check(&ops, 1_000_000);
        assert!(reports.iter().all(|r| r.verdict == Verdict::Linearizable), "{reports:?}");
    }
}
