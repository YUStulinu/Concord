//! concord-check history.json [--budget N]
//!
//! Prints a JSON verdict: {"linearizable": bool, "ops", "keys": [...], "seconds"}; exits 0 when
//! linearizable, 1 when not, 2 when the search gave up (budget) or the input was bad.

use std::time::Instant;

use concord_check::{Op, Verdict, check};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let Some(path) = args.get(1) else {
        eprintln!("usage: concord-check history.json [--budget steps-per-key]");
        std::process::exit(2);
    };
    let budget = args
        .iter()
        .position(|a| a == "--budget")
        .and_then(|i| args.get(i + 1))
        .and_then(|b| b.parse().ok())
        .unwrap_or(50_000_000);
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| {
        eprintln!("{path}: {e}");
        std::process::exit(2);
    });
    let ops: Vec<Op> = serde_json::from_str(&text).unwrap_or_else(|e| {
        eprintln!("{path}: {e}");
        std::process::exit(2);
    });
    let t0 = Instant::now();
    let reports = check(&ops, budget);
    let bad = reports.iter().any(|r| matches!(r.verdict, Verdict::NotLinearizable { .. }));
    let unknown = reports.iter().any(|r| r.verdict == Verdict::Unknown);
    let out = serde_json::json!({
        "linearizable": !bad && !unknown,
        "gave_up": unknown,
        "ops": ops.len(),
        "unknown_outcomes": ops.iter().filter(|o| o.ret.is_none()).count(),
        "seconds": t0.elapsed().as_secs_f64(),
        "keys": reports,
    });
    println!("{}", serde_json::to_string(&out).unwrap());
    std::process::exit(if bad {
        1
    } else if unknown {
        2
    } else {
        0
    });
}
