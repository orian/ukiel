# Ukiel Plan {N}: {Title} Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** {One paragraph. Map every deliverable to its driver: "(a) **issue NNNN** — …; (b) design doc '§Section' — …". Coverage must be checkable against this list.}

**Architecture:** {One paragraph. What is new vs restructured; which logic is pure/unit-testable without Docker; what is behavior-preserving; why defaults trip no existing test.}

**Tech Stack:** Rust edition 2024, sqlx/Postgres, testcontainers component tests{, + plan-specific}.

**Prerequisites:** {Executed rows this builds on, with what each provides. "None" if ungated. Must-precede constraints ("before row N, which …"). Open design questions deliberately NOT settled here, with a pointer to where they live.}

## Global Constraints

- Rust edition 2024, toolchain ≥ 1.96; workspace dep pins per the roadmap.
- **Every existing test stays green after every task.** {Say why: defaults chosen so nothing trips.}
- Unit tests (`cargo test -p <crate> --lib`) must pass without Docker; component tests via testcontainers.
- Commit messages: conventional style, no Claude/AI attribution.
- Every task ends with `cargo fmt && cargo clippy --all-targets -- -D warnings` clean.
{- Plan-specific constraints.}

---

### Task {K}: {Name} (issue {NNNN} / {driver})

**Files:**
- Create: {path}
- Modify: {path} ({what part — cite `file.rs:line` anchors verified against the code})
- Test: {path}

**Interfaces:**
- Produces: {real Rust signatures — `pub async fn name(arg: Type) -> Result<T, E>` — with one line each on semantics. Defaults and their rationale.}
- Consumes: {existing interfaces, verified to exist}
- {Semantics sentences for anything an executor might re-derive wrongly: skip-vs-error, continue-vs-break, safety arguments (exactly-once, GC bijection, conflict-replan).}

- [ ] **Step 1: Write the failing test**

{Test code, or a commented skeleton naming the real fixture/helpers to reuse — "adapt to the file's actual names" when local convention should win. State the assertions that matter and why.}

- [ ] **Step 2: Verify it fails**

Run: `{exact command}`
Expected: {exact failure — "compile error: X doesn't exist"}

- [ ] **Step 3: Implement**

{Code sketch matching surrounding style — comment density, naming, error handling. Doc comments state constraints/rationale, not narration. Note follow-on effects: "compile errors point at the N call sites; fix each".}

- [ ] **Step 4: Verify pass**

Run: `{exact command}`
Expected: {new + existing tests PASS; name the specific existing test that is the regression net, if one is}

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy -p {crates} --all-targets -- -D warnings
git add {paths}
git commit -m "{type}: {message}"
```

---

{…more tasks, dependency-ordered, each independently committable…}

---

### Task {last}: ukield wiring, example config, docs & issue status flips

**Files:**
- Modify: `crates/ukield/src/config.rs`, `crates/ukield/src/run.rs`
- Modify: `ukield.example.toml`
- Modify: `docs/superpowers/specs/2026-07-06-ukiel-monitoring.md` {if new metrics}
- Modify: `docs/issues/{NNNN}…` + touched notes (status flips)
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md` (row {N} → Executed)

- [ ] **Step 1: Wire config through ukield** {section fields + defaults + `run.rs` construction + example-toml lines with the house comment style}

- [ ] **Step 2: Full verification**

Run: `cargo fmt --check && cargo clippy --all-targets -- -D warnings && make test`
{Plus `make e2e` when storage-lifecycle invariants are touched — name the scenario that is the regression net (e.g. S8 bijection).}

- [ ] **Step 3: Docs and issue status flips** {issues → Resolved/interim with one line on what shipped; roadmap row → **Executed** listing what landed; monitoring-spec metric rows; pointers in touched notes}

- [ ] **Step 4: Commit**

```bash
git add crates/ukield ukield.example.toml docs/
git commit -m "{type}: {wiring}; docs mark plan {N} executed"
```

---

## Self-review notes

- **Coverage:** {every Goal driver → its task; anything deliberately absent, with why and where it is tracked}
- **Type consistency:** {each produced signature matched against its consumers' actual call sites — re-check, don't summarize}
- **Semantics reviewed:** {the continue-vs-break-class decisions, restated}
- **Sequencing:** {inter-task ordering and why; must-precede/-follow rows}
- **Safety argument:** {the invariant this plan leans on (exactly-once, GC bijection, conflict-replan) and why each task preserves it}
