---
name: write-plan
description: Write an implementation plan for a ukiel roadmap row in the house format (TDD tasks, real interfaces, status flips). Use when asked to "write the plan for row N", "plan <feature>", or to turn a design note / review finding into an executable plan.
---

# Writing a ukiel implementation plan

Plans live in `docs/superpowers/plans/YYYY-MM-DD-ukiel-<slug>.md` and are
executed task-by-task by agentic workers. A plan is good when every task is
independently committable, every referenced interface actually exists, and
the whole thing can be executed without re-deriving context. `template.md`
in this skill directory is the skeleton; this file is the process.

## Before writing a line

1. **Confirm the gate.** Plans are written just-in-time: check the roadmap
   (`docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`) that the row's
   prerequisites are **Executed**. If not, say so and stop — a plan against
   unexecuted prerequisites guesses interfaces, which is the one thing the
   convention exists to prevent.
2. **Read the inputs.** The roadmap row, its sketch note (`docs/notes/…`),
   any issues it closes (`docs/issues/…`), and the relevant design-spec
   sections (`docs/superpowers/specs/2026-07-05-ukiel-design.md`).
3. **Verify every interface against the code.** Grep/read each function,
   struct, config field, migration number, and test fixture the plan will
   name. Cite real signatures and `file.rs:line` anchors. Next free
   migration number = `ls crates/ukiel-catalog/migrations/`. Reuse existing
   test fixtures by their actual helper names (or say "adapt to the file's
   actual names" when the executor should follow local convention).
4. **When reality contradicts a source doc, fix the doc** in the same
   change (e.g. plan 28 found the guardrail defaults in four places, not
   the three issue 0009 claimed — the issue was corrected, not papered
   over).

## Writing the plan

Follow `template.md`. The rules that matter:

- **Goal** maps each deliverable to its driver (issue number, design-doc
  section, review finding) — coverage must be checkable.
- **Architecture** states what is new, what is pure/unit-testable without
  Docker, what is behavior-preserving, and why defaults trip nothing
  (**every existing test stays green after every task** is a global
  constraint, so defaults must be chosen to guarantee it).
- **Decision logic is extracted pure.** Threshold/policy functions
  (`flush_decision`, `plan_chunks`, `desired_assignment` are the house
  examples) get unit tests that run with `cargo test -p <crate> --lib`,
  no Docker.
- **Tasks are dependency-ordered and independently committable**, each
  with: Files (Create/Modify/Test), Interfaces (Produces/Consumes with
  real Rust signatures), and TDD checkbox steps — write failing test →
  verify it fails (state the expected failure) → implement (code sketch
  matching surrounding style) → verify pass (exact command) → lint and
  commit (exact conventional message; **no Claude/AI attribution**).
- **Semantics worth a sentence get one** at the decision site: why
  `continue` not `break`, why a cap skips instead of erroring, why a
  bound is exactly-once-safe. The executor should never have to re-derive
  a safety argument.
- **The last task is wiring + status flips:** ukield config
  (`crates/ukield/src/config.rs` + `run.rs`) and `ukield.example.toml`;
  monitoring-spec rows for new metrics; issue statuses → Resolved (or
  interim-shipped, one line saying what shipped); roadmap row → Executed
  (list what landed); pointers in touched notes. Full-workspace check:
  `cargo fmt --check && cargo clippy --all-targets -- -D warnings &&
  make test`, and `make e2e` when storage-lifecycle invariants are
  touched (S8 is the bijection regression net).
- **Global Constraints** copies the roadmap's cross-plan constraints
  (edition/toolchain, dep pins, no-attribution commits, Docker split)
  plus anything plan-specific.
- **Self-review notes** close the file: coverage mapping (every
  driver → task), type consistency (every produced signature matched to
  its consumers' call sites), sequencing constraints, and the safety
  argument restated. Write them by actually re-checking, not by summary.

## After writing

- Roadmap: flip the row's plan cell to the filename and its status to
  **Ready to execute** (keep sequencing notes: "before/after row N
  because …").
- Report back with what was verified against code and anything discovered
  that changed the source docs.

## Sizing guidance

One plan ≈ one roadmap row ≈ a day of landings (plans 17/18 were each
5–6 tasks / 5–8 commits). If the plan grows past ~8 tasks, the row wants
splitting (propose it in the roadmap rather than writing a mega-plan).
Phase-2 material with open design questions (e.g. row 29's slice sealing)
stays in the note until its questions are resolved — name the open
question in the plan's Prerequisites instead of pretending it's settled.
