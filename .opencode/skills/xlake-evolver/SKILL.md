---
name: "xlake-evolver"
description: "Keeps the xlake-context skill synchronized with architecture discussions, implementation changes, and evolving MVP reality. Invoke after meaningful xlake design or code changes."
---

# xlake Evolver

Use this skill to update `xlake-context` so it remains the canonical, current, and implementation-grounded architecture context for xlake.

The target file is:

`/Users/ximin/CodeRepository/xlake-demo/.opencode/skills/xlake-context/SKILL.md`

`xlake-context` is not ordinary docs. It is shared operational memory for future tasks and agents. Treat every update as a context-maintenance change to a canonical source of truth.

---

## When to Invoke

Invoke `xlake-evolver` whenever any of the following happens:

1. An architecture discussion settles a design direction.
2. A code iteration materially changes real behavior, boundaries, responsibilities, or terminology.
3. A feature moves from proposed → partial → implemented.
4. A previous assumption in `xlake-context` becomes outdated, incomplete, or wrong.
5. A review reveals that design intent and implementation reality have diverged.
6. New routing/storage/read/write/transaction semantics are introduced.
7. MVP status changes for any module.

Do **not** invoke for trivial renames, formatting-only changes, or speculative ideas that were not accepted.

---

## Required Goal

Your goal is to make `xlake-context` reflect the **latest stable understanding** of xlake by reconciling:

- architecture discussion outcomes,
- concrete code reality,
- implementation status,
- terminology,
- open gaps and non-implemented areas.

The result must help a future agent quickly answer:

1. What is the intended architecture?
2. What is already implemented?
3. What is only partially implemented or still missing?
4. Which files/classes matter for each subsystem?
5. What recent decisions changed earlier understanding?

---

## Source-of-Truth Order

When updating `xlake-context`, use this priority order:

1. **Current user instructions / accepted architecture decisions in the conversation**
2. **Current codebase reality**
3. **Existing `xlake-context` content**
4. **Historical wording/style in `xlake-context`**

If design intent and implementation differ:

- do **not** silently choose one,
- record both clearly,
- label what is implemented now vs planned next.

Prefer explicit wording such as:

- `Status: Implemented`
- `Status: Partial`
- `Status: Planned`
- `Current implementation:`
- `Design direction:`

---

## Non-Negotiable Rules

1. **Do not hallucinate.** Every substantive update must be traceable to discussion or code evidence.
2. **Do not erase important historical design intent** unless it is clearly superseded.
3. **Do not present planned work as implemented.**
4. **Do not present outdated implementation as current** if code shows otherwise.
5. **Do not bloat the file with raw meeting transcripts or commit logs.** Synthesize.
6. **Do not convert the skill into a changelog.** It is a living context document, not a diary.
7. **Do not remove stable sections casually.** Prefer targeted edits that preserve scanability.
8. **Do not overwrite broad architecture with narrow local implementation details** unless the implementation changes the architecture materially.

---

## Update Workflow

Follow this workflow every time.

### 1. Collect Evidence

Gather only the evidence needed to justify the update:

- the accepted architecture discussion,
- the relevant changed code,
- nearby interfaces/types/config that define the behavior,
- current `xlake-context` wording for the same subsystem.

If the change spans multiple subsystems, gather evidence for each before editing.

### 2. Classify the Change

Classify what changed:

- **Architecture principle**
- **Subsystem boundary/responsibility**
- **Data flow / execution flow**
- **Implementation status**
- **Terminology / naming**
- **Key files / module map**
- **Open gap / future work**

### 3. Decide Edit Scope

Choose the smallest correct edit scope:

- single sentence update,
- row update in a status table,
- section rewrite,
- new subsection,
- key-file list expansion,
- version history entry.

Avoid broad rewrites unless the old section is materially misleading.

### 4. Reconcile Design vs Reality

If needed, explicitly separate:

- intended model,
- current implementation,
- remaining gaps.

Example pattern:

```md
### Read Routing

**Design direction**: Driver-side planning selects DataBlocks before task execution.

**Current implementation**: Block pruning and partition planning exist, but global block metadata is still local-only, so driver-side read planning is incomplete.

**Next gap**: Add a driver-visible global block catalog.
```

### 5. Update Version History

When the change materially affects architecture understanding, add a concise row to `Version History`.

Keep entries short:

| Version | Date | Changes |
|---------|------|---------|
| v0.2 | 2026-04-12 | Clarified write lease vs readonly marker semantics; documented routing-bound lease ownership and current read-planning gaps. |

If no versioning scheme exists yet, continue using a simple monotonic `v0.x` style unless the project already uses another convention.

### 6. Validate the Document

Before finishing, verify:

- terminology is internally consistent,
- statuses match reality,
- file paths/classes still exist,
- no section now contradicts another,
- new content is concise and high-signal.

---

## Preferred Edit Patterns

### A. Architecture Settled in Discussion, Code Not Fully Landed Yet

Document the decision as accepted design, but mark implementation honestly.

### B. Code Landed and Changed Reality

Update architecture wording to match code and downgrade old wording to planned/history only if needed.

### C. Implementation Is Partial

Prefer explicit split:

- what exists,
- what is missing,
- what still depends on future work.

### D. A Section Is Too Generic

Replace vague statements with concrete subsystem semantics and key files.

Bad:

`Routing decides where data goes.`

Good:

`Write routing resolves shard owner on the executor task path, prefers same-executor or same-node delivery, and falls back to driver forwarding when direct delivery fails.`

---

## Section-by-Section Maintenance Guidance

Use these rules when editing `xlake-context`.

### Core Design Principles

Update only for foundational, durable rules. Do not add temporary tactics here.

### Architecture / Module Responsibilities

Update when boundaries between Frontend / Metastore / Backend / Driver / Executor materially change.

### Table System

Update when TableOp semantics, planner responsibilities, or table types evolve.

### Storage System

Update when LMDB instance lifecycle, flush flow, block abstraction, hot/cold layering, or ownership semantics change.

### Expression / LazyUpdate / Transaction

Update when execution semantics or correctness guarantees change.

### Data Routing

Update when shard ownership, routing epochs, preferred locality, forwarding, endpoint behavior, or read/write planning changes.

### Spark Integration

Update when DataSource V2 flow, scan/write builders, planning responsibilities, or Spark extensions change.

### MVP Modules Table

Always keep this honest. If a subsystem is only partly wired end-to-end, it is `🔄 Partial`, not `✅ Basic`.

### Quick Reference / Key Files

Update when the canonical entry points for a subsystem change. Keep this highly practical.

---

## What Good Updates Look Like

Good updates are:

- specific,
- evidence-backed,
- concise,
- architecture-relevant,
- implementation-aware,
- easy for another agent to load and trust.

Good update examples:

- clarify that readonly marker and write lease are separate mechanisms,
- document that write lease ownership is bound to `executorId + epoch`,
- note that all successful writes now converge through executor endpoint dedupe,
- capture that read routing is intended to be driver-planned but may still lack a global block catalog,
- update the key files list to include newly central classes.

---

## What Bad Updates Look Like

Avoid:

- copying raw code comments into the skill,
- documenting speculative future systems as if committed,
- adding low-level method details with no architecture value,
- failing to mention implementation gaps,
- leaving contradictory old text in place after a design change,
- making the skill longer without making it clearer.

---

## Output Contract When Using This Skill

When you finish an `xlake-context` update, report back with:

1. **What changed in `xlake-context`**
2. **Why it changed**
3. **Which evidence it was based on**
4. **Any remaining ambiguity or gaps not yet reflected as implemented reality**

If there was not enough evidence to update safely, say so explicitly and do not force an edit.

---

## Default Target Areas for Future Updates

Expect frequent updates in these areas:

- read/write path end-to-end flow,
- LMDB instance lifecycle and lease semantics,
- routing ownership and epoch fencing,
- block metadata and read planning,
- Spark DataSource V2 integration points,
- MVP module completion status,
- key file maps for newly central classes.

---

## One-Sentence Mission

Keep `xlake-context` as the shortest document that still gives future agents the most accurate possible understanding of xlake's current architecture and implementation reality.
