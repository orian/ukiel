# ClickBench adaptations

The 43 queries in `queries.sql` are ClickBench's DataFusion variant
([ClickHouse/ClickBench](https://github.com/ClickHouse/ClickBench), `datafusion/queries.sql`,
Apache-2.0), vendored and adapted for ukiel. `queries-upstream.sql` is the pristine upstream
file, so **`diff queries-upstream.sql queries.sql` is the complete, auditable adaptation set** —
nothing below is a claim you have to take on trust.

The DataFusion variant matters: upstream pins **DataFusion 54.0.0**, which is ukiel's pin too.
So `bench clickbench raw` (bare DataFusion over the same parquet) is running the same engine as
ClickBench's own published DataFusion results — a cross-check on our harness, and the ceiling
the `ukiel / raw` ratio is measured against.

Both runners execute the **same adapted file**. Any difference in the numbers is the ukiel
stack (catalog planning, provider, caches), not a difference in the SQL.

---

## 1. Table name: `hits` → `hits_cb` (all 43 queries)

The operator session registers every hypertable under its own name, and `hits` is already taken
by plan 30's *different* fixture (a 14-column lowercase per-tenant subset). The official fixture
is a separate hypertable, `hits_cb`, so the suites can coexist and plan 30's recorded baselines
stay valid (append-only bench discipline). The raw runner registers its view under the same name
`hits_cb`, so the SQL text is identical on both sides.

Mechanical, zero semantic effect.

## 2. `EventDate` date literals → day numbers (q37, q38, q39, q40, q41, q42, q43)

    -  ... AND "EventDate" >= '2013-07-01' AND "EventDate" <= '2013-07-31' ...
    +  ... AND "EventDate" >= 15887        AND "EventDate" <= 15917        ...

| literal | day number |
|---|---|
| `'2013-07-01'` | 15887 |
| `'2013-07-31'` | 15917 |
| `'2013-07-14'` | 15900 |
| `'2013-07-15'` | 15901 |

**Why.** Ukiel's type system is `int64 / float64 / utf8 / bool` (`timestamp_ms` is physically an
int64 epoch-ms) — there is **no DATE type**. In the source parquet `EventDate` is a `UInt16` day
number (days since 1970-01-01), so ukiel stores it as that same day number widened to `int64`,
and the comparison must be against the day number.

This is the *only* adaptation that touches query text, and it is exactly the class upstream also
has to handle: ClickBench's own `datafusion/create.sql` wraps the table in a view that does
`CAST(CAST("EventDate" AS INTEGER) AS DATE)` for precisely the same reason — the raw parquet has
no date type either. We adapt the literal instead of the column; the row sets are identical.

The predicates keep their shape (a bounded range over an int64 column), which is what matters
for what they exercise: `EventDate` range filters still drive ukiel's catalog-level part pruning
via plan-12 int64 column stats. A `utf8` `'YYYY-MM-DD'` representation would have kept the
literals verbatim (ISO-8601 sorts chronologically) but would have surrendered that pruning path
and made the column slower — a worse benchmark of what ukiel actually is.

**q7** (`SELECT MIN("EventDate"), MAX("EventDate")`) runs verbatim, but returns day numbers
rather than dates. Same data, different rendering.

---

## Type-level deltas (no query text changes; they affect what the numbers *mean*)

These do not change the SQL, but they are real differences between ukiel's fixture and the raw
parquet, and they are part of any `ukiel / raw` ratio. Stating them so the ratio is not
over-read as pure "stack overhead":

**T1. Integers widen to `int64`.** The source has `Int16` (`AdvEngineID`, `ResolutionWidth`,
`IsRefresh`, …), `Int32` (`CounterID`, `RegionID`, `ClientIP`) and `UInt16` (`EventDate`). Ukiel
has no narrower integer type, so all of them are stored as `int64`. Ukiel therefore reads more
bytes per row than the raw engine does for the same column. **Part of any ratio > 1 is this type
widening, not stack cost.** (All source integers are signed or unsigned-narrow, so every cast is
lossless — no value is clipped or nulled.)

**T2. Strings: `Binary` → `utf8`.** The source stores strings as Parquet `Binary`. Ukiel casts
them to `utf8` at load. Upstream does the same thing at read time
(`OPTIONS ('binary_as_string' 'true')` in its `create.sql`) — the raw runner sets the identical
option, so both sides see strings. No delta in practice; recorded for completeness.

**T3. `EventTime` stays unix *seconds*, typed `int64` (not `timestamp_ms`).** The queries call
`to_timestamp_seconds("EventTime")` (q19, q43), so the column must remain seconds. Declaring it
`timestamp_ms` would have been a lie about the unit, so it is declared `int64` — which costs the
writer's delta-packed-timestamp encoding hint on the sort column. A small, real, self-inflicted
cost, taken to keep the column's meaning honest.

**T4. 25 columns, not 105.** `hits_cb` stores exactly the 25 columns the 43 queries reference.
This only affects **q24** (`SELECT *`), which would otherwise read 105 columns on the raw side
and 25 on ukiel's. The raw runner therefore registers a **view projecting the same 25 columns**,
so `SELECT *` means the same thing to both. Note this makes the raw side *faster* on q24 than a
naive 105-column `SELECT *` would be — i.e. it is the conservative choice for ukiel's ratio.

---

## Methodology

ClickBench's own: per query, **1 cold run** (fresh session, fresh parquet-metadata cache) then
**3 hot runs**; the reported number is the hot **minimum**, with the median and the raw runs kept
in the JSON report.

**"Cold" is honestly cold only at the ukiel layer.** A fresh session drops the parquet footer /
metadata cache, so footers are re-read. It does *not* drop MinIO's or the OS's page cache, so the
object bytes may still be in memory. Our cold numbers are therefore optimistic relative to a
true cold-storage read, in the same way ClickBench's are on a repeat run.

## The ClickHouse dialect (plan 34)

`queries-clickhouse.sql` is **this repo's** `queries.sql` (already `hits_cb`-named, already using
day-number `EventDate` literals) translated to ClickHouse's SQL dialect. `diff queries.sql
queries-clickhouse.sql` is the complete change set. It runs against both ClickHouse fixtures
(`--table cb`, `--table parquet`), so those two rungs differ only in storage format.

Separately, `queries-clickhouse-official.sql` and `create-clickhouse-official.sql` are upstream's
**verbatim** ClickHouse files, run against the verbatim 105-column MergeTree. Those are the
calibration fixture and carry **zero** adaptations — that is the entire point of them.

| # | change | queries |
|---|---|---|
| CH1 | `to_timestamp_seconds(x)` → `toDateTime(x)` | q19, q43 |
| CH2 | `extract(minute FROM …)` → `toMinute(…)` | q19 |
| CH3 | `DATE_TRUNC('minute', …)` → `toStartOfMinute(…)`; the `ORDER BY DATE_TRUNC('minute', M)` collapses to `ORDER BY M` (M is already the truncated value) | q43 |
| CH4 | `REGEXP_REPLACE(…)` → `replaceRegexpOne(…)` | q29 |
| CH5 | `AVG(length(x))` → `AVG(lengthUTF8(x))` | q28, q29 |

**CH5 deserves its own paragraph, because it costs ClickHouse time and we did it deliberately.**
DataFusion's `length()` counts *characters*; ClickHouse's counts *bytes*. Upstream's own two query
files disagree here — ClickBench's DataFusion variant and its ClickHouse variant compute genuinely
different numbers for q28/q29, and nobody seems to mind. We do mind: every rung of our comparison
must return the *same answer*, or the timings are not comparing anything. So the ClickHouse dialect
uses `lengthUTF8` to match DataFusion's semantics, and pays for it — **q28 costs +613 ms versus the
byte-length version** (61.8 ms → 674.8 ms). We took the slower, correct option and recorded the
price rather than banking a free 613 ms on a different question.

Types on the `cb` fixture mirror ukiel's exactly (`Int64` / `String`, per T1 above), so ClickHouse
is not handed a narrower-and-cheaper version of the data than ukiel reads. That is what makes it
apples-to-apples; the `official` fixture keeps upstream's narrow types, which is why it is the
calibration table and not the comparison one.

### The raw reference must use ukiel's Parquet reader settings

`raw_session()` sets `pushdown_filters` and `reorder_filters` to `true`, because **ukiel's
provider does** (`ParquetSource::with_pushdown_filters(true)`), and DataFusion leaves both
**off by default**.

This is not a detail. The first version of this reference ran the stock defaults, and the
resulting comparison was *wrong in ukiel's favour*: on q24 (`SELECT * … WHERE "URL" LIKE
'%google%' …`) the reference took **2958 ms** where the correctly-configured one takes **199 ms**
— a 14.8× handicap — which made ukiel appear **10× faster than bare DataFusion**. It was not.
Ukiel had simply enabled a flag the opponent had not, and every query with a `WHERE` clause was
quietly tilted the same way.

The ratio is supposed to isolate **ukiel's stack** — catalog, provider, pruning, cache — not
ukiel's choice of a DataFusion setting that the reference is equally free to set. Any future
change to the provider's reader configuration must be mirrored here, or the number stops meaning
what it claims to mean.
