import Trilogy.Readings

/-!
# Instants: what a string cast to a datetime or a timestamp means

A string cast to `datetime` or `timestamp` (or passed to `datetime(s)` /
`timestamp(s)`) reads, between whitespace, a date as `Readings` reads one,
then optionally a time of day `HH:MM:SS` with an optional fraction, then
optionally a zone: `Z`, `+HH`, `+HHMM` or `+HH:MM`
(`trilogy.core.casts.DATETIME_PATTERN`). The value is the UTC instant the
local time names: the local time minus the zone's offset, and a string with no
zone is UTC. Each engine keeps the instant at its own precision: microseconds
on DuckDB, milliseconds on ClickHouse (`DateTime64(3)`) and whole seconds on
SQLite (`datetime()`), each truncating a longer fraction. Every other string
has no reading and casts to NULL.

Laws:
- `roll_keeps_count` (bounded): the day count is linear in the day of the
  month, so a rolled day (`2020-02-30` as March 1st) has the same count as the
  day written; the count cannot tell them apart, and only reading the day back
  can (`Readings.readback_exact`).
- `utc_monotone`, `utc_shift`: under one zone the read keeps the order of
  local times, and a zone is a shift of the UTC reading.
- `read_instant_exact`: a zone west of UTC that pushes a local time past
  midnight reads as the next UTC day.
- `trunc_trunc`, `trunc_monotone`, `trunc_ge_exact`: truncating a fraction to
  an engine's precision and then to a coarser one is truncating to the coarser
  one, it keeps the order, and a comparison with an instant the engine can
  hold is decided the same on the truncated value as on the exact one.

Counterexamples, one per bug fixed in 0.3.474:
- `duckdb_drops_zone`: DuckDB's TIMESTAMP cast ignored a zone
  (`12:34:56+02:00` as 12:34:56), where SQLite and ClickHouse read 10:34:56;
  and DuckDB raised on a string with no reading.
- `hour_24_is_not_a_clock`: `24:00:00` was the next midnight on DuckDB and
  ClickHouse and the text `24:00:00` on SQLite.
- `rolled_instant_is_not_the_instant`: `2020-02-30 00:00:00` was March 1st on
  SQLite.
- `clickhouse_epoch_seconds`: ClickHouse read `'2147483648'` as seconds since
  1970 and `'20200105'` as a day.
- `sqlite_zone_limit`: SQLite's `datetime()` refuses an offset past 14 hours,
  so the rendering applies the offset itself.
- `bound_text_is_not_datetime_text`: a folded instant bound as a parameter on
  SQLite was its isoformat (`+00:00`), which never equals the `datetime()`
  text of a column; it is inlined as `datetime('...')`.
-/

namespace Trilogy.Instants

open Trilogy.Readings (valid roll monthDays Out)

/-! ## The calendar -/

/-- Days from 1970-01-01 to `y-m-d` (proleptic Gregorian, `y ≥ 1`), as
`days_from_civil` computes it; any `d`, including one past the month end. -/
def days (y m d : Nat) : Int :=
  let y' : Int := if m ≤ 2 then (y : Int) - 1 else y
  let era := y' / 400
  let yoe := y' - era * 400
  let mp : Int := ((m : Int) + 9) % 12
  let doy := (153 * mp + 2) / 5 + d - 1
  era * 146097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719468

theorem epoch : days 1970 1 1 = 0 ∧ days 2020 1 5 = 18266 := by decide

set_option maxRecDepth 100000 in
/-- Bounded: three years, every month, days 1 to 31. -/
theorem roll_keeps_count :
    ∀ y : Nat, y < 3 → ∀ m : Nat, m < 12 → ∀ d : Nat, d < 31 →
      days (2019 + y) (m + 1) (d + 1) =
        (match roll (2019 + y) (m + 1) (d + 1) with | (y', m', d') => days y' m' d') := by
  decide

/-! ## Local time, zones and UTC -/

/-- Seconds since 1970-01-01 00:00:00 of a local date and time. -/
def seconds (y m d h mi s : Nat) : Int := days y m d * 86400 + h * 3600 + mi * 60 + s

/-- The UTC instant of a local time at a zone `z` minutes east of UTC. -/
def utc (local_ z : Int) : Int := local_ - 60 * z

theorem utc_monotone (a b z : Int) : a ≤ b ↔ utc a z ≤ utc b z := by
  unfold utc; omega

theorem utc_shift (a z : Int) : utc a z = utc (a - 60 * z) 0 := by
  unfold utc; omega

/-- `2020-01-05 23:34:56-05:30` is `2020-01-06 05:04:56` UTC, and
`2020-01-05 12:34:56+02:00` is `10:34:56`. -/
theorem read_instant_exact :
    utc (seconds 2020 1 5 23 34 56) (-330) = seconds 2020 1 6 5 4 56 ∧
      utc (seconds 2020 1 5 12 34 56) 120 = seconds 2020 1 5 10 34 56 ∧
      utc (seconds 2020 1 5 0 0 0) 840 = seconds 2020 1 4 10 0 0 := by
  decide

/-- The clock the pattern reads: an hour to 23, minutes and seconds to 59. -/
def clockValid (h mi s : Nat) : Bool := h ≤ 23 && mi ≤ 59 && s ≤ 59

/-- A zone the pattern reads: an hour to 23 and minutes to 59, either sign. -/
def zoneValid (z : Int) : Bool := z.natAbs / 60 ≤ 23

/-- DuckDB's TIMESTAMP cast keeps the local time whatever the zone. -/
def duckdbTimestamp (local_ _z : Int) : Int := local_

theorem duckdb_drops_zone :
    duckdbTimestamp (seconds 2020 1 5 12 34 56) 120 ≠ utc (seconds 2020 1 5 12 34 56) 120 := by
  decide

/-- DuckDB and ClickHouse read `24:00:00` as the next midnight. -/
theorem hour_24_is_not_a_clock :
    clockValid 24 0 0 = false ∧ seconds 2020 1 5 24 0 0 = seconds 2020 1 6 0 0 0 := by
  decide

/-- SQLite's `datetime()` rolls an overflowing day; its count is the rolled
day's, so the instant is a real instant of another day. -/
theorem rolled_instant_is_not_the_instant :
    valid 2020 2 30 = false ∧ seconds 2020 2 30 0 0 0 = seconds 2020 3 1 0 0 0 := by
  decide

/-- ClickHouse's best-effort parse reads a bare number as seconds since 1970;
the reference has no reading for a string with no day. -/
def clickhouseBestEffort (n : Nat) : Out := .val n

theorem clickhouse_epoch_seconds :
    clickhouseBestEffort 2147483648 = .val (seconds 2038 1 19 3 14 8) := by decide

/-- SQLite's `datetime()` reads a zone only to 14 hours. -/
def sqliteZone (z : Int) : Bool := z.natAbs ≤ 14 * 60

theorem sqlite_zone_limit : zoneValid (23 * 60) = true ∧ sqliteZone (23 * 60) = false := by
  decide

/-- A bound instant's text on SQLite is its isoformat; a column's is
`datetime()`'s, and SQLite compares them as text. -/
theorem bound_text_is_not_datetime_text :
    ("2020-01-05 12:34:56+00:00" == "2020-01-05 12:34:56") = false := by decide

/-! ## Precision -/

/-- A fraction of `k` digits truncated by `u` (`10^(k - p)` for `p` digits
kept), scaled back. -/
def trunc (f u : Nat) : Nat := f / u * u

theorem trunc_trunc (f a b : Nat) : f / a / b = f / (a * b) := Nat.div_div_eq_div_mul f a b

theorem trunc_monotone (f g u : Nat) (h : f ≤ g) : trunc f u ≤ trunc g u :=
  Nat.mul_le_mul_right u (Nat.div_le_div_right h)

/-- Microseconds truncated to milliseconds, then to seconds, are seconds. -/
theorem trunc_chain : (999999 / 1000) / 1000 = 999999 / 1000000 ∧ trunc 123456 1000 = 123000 := by
  decide

/-- A comparison with a value the engine holds (a multiple of its unit) is
decided the same on the truncated fraction. -/
theorem trunc_ge_exact (f c u : Nat) (hc : c % u = 0) : c ≤ trunc f u ↔ c ≤ f := by
  unfold trunc
  constructor
  · intro h; exact Nat.le_trans h (Nat.div_mul_le_self f u)
  · intro h
    have hc' : c = c / u * u := by
      have := Nat.div_add_mod c u; rw [hc, Nat.add_zero] at this
      rw [Nat.mul_comm] at this; exact this.symm
    rw [hc']; exact Nat.mul_le_mul_right u (Nat.div_le_div_right h)

end Trilogy.Instants
