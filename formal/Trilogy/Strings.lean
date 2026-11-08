import Trilogy.StringMatch

/-!
# String functions: positions, lengths and case count characters

A string is a list of characters. `len` counts them, `substring` and `strpos`
index them from 1, and `upper`/`lower` map each one. `trim` strips spaces,
`replace` and `regexp_replace` replace every occurrence, and `regexp_extract`
returns the group it is asked for. DuckDB and SQLite are the reference;
`trilogy/dialect/common.py` (`render_window_substring`) and the dialect maps
render it everywhere else.

`substring(s, start, n)` reads positions the way DuckDB and SQLite do: a
negative start counts from the end (`-1` is the last character), `0` is the
position before the first, and a negative `n` takes the characters before
`start`. The taken positions are a window clipped to `[1, len s]`.

Laws:
- `clamp_exact` (bounded): every engine's substring agrees once
  `start ≥ 1` and `n ≥ 0`; the renderer clamps any other window into that
  form and keeps the reference's characters.
- `strpos_extract`: the substring at `strpos(s, t)` of `len t` characters is
  `t`, whenever `t` is found. `strpos_empty`: an empty needle is found at 1.
- `marker_len`: SQL Server's `LEN` drops trailing spaces; `LEN(s + 'x') - 1`
  is the length.
- `trim_left_idem`: stripping leading spaces twice is stripping once.
- `percent_matches_all`: `like '%'` holds for every string, so it is
  `is not null` (`trilogy/parsing/common.py`, `like_or_equality`).
- `sibling_rejects_null`: `x = c and x is not null` keeps the rows `x = c`
  keeps, so the NOT NULL strip drops the second atom.

Counterexamples, one per bug fixed in 0.3.471:
- `clickhouse_zero_start`, `clickhouse_negative_count`: ClickHouse reads
  start 0 as empty and a negative count as "all but the last".
- `standard_negative_start`: Postgres and SQL Server read a negative start as
  a window before the string, not from its end.
- `byte_length_counts_bytes`, `byte_position_counts_bytes`: ClickHouse's
  `length`/`position`/`substring` and MySQL's `LENGTH` count UTF-8 bytes.
- `replace_first_only`: DuckDB's and Postgres's `regexp_replace` replace the
  first match unless asked for all.
- `clickhouse_extract_ignores_group`: `extract` returns the first group
  whatever group was asked for.
- `bigquery_trim_strips_tabs`: a bare `TRIM` strips all whitespace there.
- `charindex_misses_empty`: SQL Server's `CHARINDEX` finds no empty needle.
-/

namespace Trilogy.Strings

abbrev Str := List Char

/-! ## Substring positions -/

/-- The window `[lo, hi)` of 1-based positions DuckDB and SQLite take. -/
def window (n : Nat) (start count : Int) : Int × Int :=
  let b := if start < 0 then n + 1 + start else start
  if count < 0 then (b + count, b) else (b, b + count)

/-- The reference: the positions of the window inside `[1, n]`. -/
def refPositions (n : Nat) (start count : Int) : List Nat :=
  let w := window n start count
  (List.range' 1 n).filter fun p => w.1 ≤ (p : Int) ∧ (p : Int) < w.2

/-- What every engine takes for `start ≥ 1`, `count ≥ 0`. -/
def nativePositions (n lo count : Nat) : List Nat :=
  (List.range' lo count).filter fun p => p ≤ n

/-- `render_window_substring`'s clamped arguments. -/
def clamped (n : Nat) (start count : Int) : Nat × Nat :=
  let w := window n start count
  let first := if w.1 < 1 then 1 else w.1
  let taken := if w.2 - first < 0 then 0 else w.2 - first
  (first.toNat, taken.toNat)

def substr (s : Str) (start count : Int) : Str :=
  (refPositions s.length start count).filterMap fun p => s[p - 1]?

def native (s : Str) (lo count : Nat) : Str := (s.drop (lo - 1)).take count

/-- Bounded: strings up to 6 characters, start and count in `[-8, 8]`. -/
theorem clamp_exact :
    ∀ n : Nat, n < 7 → ∀ a : Nat, a < 17 → ∀ c : Nat, c < 17 →
      1 ≤ (clamped n ((a : Int) - 8) ((c : Int) - 8)).1 ∧
      refPositions n ((a : Int) - 8) ((c : Int) - 8) =
        nativePositions n (clamped n ((a : Int) - 8) ((c : Int) - 8)).1
          (clamped n ((a : Int) - 8) ((c : Int) - 8)).2 := by
  decide

theorem take_drop_positions (s : Str) (m count : Nat) :
    (List.range' (m + 1) count).filterMap (fun p => s[p - 1]?) = (s.drop m).take count := by
  induction count generalizing m with
  | zero => simp
  | succ k ih =>
    rw [List.range'_succ, List.filterMap_cons, show m + 1 - 1 = m by omega, ih (m + 1)]
    by_cases hm : m < s.length
    · rw [List.getElem?_eq_getElem hm]
      simp only [List.drop_eq_getElem_cons hm, List.take_succ_cons]
    · have h1 : s.length ≤ m := by omega
      simp [List.getElem?_eq_none h1, List.drop_eq_nil_of_le h1,
        List.drop_eq_nil_of_le (show s.length ≤ m + 1 by omega)]

theorem filterMap_drop_out (s : Str) :
    ∀ l : List Nat, (∀ p ∈ l, 1 ≤ p) →
      (l.filter fun p => decide (p ≤ s.length)).filterMap (fun p => s[p - 1]?) =
        l.filterMap (fun p => s[p - 1]?) := by
  intro l
  induction l with
  | nil => simp
  | cons p l ih =>
    intro h
    have hl := ih (fun q hq => h q (by simp [hq]))
    by_cases hp : p ≤ s.length
    · simp only [List.filter_cons, hp, decide_true, ↓reduceIte, List.filterMap_cons, hl]
    · have : s.length ≤ p - 1 := by have := h p (by simp); omega
      simp only [List.filter_cons, hp, decide_false, Bool.false_eq_true, ↓reduceIte,
        List.filterMap_cons, List.getElem?_eq_none this, hl]

/-- The characters of `nativePositions` are the engines' `substring`. -/
theorem native_positions (s : Str) (lo count : Nat) (h : 1 ≤ lo) :
    native s lo count = (nativePositions s.length lo count).filterMap fun p => s[p - 1]? := by
  obtain ⟨m, rfl⟩ : ∃ m, lo = m + 1 := ⟨lo - 1, by omega⟩
  rw [nativePositions, filterMap_drop_out s _ (fun p hp => by simp [List.mem_range'] at hp; omega),
    take_drop_positions]
  simp [native]

/-! ## strpos -/

/-- The 1-based position of the first occurrence of `t`, `0` if none. -/
def strposFrom (t : Str) : Str → Nat → Nat
  | [], i => if t.isEmpty then i else 0
  | c :: s, i => if t.isPrefixOf (c :: s) then i else strposFrom t s (i + 1)

def strpos (s t : Str) : Nat := strposFrom t s 1

theorem strpos_empty (s : Str) : strpos s [] = 1 := by
  cases s <;> simp [strpos, strposFrom, List.isPrefixOf]

theorem strposFrom_found (t : Str) :
    ∀ (s : Str) (i : Nat), strposFrom t s i ≠ 0 → 1 ≤ i →
      i ≤ strposFrom t s i ∧ t <+: s.drop (strposFrom t s i - i) := by
  intro s
  induction s with
  | nil =>
    intro i h hi
    by_cases ht : t.isEmpty = true
    · simp only [strposFrom, ht, ↓reduceIte] at h ⊢
      simp at ht; subst ht; simp
    · simp [strposFrom, ht] at h
  | cons c s ih =>
    intro i h hi
    by_cases hp : t.isPrefixOf (c :: s) = true
    · simp only [strposFrom, hp, ↓reduceIte] at h ⊢
      exact ⟨Nat.le_refl _, by simpa using List.isPrefixOf_iff_prefix.mp hp⟩
    · simp only [strposFrom, hp, Bool.false_eq_true, ↓reduceIte] at h ⊢
      obtain ⟨hle, hpre⟩ := ih (i + 1) h (by omega)
      refine ⟨by omega, ?_⟩
      have : strposFrom t s (i + 1) - i = (strposFrom t s (i + 1) - (i + 1)) + 1 := by omega
      rw [this]; simpa using hpre

/-- The substring at a found position, `len t` long, is `t`. -/
theorem strpos_extract (s t : Str) (h : strpos s t ≠ 0) :
    native s (strpos s t) t.length = t := by
  obtain ⟨_, hpre⟩ := strposFrom_found t s 1 h (Nat.le_refl _)
  simp only [native, strpos] at hpre ⊢
  exact (List.prefix_iff_eq_take.mp hpre).symm

/-! ## Length -/

/-- UTF-8 bytes in a character. -/
def utf8Bytes (c : Char) : Nat :=
  if c.toNat < 0x80 then 1 else if c.toNat < 0x800 then 2
  else if c.toNat < 0x10000 then 3 else 4

def byteLength (s : Str) : Nat := (s.map utf8Bytes).sum

/-- SQL Server's `LEN`: the length without trailing spaces. -/
def lenTrimmed (s : Str) : Nat := (s.reverse.dropWhile (· == ' ')).length

theorem marker_len (s : Str) : lenTrimmed (s ++ ['x']) - 1 = s.length := by
  simp [lenTrimmed, List.reverse_append]

/-! ## Trim -/

def trimLeft (s : Str) : Str := s.dropWhile (· == ' ')
def trimRight (s : Str) : Str := (trimLeft s.reverse).reverse
def trim (s : Str) : Str := trimRight (trimLeft s)

theorem dropWhile_idem (p : Char → Bool) (s : Str) :
    (s.dropWhile p).dropWhile p = s.dropWhile p := by
  induction s with
  | nil => rfl
  | cons c s ih => by_cases h : p c = true <;> simp [List.dropWhile, h, ih]

theorem trim_left_idem (s : Str) : trimLeft (trimLeft s) = trimLeft s :=
  dropWhile_idem _ s

/-! ## Planning -/

theorem percent_matches_all (v : Str) : StringMatch.like v ['%'] = true := by
  simp only [StringMatch.like, StringMatch.tokenize, ↓reduceIte, StringMatch.mt]
  suffices h : ∀ n, v.length + 2 ≤ n → StringMatch.mtF n [.any] v = true by
    exact h _ (by simp; omega)
  induction v with
  | nil =>
    intro n hn
    obtain ⟨k, rfl⟩ : ∃ k, n = k + 2 := ⟨n - 2, by simp at hn; omega⟩
    simp [StringMatch.mtF]
  | cons d s ih =>
    intro n hn
    obtain ⟨k, rfl⟩ : ∃ k, n = k + 1 := ⟨n - 1, by omega⟩
    simp only [StringMatch.mtF, Bool.or_eq_true]
    exact Or.inr (ih k (by simp at hn; omega))

/-- Three-valued `x = c and x is not null` against `x = c`, per row. -/
theorem sibling_rejects_null (x : Option Int) (c : Int) :
    ((x.map (· == c)) == some true && x.isSome) = ((x.map (· == c)) == some true) := by
  cases x <;> simp

/-! ## Counterexamples -/

/-- ClickHouse's `substring` with start 0 is empty. -/
def chPositions (n : Nat) (start count : Int) : List Nat :=
  if start = 0 then [] else
  let b := if start < 0 then n + 1 + start else start
  let e := if count < 0 then (n : Int) + 1 + count else b + count
  (List.range' 1 n).filter fun p => b ≤ (p : Int) ∧ (p : Int) < e

theorem clickhouse_zero_start : chPositions 6 0 3 ≠ refPositions 6 0 3 := by decide

theorem clickhouse_negative_count : chPositions 6 2 (-1) ≠ refPositions 6 2 (-1) := by decide

/-- Postgres and SQL Server: the SQL-standard window `[start, start + n)`. -/
def standardPositions (n : Nat) (start count : Int) : List Nat :=
  (List.range' 1 n).filter fun p => start ≤ (p : Int) ∧ (p : Int) < start + count

theorem standard_negative_start :
    standardPositions 6 (-2) 2 ≠ refPositions 6 (-2) 2 := by decide

theorem byte_length_counts_bytes : byteLength ['h', 'é'] ≠ ['h', 'é'].length := by decide

/-- `strpos` counted in bytes: the bytes before the match, plus one. -/
def bytePosition (s t : Str) : Nat :=
  let i := strpos s t
  if i = 0 then 0 else byteLength (s.take (i - 1)) + 1

theorem byte_position_counts_bytes :
    bytePosition ['h', 'é', 'l'] ['l'] ≠ strpos ['h', 'é', 'l'] ['l'] := by decide

/-- Replacing a single character: every occurrence, or the first. -/
def replaceAll (c : Char) (r : Str) : Str → Str
  | [] => []
  | d :: s => if d = c then r ++ replaceAll c r s else d :: replaceAll c r s

def replaceFirst (c : Char) (r : Str) : Str → Str
  | [] => []
  | d :: s => if d = c then r ++ s else d :: replaceFirst c r s

theorem replace_first_only :
    replaceFirst 'a' ['b'] ['a', 'a'] ≠ replaceAll 'a' ['b'] ['a', 'a'] := by decide

/-- A match's groups, whole match first: `regexp_extract` returns the one
asked for. `extract` returns group 1 when there is one. -/
def extractGroup (groups : List Str) (g : Nat) : Str := groups.getD g []

def chExtract (groups : List Str) (_ : Nat) : Str :=
  if groups.length > 1 then groups.getD 1 [] else groups.getD 0 []

theorem clickhouse_extract_ignores_group :
    chExtract [['a', '1'], ['a'], ['1']] 2 ≠ extractGroup [['a', '1'], ['a'], ['1']] 2 := by
  decide

def trimWhitespace (s : Str) : Str :=
  let w := fun c : Char => c == ' ' || c == '\t' || c == '\n'
  ((s.dropWhile w).reverse.dropWhile w).reverse

theorem bigquery_trim_strips_tabs : trimWhitespace ['\t', 'a'] ≠ trim ['\t', 'a'] := by decide

/-- `CHARINDEX(t, s)`: no empty needle is found. -/
def charindex (s t : Str) : Nat := if t.isEmpty then 0 else strpos s t

theorem charindex_misses_empty : charindex ['a'] [] ≠ strpos ['a'] [] := by decide

end Trilogy.Strings
