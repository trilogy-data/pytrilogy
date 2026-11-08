/-!
# String matching: one pattern language for `like`, `ilike` and `contains`

A `like` pattern is read into tokens: `%` matches any run of characters, `_`
any one, and a backslash makes the next character literal (a trailing one is
itself literal). `like` keeps case, `ilike` folds it, and `contains(s, t)` is
a case-folded substring test. `trilogy/core/like_patterns.py` is this model
in Python, and every dialect renders it.

Laws:
- `escaped_literal_exact`: a string with its specials escaped matches exactly
  itself. The renderer writes every literal pattern this way, so it means the
  same under any LIKE.
- `exact_pattern_is_eq`: a pattern with no wildcard is its string's equality.
  The parser plans it as one, which a partition's `complete where` reads.
- `glob_sound`: SQLite's GLOB over the translated pattern matches what the
  pattern does, case-sensitively.
- `contains_is_folded_infix`, `ch_strip_sound` (bounded): the `contains`
  rendering and ClickHouse's rewrite of a computed pattern.
- `ch_literal_roundtrip`: a ClickHouse literal with its backslashes doubled
  reads back as the string.

Counterexamples, one per bug fixed in 0.3.470:
- `sqlite_like_folds_case`: SQLite's LIKE folds ASCII case.
- `unescaped_wildcard_matches_more`: DuckDB and SQLite LIKE had no escape, so
  `a\%c` matched `a\xc`; ClickHouse escaped, so engines split.
- `clickhouse_contains_keeps_case`: `position` is case-sensitive.
- `clickhouse_keeps_ordinary_escape`: ClickHouse keeps the backslash in `\?`.
- `backslash_literal_halved`: an undoubled backslash in a ClickHouse literal is
  read as an escape, so a persisted string lost it.
-/

namespace Trilogy.StringMatch

abbrev Str := List Char

inductive Tok where
  | lit (c : Char)
  | one
  | any
  deriving DecidableEq, Repr

def bs : Char := '\\'

/-- Read a pattern: `\c` is the literal `c`, `%` and `_` are wildcards. -/
def tokenize : Str → List Tok
  | [] => []
  | c :: d :: s => if c = bs then .lit d :: tokenize s else
      (if c = '%' then .any else if c = '_' then .one else .lit c) :: tokenize (d :: s)
  | [c] => [if c = '%' then .any else if c = '_' then .one else .lit c]

/-- Matching with fuel, so `decide` can run it; every call shortens the
tokens or the string, so `|tokens| + |string| + 1` is enough. -/
def mtF : Nat → List Tok → Str → Bool
  | 0, _, _ => false
  | _ + 1, [], s => s.isEmpty
  | _ + 1, .lit _ :: _, [] => false
  | n + 1, .lit c :: ts, d :: s => c == d && mtF n ts s
  | _ + 1, .one :: _, [] => false
  | n + 1, .one :: ts, _ :: s => mtF n ts s
  | n + 1, .any :: ts, [] => mtF n ts []
  | n + 1, .any :: ts, d :: s => mtF n ts (d :: s) || mtF n (.any :: ts) s

def mt (ts : List Tok) (s : Str) : Bool := mtF (ts.length + s.length + 1) ts s

def like (v p : Str) : Bool := mt (tokenize p) v

/-- ASCII case folding, which every engine in CI shares. -/
def fold (c : Char) : Char :=
  if 'A' ≤ c ∧ c ≤ 'Z' then Char.ofNat (c.toNat + 32) else c

def ilike (v p : Str) : Bool := like (v.map fold) (p.map fold)

/-- A NULL operand gives NULL. -/
def like3 : Option Str → Option Str → Option Bool
  | some v, some p => some (like v p)
  | _, _ => none

theorem like_strict (p : Option Str) : like3 none p = none ∧ like3 p none = none := by
  cases p <;> simp [like3]

/-! ## Literal patterns -/

def special (c : Char) : Bool := c = '%' || c = '_' || c = bs

def escapeLit : Str → Str
  | [] => []
  | c :: s => if special c then bs :: c :: escapeLit s else c :: escapeLit s

theorem tokenize_escapeLit (s : Str) : tokenize (escapeLit s) = s.map .lit := by
  induction s with
  | nil => rfl
  | cons c s ih =>
    unfold escapeLit
    by_cases h : special c = true
    · simp only [h, ↓reduceIte, tokenize, bs, List.map_cons, ih]
    · simp only [h, Bool.false_eq_true, ↓reduceIte, List.map_cons]
      have h1 : c ≠ '%' := by intro e; simp [special, e] at h
      have h2 : c ≠ '_' := by intro e; simp [special, e] at h
      have h3 : c ≠ '\\' := by intro e; simp [special, bs, e] at h
      cases hs : escapeLit s with
      | nil =>
        rw [hs] at ih
        simp only [tokenize, h1, h2, ↓reduceIte, ← ih]
      | cons d r =>
        rw [hs] at ih
        simp only [tokenize, bs, h1, h2, h3, ↓reduceIte, ← ih]

theorem mtF_lits (s v : Str) (n : Nat) (hn : s.length + 1 ≤ n) :
    mtF n (s.map .lit) v = (v == s) := by
  induction s generalizing v n with
  | nil =>
    obtain ⟨k, rfl⟩ : ∃ k, n = k + 1 := ⟨n - 1, by simp at hn; omega⟩
    cases v <;> simp [mtF]
  | cons c s ih =>
    obtain ⟨k, rfl⟩ : ∃ k, n = k + 1 := ⟨n - 1, by simp at hn; omega⟩
    cases v with
    | nil => simp [mtF]
    | cons d v =>
      simp only [List.map_cons, mtF, ih v k (by simp at hn; omega), List.cons_beq_cons]
      by_cases h : c = d
      · subst h; rfl
      · rw [beq_false_of_ne h, beq_false_of_ne (Ne.symm h)]

theorem mt_lits (s v : Str) : mt (s.map .lit) v = (v == s) :=
  mtF_lits s v _ (by simp)

theorem escaped_literal_exact (v s : Str) : like v (escapeLit s) = (v == s) := by
  simp only [like, tokenize_escapeLit, mt_lits]

/-- The string a pattern without wildcards spells. -/
def litsOf : List Tok → Option Str
  | [] => some []
  | .lit c :: ts => (litsOf ts).map (c :: ·)
  | _ :: _ => none

theorem litsOf_map (ts : List Tok) (s : Str) (h : litsOf ts = some s) :
    ts = s.map .lit := by
  induction ts generalizing s with
  | nil => simp [litsOf] at h; simp [← h]
  | cons t ts ih =>
    cases t with
    | lit c =>
      simp only [litsOf, Option.map_eq_some_iff] at h
      obtain ⟨r, hr, rfl⟩ := h
      simp [ih r hr]
    | one => simp [litsOf] at h
    | any => simp [litsOf] at h

theorem exact_pattern_is_eq (v p s : Str) (h : litsOf (tokenize p) = some s) :
    like v p = (v == s) := by
  simp only [like, litsOf_map _ _ h, mt_lits]

/-! ## SQLite: GLOB keeps case -/

/-- GLOB: `*`, `?`, and `[c]` for a literal `c`; there is no escape. -/
def globTokenize : Str → List Tok
  | [] => []
  | '[' :: c :: ']' :: s => .lit c :: globTokenize s
  | c :: s => (if c = '*' then .any else if c = '?' then .one else .lit c) :: globTokenize s

theorem globTokenize_plain (c : Char) (s : Str) (h : c ≠ '[') :
    globTokenize (c :: s) =
      (if c = '*' then .any else if c = '?' then .one else .lit c) :: globTokenize s := by
  rw [globTokenize.eq_def]
  split
  · contradiction
  · rename_i heq; simp at heq; exact absurd heq.1 h
  · rename_i heq; simp only [List.cons.injEq] at heq; obtain ⟨rfl, rfl⟩ := heq; rfl

def toGlob : List Tok → Str
  | [] => []
  | .any :: ts => '*' :: toGlob ts
  | .one :: ts => '?' :: toGlob ts
  | .lit c :: ts =>
      if c = '*' ∨ c = '?' ∨ c = '[' then '[' :: c :: ']' :: toGlob ts
      else c :: toGlob ts

theorem glob_tokenize_toGlob (ts : List Tok) : globTokenize (toGlob ts) = ts := by
  induction ts with
  | nil => simp [toGlob, globTokenize]
  | cons t ts ih =>
    cases t with
    | any => simp [toGlob, globTokenize_plain, ih]
    | one => simp [toGlob, globTokenize_plain, ih]
    | lit c =>
      by_cases h : c = '*' ∨ c = '?' ∨ c = '['
      · simp only [toGlob, h, ↓reduceIte]
        rw [globTokenize.eq_def]; simp [ih]
      · simp only [not_or] at h
        obtain ⟨h1, h2, h3⟩ := h
        simp [toGlob, h1, h2, h3, globTokenize_plain, ih]

theorem glob_sound (v p : Str) : mt (globTokenize (toGlob (tokenize p))) v = like v p := by
  rw [glob_tokenize_toGlob]; rfl

/-- SQLite's LIKE: the same tokens, matched after folding ASCII case. -/
def sqliteLike (v p : Str) : Bool := ilike v p

theorem sqlite_like_folds_case : sqliteLike "Abc".toList "a%".toList = true ∧
    like "Abc".toList "a%".toList = false := by decide

/-! ## Escapes the engines read differently -/

/-- DuckDB and SQLite LIKE with no ESCAPE clause: a backslash is a character. -/
def tokenizeNoEscape : Str → List Tok :=
  List.map fun c => if c = '%' then .any else if c = '_' then .one else .lit c

theorem unescaped_wildcard_matches_more :
    mt (tokenizeNoEscape "a\\%c".toList) "a\\xc".toList = true ∧
    like "a\\xc".toList "a\\%c".toList = false := by decide

/-- ClickHouse: a backslash escapes only `%`, `_` and itself. -/
def chTokenize : Str → List Tok
  | [] => []
  | c :: d :: s => if c = bs ∧ special d then .lit d :: chTokenize s else
      (if c = '%' then .any else if c = '_' then .one else .lit c) :: chTokenize (d :: s)
  | [c] => [if c = '%' then .any else if c = '_' then .one else .lit c]

theorem clickhouse_keeps_ordinary_escape :
    mt (chTokenize "\\?".toList) "\\?".toList = true ∧
    like "\\?".toList "\\?".toList = false := by decide

/-- The rendered rewrite of a computed pattern: a backslash before an ordinary
character is dropped (`replaceRegexpAll(p, '\\([^%_\\])', '\1')`). -/
def chStrip : Str → Str
  | c :: d :: s => if c = bs ∧ !special d then d :: chStrip s else c :: chStrip (d :: s)
  | s => s

def strs : Nat → List Str
  | 0 => [[]]
  | n + 1 => [] :: (strs n).flatMap fun s => ['\\', 'a', '%', '?'].map (· :: s)

/-- No lone trailing backslash: engines refuse such a pattern. -/
def wellEnded (p : Str) : Bool :=
  (p.reverse.takeWhile (· = bs)).length % 2 = 0

set_option maxRecDepth 100000 in
theorem ch_strip_sound : ∀ p ∈ strs 4, ∀ v ∈ strs 3,
    wellEnded p → mt (chTokenize (chStrip p)) v = like v p := by decide

/-! ## `contains` -/

def isInfix (t s : Str) : Bool :=
  (List.range (s.length + 1)).any fun i => (s.drop i).take t.length == t

def containsFolded (s t : Str) : Bool := isInfix (t.map fold) (s.map fold)

/-- The SQL Server rendering: `lower(s) LIKE '%' + escaped(lower(t)) + '%'`. -/
theorem contains_is_folded_infix : ∀ s ∈ strs 3, ∀ t ∈ strs 2,
    ilike s ('%' :: escapeLit t ++ ['%']) = containsFolded s t := by decide

def position (s t : Str) : Bool := isInfix t s

theorem clickhouse_contains_keeps_case :
    position "aBc".toList "b".toList = false ∧
    containsFolded "aBc".toList "b".toList = true := by decide

/-! ## A ClickHouse string literal -/

/-- How ClickHouse reads a literal's body: `\c` is an escape. -/
def chRead : Str → Str
  | c :: d :: s => if c = bs then (if d = 'n' then '\n' else d) :: chRead s else
      c :: chRead (d :: s)
  | s => s

def chRender : Str → Str
  | [] => []
  | c :: s => if c = bs then bs :: bs :: chRender s else c :: chRender s

theorem ch_literal_roundtrip (s : Str) : chRead (chRender s) = s := by
  induction s with
  | nil => rfl
  | cons c s ih =>
    by_cases h : c = bs
    · subst h; simp [chRender, chRead, ih, bs]
    · simp only [chRender, h, ↓reduceIte]
      cases hs : chRender s with
      | nil => rw [hs] at ih; simp [chRead, ← ih]
      | cons d r =>
        rw [hs] at ih
        simp only [chRead, h, ↓reduceIte, ← hs]
        rw [hs, ih]

theorem backslash_literal_halved :
    chRead "a\\n".toList ≠ "a\\n".toList := by decide

end Trilogy.StringMatch
