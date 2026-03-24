## lattice.nim -- Minimal result lattice for mqtt.
##
## Result[T, E]: Good | Bad     -- fallible operations
## Maybe[T, E]:  Good | None | Bad -- nullable + fallible

{.experimental: "strict_funcs".}

# =====================================================================================================================
# Types
# =====================================================================================================================

type
  Result*[T, E] = object
    ## Good | Bad. Fallible operations.
    case ok*: bool
    of true:
      val*: T
    of false:
      err*: E

  MaybeKind* = enum
    mkGood
    mkNone
    mkBad

  Maybe*[T, E] = object
    ## Good | None | Bad. Nullable + fallible.
    case kind*: MaybeKind
    of mkGood:
      val*: T
    of mkNone:
      discard
    of mkBad:
      err*: E

# =====================================================================================================================
# Result constructors
# =====================================================================================================================

func good*[T, E](R: typedesc[Result[T, E]], val: T): Result[T, E] =
  Result[T, E](ok: true, val: val)

func bad*[T, E](R: typedesc[Result[T, E]], err: E): Result[T, E] =
  Result[T, E](ok: false, err: err)

# =====================================================================================================================
# Maybe constructors
# =====================================================================================================================

func good*[T, E](M: typedesc[Maybe[T, E]], val: T): Maybe[T, E] =
  Maybe[T, E](kind: mkGood, val: val)

func none*[T, E](M: typedesc[Maybe[T, E]]): Maybe[T, E] =
  Maybe[T, E](kind: mkNone)

func bad*[T, E](M: typedesc[Maybe[T, E]], err: E): Maybe[T, E] =
  Maybe[T, E](kind: mkBad, err: err)

# =====================================================================================================================
# Predicates
# =====================================================================================================================

func is_good*[T, E](r: Result[T, E]): bool = r.ok
func is_bad*[T, E](r: Result[T, E]): bool = not r.ok

func is_good*[T, E](m: Maybe[T, E]): bool = m.kind == mkGood
func is_none*[T, E](m: Maybe[T, E]): bool = m.kind == mkNone
func is_bad*[T, E](m: Maybe[T, E]): bool = m.kind == mkBad

# =====================================================================================================================
# get_or
# =====================================================================================================================

func get_or*[T, E](r: Result[T, E], default: T): T =
  if r.ok: r.val else: default

func get_or*[T, E](m: Maybe[T, E], default: T): T =
  if m.kind == mkGood: m.val else: default
