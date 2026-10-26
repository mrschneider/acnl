# Contributing to ACNL

Thanks for contributing. The rules below are not style preferences — they are
the contract, and CI + review enforce them.

## The non-negotiables

1. **The vocabulary stays closed.** Seven wire kinds, seven operations. A new
   verb or wire kind is a **major version bump** and needs a documented
   version-policy entry in `docs/requirements/README.md` (R8).
2. **Every refusal is typed.** User (never retryable), system (carries
   `retryable`), message (bytes unparsable, connection stays open). A new
   error path without a taxonomy entry will be refused in review.
3. **Validation at one entrance.** `acnl_core` validates; handlers trust the
   message. Validation spread across handlers is validation you will forget.
4. **Implemented-but-unverified is not done.** Every claim in a PR carries the
   real command output: test counts, E2E check counts.
5. **Portability is proven, not claimed.** Wire-level changes must pass the
   cross-language E2E (`python experiments/run_e2e.py`).

## Workflow

```bash
python -m pytest tests/ -q          # unit + contract
python experiments/run_e2e.py       # cross-language interop gate
python -m pip wheel --no-deps -w dist/ .   # packaging sanity
```

- Branch from `main`, open a PR, CI must be green.
- One logical change per PR.
- The core (`acnl_core/`) stays IO-free: no sockets, no files, no clocks
  except injected time. Transports only move bytes.

## Docs

Behavior changes update `docs/architecture/` and `docs/requirements/` in the
same PR. The README's gap list must stay truthful: every unverified item is
named as such, in the words you would use to a colleague.
