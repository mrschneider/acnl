## What does this PR change?

<!-- One line. What behavior changes for a participant on the wire? -->

## Which requirement does it touch?

<!-- Reference docs/requirements/README.md (R1–R9) and, when relevant, an invariant from docs/architecture/README.md. -->

## How was it verified?

<!-- The rule: implemented-but-unverified is not done. Paste the real output. -->

- [ ] `python -m pytest tests/ -q` — N passed (paste the count)
- [ ] `python experiments/run_e2e.py` — N/N checks passed (only if the wire contract changed)

## Checklist

- [ ] The wire vocabulary stayed closed (no new verb without a major version bump)
- [ ] Every refusal is typed (user / system-with-retryable / message)
- [ ] Validation stayed at one entrance (`acnl_core`), not spread across handlers
- [ ] If the protocol changed: docs/architecture and docs/requirements updated
