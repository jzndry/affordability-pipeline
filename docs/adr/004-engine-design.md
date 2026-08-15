# ADR 004: Deterministic Policy Matrix for Credit Decisioning

## Status
Accepted

## Context
FCA CONC 5.2A regulations require transparent, explainable credit affordability assessments and vulnerability checks.

## Decision
Use a deterministic Policy Rule Matrix evaluating explicit thresholds rather than a black-box machine learning model.

## Consequences

### Advantages
* **Full Explainability:** Rejections/referrals output explicit statutory risk flags (e.g. `EXCESSIVE_GAMBLING_RISK`).
* **Deterministic Execution:** The same statement always yields the exact same decision.
* **Auditable:** Risk managers can inspect and adjust thresholds directly.

### Disadvantages
* **Binary Cut-offs:** Rigid boundary thresholds require layered referral bands for nuance.