# ADR 003: Rule-Based Regular Expression Categorisation Engine

## Status
Accepted

## Context
Bank statement descriptions are noisy (e.g. `POS 4829 14OCT26 BET365 UK`). We must normalise and categorise transactions reliably.

## Decision
Use compiled regular expressions (Regex) combined with string sanitisation rules rather than external ML or LLM APIs.

## Consequences

### Advantages
* **100% Deterministic & Auditable:** Every category assignment is traceable to an exact rule.
* **Sub-millisecond Speed:** Executes in $<5\text{ms}$ with zero memory overhead.
* **Zero Recurring Costs:** Runs offline without third-party API dependencies.

### Disadvantages
* **Rule Maintenance:** New merchant patterns must be added to the regex dictionary.