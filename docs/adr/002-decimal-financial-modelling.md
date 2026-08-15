# ADR 002: Exact Decimal Representation for Financial Calculations

## Status
Accepted

## Context
We need to calculate totals, debt ratios, and risk scores without floating-point rounding drift (e.g. `0.1 + 0.2 = 0.30000000000000004`).

## Decision
Use Python's `decimal.Decimal` module across all financial domain models and calculations.

## Consequences

### Advantages
* **Exact Base-10 Arithmetic:** Eliminates binary float rounding errors, satisfying FCA auditing rules.
* **Deterministic Rounding:** Explicit Banker's Rounding (`ROUND_HALF_UP`).

### Disadvantages
* **Serialisation Overhead:** Must be encoded as strings in JSON payloads.