"""Algorithmic performance benchmark for the credit underwriting pipeline.

This module measures raw Central Processing Unit (CPU) execution latency and
throughput for transaction categorisation and credit risk evaluation rules
without network or database overhead.
"""

import random
import statistics
import time
from datetime import date
from decimal import Decimal
from typing import Dict, List

from app.core.affordability import AffordabilityEngine
from app.core.categoriser import TransactionCategoriser
from app.core.models import (
    BankStatementPayload,
    Transaction,
    TransactionCategory,
)

SAMPLE_TRANSACTION_DESCRIPTIONS: List[str] = [
    "BGC EMPLOYER SALARY DWP",
    "DIRECT DEBIT HOUSING RENT",
    "POS 4829 BET365 UK",
    "TFL TRAVEL CHARGE TUBE",
    "SAINSBURYS S/MKTS LONDON",
    "NETFLIX.COM PAYMENT",
    "DD CLYDESDALE LOAN REPAYMENT",
    "ATM CASH WITHDRAWAL HIGH ST",
    "TRANSFER TO SAVINGS POT",
    "SPOTIFY PREMIUM GB",
]


def generate_synthetic_transactions(
    transaction_count: int,
) -> List[Transaction]:
    """Generate a pseudo-random list of transaction records for testing.

    Args:
        transaction_count: The total number of transactions to generate.

    Returns:
        A list of populated Transaction instances.
    """
    transaction_list: List[Transaction] = []
    transaction_date: date = date(2026, 8, 1)

    for index in range(transaction_count):
        raw_description: str = random.choice(SAMPLE_TRANSACTION_DESCRIPTIONS)

        # Salary transactions represent positive credit inflows; all others are outflows.
        if "SALARY" in raw_description:
            transaction_amount: Decimal = Decimal(str(random.randint(1500, 4500)))
        else:
            transaction_amount = Decimal(str(-random.randint(5, 500)))

        record = Transaction(
            id=f"tx_benchmark_{index:05d}",
            date=transaction_date,
            raw_description=raw_description,
            amount=transaction_amount,
            category=TransactionCategory.UNCATEGORISED,
        )
        transaction_list.append(record)

    return transaction_list


def create_synthetic_statement(
    transaction_count: int,
) -> BankStatementPayload:
    """Construct a complete BankStatementPayload containing synthetic transactions.

    Args:
        transaction_count: Number of transactions to attach to the statement.

    Returns:
        A fully validated BankStatementPayload instance.
    """
    transactions = generate_synthetic_transactions(transaction_count)

    return BankStatementPayload(
        statement_id="stmt_benchmark_load_test",
        account_holder="Synthetic Underwriting Subject",
        account_number="12345678",
        sort_code="20-00-00",
        transactions=transactions,
    )


def execute_profiling_run(
    base_statement: BankStatementPayload,
) -> Dict[str, float]:
    """Execute a single end-to-end processing run and return elapsed timings.

    Args:
        base_statement: The input bank statement to process.

    Returns:
        A dictionary containing elapsed milliseconds for categorisation,
        risk underwriting evaluation, and total execution time.
    """
    # Create isolated deep copies of the transaction records for this iteration
    transactions_copy = [transaction.model_copy() for transaction in base_statement.transactions]

    start_time_nanoseconds = time.perf_counter_ns()

    # Stage 1: Categorisation and regex sanitisation
    categorised_transactions = TransactionCategoriser.process_statement(transactions_copy)
    categorisation_end_nanoseconds = time.perf_counter_ns()

    # Stage 2: Financial risk and affordability evaluation
    statement_for_evaluation = base_statement.model_copy(
        update={"transactions": categorised_transactions}
    )
    _ = AffordabilityEngine.evaluate(statement_for_evaluation)
    underwriting_end_nanoseconds = time.perf_counter_ns()

    categorisation_time_ms = (categorisation_end_nanoseconds - start_time_nanoseconds) / 1_000_000.0

    underwriting_time_ms = (
        underwriting_end_nanoseconds - categorisation_end_nanoseconds
    ) / 1_000_000.0

    total_time_ms = (underwriting_end_nanoseconds - start_time_nanoseconds) / 1_000_000.0

    return {
        "categorisation_time_ms": categorisation_time_ms,
        "underwriting_time_ms": underwriting_time_ms,
        "total_time_ms": total_time_ms,
    }


def run_benchmark_suite(
    iteration_count: int = 50,
    transactions_per_statement: int = 1000,
) -> None:
    """Run the benchmark suite across multiple iterations and print statistics.

    Args:
        iteration_count: The number of sequential iterations to execute.
        transactions_per_statement: The number of transactions per statement.
    """
    separator_bar = "-" * 72
    print(separator_bar)
    print("Underwriting Engine Performance Benchmark")
    print(
        f"Configuration: {iteration_count} iterations | "
        f"{transactions_per_statement} transactions per statement"
    )
    print(separator_bar)

    base_statement = create_synthetic_statement(transactions_per_statement)

    categorisation_timings: List[float] = []
    underwriting_timings: List[float] = []
    total_timings: List[float] = []

    for _ in range(iteration_count):
        timings = execute_profiling_run(base_statement)
        categorisation_timings.append(timings["categorisation_time_ms"])
        underwriting_timings.append(timings["underwriting_time_ms"])
        total_timings.append(timings["total_time_ms"])

    # Calculate statistical metrics
    average_categorisation_ms = statistics.mean(categorisation_timings)
    average_underwriting_ms = statistics.mean(underwriting_timings)
    average_total_ms = statistics.mean(total_timings)

    min_total_ms = min(total_timings)
    max_total_ms = max(total_timings)
    std_dev_total_ms = statistics.stdev(total_timings)

    total_transactions_processed = iteration_count * transactions_per_statement
    total_elapsed_seconds = sum(total_timings) / 1000.0
    throughput_transactions_per_second = total_transactions_processed / total_elapsed_seconds

    # Display results
    print(f"Categorisation Latency (Mean) : {average_categorisation_ms:8.3f} ms")
    print(f"Underwriting Engine (Mean)   : {average_underwriting_ms:8.3f} ms")
    print(f"Total Latency per Statement   : {average_total_ms:8.3f} ms")
    print(f"Latency Range (Min / Max)     : {min_total_ms:8.3f} ms / {max_total_ms:.3f} ms")
    print(f"Standard Deviation            : {std_dev_total_ms:8.3f} ms")
    print(separator_bar)
    print(
        f"Calculated Throughput         : "
        f"{throughput_transactions_per_second:,.0f} transactions/second"
    )
    print(separator_bar)


if __name__ == "__main__":
    run_benchmark_suite()
