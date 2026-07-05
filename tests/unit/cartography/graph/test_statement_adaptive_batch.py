from unittest.mock import MagicMock

import neo4j.exceptions
import pytest

from cartography.graph.statement import GraphStatement
from cartography.graph.statement import MIN_ITERATIONSIZE_ON_MEMORY_ERROR

MEMORY_OOM_CODE = "Neo.TransientError.General.MemoryPoolOutOfMemoryError"


def _memory_oom_error() -> neo4j.exceptions.TransientError:
    return neo4j.exceptions.Neo4jError._hydrate_neo4j(
        code=MEMORY_OOM_CODE,
        message="transaction memory limit reached",
    )


def _summary(contains_updates: bool) -> MagicMock:
    summary = MagicMock()
    summary.counters.contains_updates = contains_updates
    return summary


def test_iterative_statement_halves_batch_on_memory_error():
    # Arrange: the first two write attempts blow the transaction memory limit,
    # then one iteration succeeds with updates, then a final iteration reports no updates.
    stmt = GraphStatement(
        "MATCH (n) WITH n LIMIT $LIMIT_SIZE DETACH DELETE n",
        iterative=True,
        iterationsize=1000,
    )
    session = MagicMock()
    session.write_transaction.side_effect = [
        _memory_oom_error(),
        _memory_oom_error(),
        _summary(contains_updates=True),
        _summary(contains_updates=False),
    ]

    # Act
    stmt._run_iterative(session)

    # Assert: 1000 -> 500 -> 250
    assert stmt.parameters["LIMIT_SIZE"] == 250
    assert session.write_transaction.call_count == 4


def test_iterative_statement_never_goes_below_floor():
    stmt = GraphStatement(
        "MATCH (n) WITH n LIMIT $LIMIT_SIZE DETACH DELETE n",
        iterative=True,
        iterationsize=200,
    )
    session = MagicMock()
    # 200 -> 100 (floor); the next memory error at the floor is re-raised.
    session.write_transaction.side_effect = [
        _memory_oom_error(),
        _memory_oom_error(),
    ]

    with pytest.raises(neo4j.exceptions.TransientError):
        stmt._run_iterative(session)

    assert stmt.parameters["LIMIT_SIZE"] == MIN_ITERATIONSIZE_ON_MEMORY_ERROR


def test_iterative_statement_with_small_iterationsize_raises_immediately():
    # An explicitly small iterationsize is already at/below the floor: do not shrink it further.
    stmt = GraphStatement(
        "MATCH (n) WITH n LIMIT $LIMIT_SIZE DETACH DELETE n",
        iterative=True,
        iterationsize=50,
    )
    session = MagicMock()
    session.write_transaction.side_effect = [_memory_oom_error()]

    with pytest.raises(neo4j.exceptions.TransientError):
        stmt._run_iterative(session)

    assert stmt.parameters["LIMIT_SIZE"] == 50


def test_iterative_statement_reraises_other_transient_errors():
    stmt = GraphStatement(
        "MATCH (n) WITH n LIMIT $LIMIT_SIZE DETACH DELETE n",
        iterative=True,
        iterationsize=1000,
    )
    session = MagicMock()
    session.write_transaction.side_effect = [
        neo4j.exceptions.Neo4jError._hydrate_neo4j(
            code="Neo.TransientError.Transaction.DeadlockDetected",
            message="deadlock",
        ),
    ]

    with pytest.raises(neo4j.exceptions.TransientError):
        stmt._run_iterative(session)

    # Batch size untouched by non-memory errors
    assert stmt.parameters["LIMIT_SIZE"] == 1000
