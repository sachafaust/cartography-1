from unittest.mock import MagicMock
from unittest.mock import patch

from cartography.client.core.tx import clear_ensure_indexes_cache
from cartography.client.core.tx import ensure_indexes
from cartography.client.core.tx import load
from cartography.models.core.nodes import CartographyNodeSchema
from tests.data.graph.querybuilder.sample_models.simple_node import (
    SimpleNodeWithSubResourceSchema,
)


def test_ensure_indexes_runs_ddl_once_per_schema():
    # Arrange
    clear_ensure_indexes_cache()
    mock_session = MagicMock()
    with patch(
        "cartography.client.core.tx._run_index_query_with_retry"
    ) as mock_run_index:
        # Act: first call issues the CREATE INDEX statements
        ensure_indexes(mock_session, SimpleNodeWithSubResourceSchema())
        first_call_count = mock_run_index.call_count

        # Act: subsequent calls for the same schema class are no-ops
        ensure_indexes(mock_session, SimpleNodeWithSubResourceSchema())
        ensure_indexes(mock_session, SimpleNodeWithSubResourceSchema())

    # Assert
    assert first_call_count > 0
    assert mock_run_index.call_count == first_call_count

    # Cleanup so other tests see a fresh cache
    clear_ensure_indexes_cache()


def test_ensure_indexes_cache_can_be_cleared():
    # Arrange
    clear_ensure_indexes_cache()
    mock_session = MagicMock()
    with patch(
        "cartography.client.core.tx._run_index_query_with_retry"
    ) as mock_run_index:
        ensure_indexes(mock_session, SimpleNodeWithSubResourceSchema())
        first_call_count = mock_run_index.call_count

        # Act: clearing the cache makes the next call re-issue the DDL
        clear_ensure_indexes_cache()
        ensure_indexes(mock_session, SimpleNodeWithSubResourceSchema())

    # Assert
    assert mock_run_index.call_count == 2 * first_call_count

    clear_ensure_indexes_cache()


def test_load_empty_dict_list():
    # Setup
    mock_session = MagicMock()
    mock_schema = MagicMock(spec=CartographyNodeSchema)
    empty_dict_list = []

    # Execute
    load(mock_session, mock_schema, empty_dict_list)

    # Assert
    mock_session.run.assert_not_called()  # Ensure no database calls were made
    # Verify that ensure_indexes was not called since we short-circuit on empty list
    mock_session.write_transaction.assert_not_called()
