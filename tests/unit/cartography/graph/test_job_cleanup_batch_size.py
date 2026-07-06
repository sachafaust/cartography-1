import pytest

from cartography.graph.job import DEFAULT_CLEANUP_BATCH_SIZE
from cartography.graph.job import get_cleanup_batch_size
from cartography.graph.job import get_configured_cleanup_batch_size
from cartography.graph.job import GraphJob
from cartography.graph.job import set_cleanup_batch_size
from tests.data.graph.querybuilder.sample_models.simple_node import (
    SimpleNodeWithSubResourceSchema,
)


@pytest.fixture(autouse=True)
def reset_cleanup_batch_size():
    yield
    set_cleanup_batch_size(None)


def test_default_cleanup_batch_size():
    assert get_configured_cleanup_batch_size() is None
    assert get_cleanup_batch_size() == DEFAULT_CLEANUP_BATCH_SIZE


def test_set_cleanup_batch_size_overrides_default():
    set_cleanup_batch_size(250)
    assert get_configured_cleanup_batch_size() == 250
    assert get_cleanup_batch_size() == 250

    set_cleanup_batch_size(None)
    assert get_cleanup_batch_size() == DEFAULT_CLEANUP_BATCH_SIZE


def test_set_cleanup_batch_size_rejects_nonpositive():
    with pytest.raises(ValueError):
        set_cleanup_batch_size(0)
    with pytest.raises(ValueError):
        set_cleanup_batch_size(-5)


def test_from_node_schema_uses_configured_batch_size():
    set_cleanup_batch_size(123)
    job = GraphJob.from_node_schema(
        SimpleNodeWithSubResourceSchema(),
        {"sub_resource_id": "sub-resource-id", "UPDATE_TAG": 1},
    )
    for statement in job.statements:
        assert statement.iterationsize == 123
        assert statement.parameters["LIMIT_SIZE"] == 123


def test_from_node_schema_explicit_iterationsize_wins():
    set_cleanup_batch_size(123)
    job = GraphJob.from_node_schema(
        SimpleNodeWithSubResourceSchema(),
        {"sub_resource_id": "sub-resource-id", "UPDATE_TAG": 1},
        iterationsize=7,
    )
    for statement in job.statements:
        assert statement.iterationsize == 7


def test_set_iterationsize_overrides_json_baked_value():
    blob = {
        "name": "test job",
        "statements": [
            {
                "query": "MATCH (n:Foo) WHERE n.lastupdated <> $UPDATE_TAG WITH n LIMIT $LIMIT_SIZE DETACH DELETE n",
                "iterative": True,
                "iterationsize": 1000,
            },
            {
                "query": "MATCH (n:Bar) RETURN count(n)",
                "iterative": False,
                "iterationsize": 0,
            },
        ],
    }
    job = GraphJob.from_json(blob)

    job.set_iterationsize(200)

    iterative, non_iterative = job.statements
    assert iterative.iterationsize == 200
    assert iterative.parameters["LIMIT_SIZE"] == 200
    # Non-iterative statements are left alone
    assert non_iterative.iterationsize == 0
