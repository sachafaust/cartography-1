import json
import logging
import string
from pathlib import Path
from string import Template
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Union

import neo4j

from cartography.graph.cleanupbuilder import build_cleanup_queries
from cartography.graph.cleanupbuilder import build_cleanup_query_for_matchlink
from cartography.graph.statement import get_job_shortname
from cartography.graph.statement import GraphStatement
from cartography.models.core.nodes import CartographyNodeSchema
from cartography.models.core.relationships import CartographyRelSchema

logger = logging.getLogger(__name__)

# Default number of items processed per transaction by iterative cleanup statements. Larger batches
# mean fewer transactions (faster, especially over high-latency connections); smaller batches bound
# transaction memory when deleting nodes that have very many relationships.
DEFAULT_CLEANUP_BATCH_SIZE = 1000

# Operator override, set from config (--cleanup-batch-size) via set_cleanup_batch_size().
_configured_cleanup_batch_size: Optional[int] = None


def set_cleanup_batch_size(batch_size: Optional[int]) -> None:
    """
    Set a process-wide override for the cleanup batch size (iterationsize) used by cleanup jobs.
    Called during sync startup when the operator passes --cleanup-batch-size; also applied to the
    packaged JSON cleanup jobs via cartography.util.run_cleanup_job.
    :param batch_size: The batch size to use, or None to fall back to DEFAULT_CLEANUP_BATCH_SIZE.
    """
    global _configured_cleanup_batch_size
    if batch_size is not None and batch_size <= 0:
        raise ValueError(
            f"cleanup batch size must be a positive integer, got {batch_size}"
        )
    if batch_size is not None and batch_size != _configured_cleanup_batch_size:
        logger.info(
            "Cleanup batch size set to %d (default is %d). Iterative cleanup jobs will delete "
            "up to this many items per transaction.",
            batch_size,
            DEFAULT_CLEANUP_BATCH_SIZE,
        )
    _configured_cleanup_batch_size = batch_size


def get_cleanup_batch_size() -> int:
    """
    The effective cleanup batch size: the operator-configured value if set, else the default.
    """
    return (
        _configured_cleanup_batch_size
        if _configured_cleanup_batch_size is not None
        else DEFAULT_CLEANUP_BATCH_SIZE
    )


def get_configured_cleanup_batch_size() -> Optional[int]:
    """
    The operator-configured cleanup batch size, or None if the operator did not set one.
    Used to decide whether to override the iterationsize baked into packaged JSON cleanup jobs.
    """
    return _configured_cleanup_batch_size


def _get_identifiers(template: string.Template) -> List[str]:
    """
    :param template: A string Template
    :return: the variable names that start with a '$' like $this in the given Template.
    Stolen from https://github.com/python/cpython/issues/90465#issuecomment-1093941790.
    TODO we can get rid of this and use template.get_identifiers() once we are on python 3.11
    """
    return list(
        set(
            filter(
                lambda v: v is not None,
                (
                    mo.group("named") or mo.group("braced")
                    for mo in template.pattern.finditer(template.template)
                ),
            ),
        ),
    )


def get_parameters(queries: List[str]) -> Set[str]:
    """
    :param queries: A list of Neo4j queries with parameters indicated by leading '$' like $this.
    :return: The set of all parameters across all given Neo4j queries.
    """
    parameter_set = set()
    for query in queries:
        as_template = Template(query)
        params = _get_identifiers(as_template)
        parameter_set.update(params)
    return parameter_set


class GraphJobJSONEncoder(json.JSONEncoder):
    """
    Support JSON serialization for GraphJob instances.
    """

    def default(self, obj: Any) -> Any:
        if isinstance(obj, GraphJob):
            return obj.as_dict()
        else:
            # Let the default encoder roll up the exception.
            return json.JSONEncoder.default(self, obj)


class GraphJob:
    """
    A job that will run against the cartography graph. A job is a sequence of statements which execute sequentially.
    """

    def __init__(
        self,
        name: str,
        statements: List[GraphStatement],
        short_name: Optional[str] = None,
    ):
        # E.g. "Okta intel module cleanup"
        self.name = name
        self.statements: List[GraphStatement] = statements
        # E.g. "okta_import_cleanup"
        self.short_name = short_name

    def merge_parameters(self, parameters: Dict) -> None:
        """
        Merge parameters for all job statements.
        """
        for s in self.statements:
            s.merge_parameters(parameters)

    def set_iterationsize(self, iterationsize: int) -> None:
        """
        Override the iterationsize (and its LIMIT_SIZE query parameter) of every iterative
        statement in this job. Used to apply the operator-configured cleanup batch size to
        jobs whose statements were loaded from packaged JSON files with a baked-in value.
        """
        for s in self.statements:
            if s.iterative:
                s.iterationsize = iterationsize
                s.parameters["LIMIT_SIZE"] = iterationsize

    def run(self, neo4j_session: neo4j.Session) -> None:
        """
        Run the job. This will execute all statements sequentially.
        """
        logger.debug("Starting job '%s'.", self.name)
        for stm in self.statements:
            try:
                stm.run(neo4j_session)
            except Exception as e:
                logger.error(
                    "Unhandled error while executing statement in job '%s': %s",
                    self.name,
                    e,
                )
                raise
        log_msg = (
            f"Finished job {self.short_name}"
            if self.short_name
            else f"Finished job {self.name}"
        )
        logger.info(log_msg)

    def as_dict(self) -> Dict:
        """
        Convert job to a dictionary.
        """
        return {
            "name": self.name,
            "statements": [s.as_dict() for s in self.statements],
            "short_name": self.short_name,
        }

    @classmethod
    def from_json(
        cls, blob: Union[str, dict], short_name: Optional[str] = None
    ) -> "GraphJob":
        """
        Create a job from a JSON dict or blob.
        """
        data = json.loads(blob) if isinstance(blob, str) else blob
        statements = _get_statements_from_json(data, short_name)
        name = data["name"]
        return cls(name, statements, short_name)

    @classmethod
    def from_node_schema(
        cls,
        node_schema: CartographyNodeSchema,
        parameters: Dict[str, Any],
        iterationsize: Optional[int] = None,
    ) -> "GraphJob":
        """
        Create a cleanup job from a CartographyNodeSchema object.
        For a given node, the fields used in the node_schema.sub_resource_relationship.target_node_node_matcher.keys()
        must be provided as keys and values in the params dict.
        :param iterationsize: The number of items to process in each iteration. Defaults to the configured cleanup
        batch size (--cleanup-batch-size, falling back to DEFAULT_CLEANUP_BATCH_SIZE = 1000), which measures ~2x
        faster than 100 while staying well within default transaction memory limits. Lower this for node types that
        are unusually dense (many relationships per node) if DETACH DELETE transactions run into memory limits.
        """
        if iterationsize is None:
            iterationsize = get_cleanup_batch_size()
        queries: List[str] = build_cleanup_queries(node_schema)

        expected_param_keys: Set[str] = get_parameters(queries)
        actual_param_keys: Set[str] = set(parameters.keys())
        # Hacky, but LIMIT_SIZE is specified by default in cartography.graph.statement, so we exclude it from validation
        actual_param_keys.add("LIMIT_SIZE")

        missing_params: Set[str] = expected_param_keys - actual_param_keys

        if missing_params:
            raise ValueError(
                f'GraphJob is missing the following expected query parameters: "{missing_params}". Please check the '
                f"value passed to `parameters`.",
            )

        statements: List[GraphStatement] = [
            GraphStatement(
                query,
                parameters=parameters,
                iterative=True,
                iterationsize=iterationsize,
                parent_job_name=node_schema.label,
                parent_job_sequence_num=idx,
            )
            for idx, query in enumerate(queries, start=1)
        ]

        return cls(
            f"Cleanup {node_schema.label}",
            statements,
            node_schema.label,
        )

    @classmethod
    def from_matchlink(
        cls,
        rel_schema: CartographyRelSchema,
        sub_resource_label: str,
        sub_resource_id: str,
        update_tag: int,
        iterationsize: Optional[int] = None,
    ) -> "GraphJob":
        """
        Create a cleanup job from a CartographyRelSchema object (specifically, a MatchLink).
        This is used for cleaning up stale links between nodes created by load_rels(). Do not use for other purposes.

        Other notes:
        - For a given rel_schema, the fields used in the rel_schema.properties._sub_resource_label.name and
        rel_schema.properties._sub_resource_id.name must be provided as keys and values in the params dict.
        - The rel_schema must have a source_node_matcher and target_node_matcher.
        :param iterationsize: The number of items to process in each iteration. Defaults to the configured cleanup
        batch size (--cleanup-batch-size, falling back to DEFAULT_CLEANUP_BATCH_SIZE = 1000).
        """
        if iterationsize is None:
            iterationsize = get_cleanup_batch_size()
        cleanup_link_query = build_cleanup_query_for_matchlink(rel_schema)
        logger.debug(f"Cleanup query: {cleanup_link_query}")

        parameters = {
            "UPDATE_TAG": update_tag,
            "_sub_resource_label": sub_resource_label,
            "_sub_resource_id": sub_resource_id,
        }

        statement = GraphStatement(
            cleanup_link_query,
            parameters=parameters,
            iterative=True,
            iterationsize=iterationsize,
            parent_job_name=rel_schema.rel_label,
        )

        return cls(
            f"Cleanup {rel_schema.rel_label} between {rel_schema.source_node_label} and {rel_schema.target_node_label}",
            [statement],
            rel_schema.rel_label,
        )

    @classmethod
    def from_json_file(cls, file_path: Union[str, Path]) -> "GraphJob":
        """
        Create a job from a JSON file.
        """
        with open(file_path) as j_file:
            data: Dict = json.load(j_file)

        job_shortname: str = get_job_shortname(file_path)
        statements: List[GraphStatement] = _get_statements_from_json(
            data,
            job_shortname,
        )
        name: str = data["name"]
        return cls(name, statements, job_shortname)

    @classmethod
    def run_from_json(
        cls,
        neo4j_session: neo4j.Session,
        blob: Union[str, dict],
        parameters: Dict,
        short_name: Optional[str] = None,
    ) -> None:
        """
        Run a job from a JSON dict or blob. This will deserialize the job and execute all statements sequentially.
        """
        if not parameters:
            parameters = {}

        job: GraphJob = cls.from_json(blob, short_name)
        job.merge_parameters(parameters)
        job.run(neo4j_session)

    @classmethod
    def run_from_json_file(
        cls,
        file_path: Union[str, Path],
        neo4j_session: neo4j.Session,
        parameters: Dict,
    ) -> None:
        """
        Run a job from a JSON file. This will deserialize the job and execute all statements sequentially.
        """
        if not parameters:
            parameters = {}

        job: GraphJob = cls.from_json_file(file_path)

        job.merge_parameters(parameters)
        job.run(neo4j_session)


def _get_statements_from_json(
    blob: Dict,
    short_job_name: Optional[str] = None,
) -> List[GraphStatement]:
    """
    Deserialize all statements from the JSON blob.
    """
    statements: List[GraphStatement] = []
    for i, statement_data in enumerate(blob["statements"]):
        # i+1 to make it 1-based and not 0-based to help with log readability
        statement: GraphStatement = GraphStatement.create_from_json(
            statement_data,
            short_job_name,
            i + 1,
        )
        statements.append(statement)

    return statements
