from dataclasses import dataclass

from cartography.models.core.common import PropertyRef
from cartography.models.core.nodes import CartographyNodeProperties
from cartography.models.core.nodes import CartographyNodeSchema
from cartography.models.core.relationships import CartographyRelProperties
from cartography.models.core.relationships import CartographyRelSchema
from cartography.models.core.relationships import LinkDirection
from cartography.models.core.relationships import make_target_node_matcher
from cartography.models.core.relationships import OtherRelationships
from cartography.models.core.relationships import TargetNodeMatcher


@dataclass(frozen=True)
class FakeEmp3ToGitHubUserRelProperties(CartographyRelProperties):
    lastupdated: PropertyRef = PropertyRef("lastupdated", set_in_kwargs=True)


@dataclass(frozen=True)
class FakeEmp3ToGitHubUser(CartographyRelSchema):
    target_node_label: str = "GitHubUser"
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {"username": PropertyRef("github_username_prefix", starts_with=True)},
    )
    direction: LinkDirection = LinkDirection.OUTWARD
    rel_label: str = "IDENTITY_GITHUB"
    properties: FakeEmp3ToGitHubUserRelProperties = FakeEmp3ToGitHubUserRelProperties()


@dataclass(frozen=True)
class FakeEmp3NodeProperties(CartographyNodeProperties):
    id: PropertyRef = PropertyRef("id")
    lastupdated: PropertyRef = PropertyRef("lastupdated", set_in_kwargs=True)
    email: PropertyRef = PropertyRef("email")
    github_username_prefix: PropertyRef = PropertyRef("github_username_prefix")


@dataclass(frozen=True)
class FakeEmp3Schema(CartographyNodeSchema):
    label: str = "FakeEmployee3"
    properties: FakeEmp3NodeProperties = FakeEmp3NodeProperties()
    other_relationships: OtherRelationships = OtherRelationships(
        [
            FakeEmp3ToGitHubUser(),
        ],
    )
