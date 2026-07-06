import pytest

from cartography.models.core.common import PropertyRef


def test_property_ref_starts_with_repr():
    prop = PropertyRef("RoleNamePrefix", starts_with=True)
    assert repr(prop) == "item.RoleNamePrefix"


def test_property_ref_starts_with_rejects_ignore_case():
    with pytest.raises(ValueError):
        PropertyRef("x", starts_with=True, ignore_case=True)


def test_property_ref_starts_with_rejects_fuzzy():
    with pytest.raises(ValueError):
        PropertyRef("x", starts_with=True, fuzzy_and_ignore_case=True)


def test_property_ref_starts_with_rejects_one_to_many():
    with pytest.raises(ValueError):
        PropertyRef("x", starts_with=True, one_to_many=True)
