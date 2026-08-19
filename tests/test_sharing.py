"""Tests for share-context inheritance (:mod:`ifetch.sharing`).

The scenario these are written against is iFetch issue #15 and rclone #9477:
a folder shared by another Apple ID lists fine at its root and then fails on
everything underneath, because ``shareID`` lives on the share root and Apple
does not repeat it on the children it returns.

:class:`FakeDriveNode` reproduces that exactly - it builds children from raw
payloads the way pyicloud's ``DriveNode.get_children()`` does, injecting
nothing - so a test that walks it is testing the real failure and not a
convenient stand-in for it.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.sharing import (  # noqa: E402
    PROVENANCE_KEY,
    SHARE_ID_KEY,
    SOURCE_INHERITED,
    SOURCE_OWN,
    ShareContext,
    adopt,
    apply,
    context_of,
    descend,
    provenance,
    read_share_id,
    read_zone,
)


class FakeDriveNode:
    """A pyicloud ``DriveNode`` lookalike, faithful in the way that matters.

    ``children()`` mirrors pyicloud: each child is constructed from the raw item
    payload with nothing added. That is the defect under test; a fake that
    helpfully propagated ``shareID`` would make every test below pass while the
    real bug survived.
    """

    def __init__(self, data, items=None):
        self.data = dict(data)
        self._items = list(items or [])

    def children(self):
        return [FakeDriveNode(item) for item in self._items]


def node(name="f.txt", share_id=None, zone=None, **extra):
    data = {"drivewsid": f"FILE::{name}", "docwsid": f"DOC-{name}", "name": name}
    if share_id is not None:
        data[SHARE_ID_KEY] = share_id
    if zone is not None:
        data["zone"] = zone
    data.update(extra)
    return FakeDriveNode(data)


# ---------------------------------------------------------------------------
# read_share_id / read_zone
# ---------------------------------------------------------------------------


class TestReadShareId:
    def test_reads_the_nodes_own_identifier(self):
        assert read_share_id(node(share_id="SHARE-ABC")) == "SHARE-ABC"

    def test_absent_key_is_none(self):
        assert read_share_id(node()) is None

    def test_accepts_a_bare_dict_as_well_as_a_node(self):
        assert read_share_id({SHARE_ID_KEY: "SHARE-ABC"}) == "SHARE-ABC"

    @pytest.mark.parametrize("empty", ["", {}, None])
    def test_empty_values_count_as_absent(self, empty):
        """An empty shareID produces the same unscoped request as none at all.

        Treating it as present would make a later reader believe a share had
        been found, which is the failure mode this whole module exists to stop.
        """
        assert read_share_id(node(share_id=empty)) is None

    def test_a_dict_shaped_identifier_is_passed_through_unchanged(self):
        """pyicloud annotates shareID as a dict; Apple sends a string.

        Nothing here needs to resolve that - the value is only ever handed back
        to Apple verbatim, so it is returned by identity.
        """
        value = {"token": "abc"}
        assert read_share_id(node(share_id=value)) is value

    def test_object_without_a_data_mapping_is_none_not_an_error(self):
        assert read_share_id(object()) is None

    def test_non_dict_data_attribute_is_none_not_an_error(self):
        broken = FakeDriveNode({})
        broken.data = "not a mapping"
        assert read_share_id(broken) is None


class TestReadZone:
    def test_reads_zone(self):
        assert read_zone(node(zone="com.apple.CloudDocs")) == "com.apple.CloudDocs"

    def test_missing_or_empty_zone_is_none(self):
        assert read_zone(node()) is None
        assert read_zone(node(zone="")) is None

    def test_non_string_zone_is_rejected(self):
        assert read_zone(node(zone=123)) is None


# ---------------------------------------------------------------------------
# context_of
# ---------------------------------------------------------------------------


class TestContextOf:
    def test_share_root_yields_an_owned_context_at_depth_zero(self):
        ctx = context_of(node(share_id="SHARE-ABC", zone="Z"))
        assert ctx == ShareContext("SHARE-ABC", "Z", SOURCE_OWN, 0)
        assert ctx.inherited is False

    def test_unshared_node_has_no_context(self):
        assert context_of(node()) is None

    def test_context_is_immutable(self):
        ctx = context_of(node(share_id="SHARE-ABC"))
        with pytest.raises(Exception):
            ctx.share_id = "SHARE-OTHER"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# descend — the inheritance rule
# ---------------------------------------------------------------------------


class TestDescend:
    def test_child_without_an_identifier_inherits_the_parents(self):
        parent = ShareContext("SHARE-ABC", "Z", SOURCE_OWN, 0)
        ctx = descend(parent, node())
        assert ctx.share_id == "SHARE-ABC"
        assert ctx.source == SOURCE_INHERITED
        assert ctx.depth == 1
        assert ctx.inherited is True

    def test_depth_counts_levels_below_the_share_root(self):
        ctx = ShareContext("SHARE-ABC", None, SOURCE_OWN, 0)
        for expected in (1, 2, 3, 4):
            ctx = descend(ctx, node())
            assert ctx.depth == expected
            assert ctx.source == SOURCE_INHERITED

    def test_a_childs_own_identifier_wins_over_the_inherited_one(self):
        """A share nested inside a share overrides; it is not masked."""
        parent = ShareContext("SHARE-OUTER", None, SOURCE_OWN, 0)
        ctx = descend(parent, node(share_id="SHARE-INNER"))
        assert ctx.share_id == "SHARE-INNER"
        assert ctx.source == SOURCE_OWN
        assert ctx.depth == 0

    def test_owned_content_under_no_share_has_no_context(self):
        assert descend(None, node()) is None

    def test_a_shared_node_reached_without_a_parent_context_is_owned(self):
        ctx = descend(None, node(share_id="SHARE-ABC"))
        assert ctx == ShareContext("SHARE-ABC", None, SOURCE_OWN, 0)

    def test_child_zone_wins_but_parent_zone_fills_a_gap(self):
        parent = ShareContext("SHARE-ABC", "PARENT-ZONE", SOURCE_OWN, 0)
        assert descend(parent, node(zone="CHILD-ZONE")).zone == "CHILD-ZONE"
        assert descend(parent, node()).zone == "PARENT-ZONE"

    def test_nested_share_still_inherits_a_zone_it_lacks(self):
        parent = ShareContext("SHARE-OUTER", "PARENT-ZONE", SOURCE_OWN, 0)
        ctx = descend(parent, node(share_id="SHARE-INNER"))
        assert ctx.share_id == "SHARE-INNER"
        assert ctx.zone == "PARENT-ZONE"

    def test_descend_does_not_mutate_the_child(self):
        before = dict(node().data)
        child = node()
        descend(ShareContext("SHARE-ABC"), child)
        assert child.data == before


# ---------------------------------------------------------------------------
# apply — the write-back that also repairs pyicloud's own traversal
# ---------------------------------------------------------------------------


class TestApply:
    def test_writes_the_identifier_and_its_provenance(self):
        child = node()
        assert apply(ShareContext("SHARE-ABC", None, SOURCE_INHERITED, 1), child) is True
        assert child.data[SHARE_ID_KEY] == "SHARE-ABC"
        assert child.data[PROVENANCE_KEY] == SOURCE_INHERITED

    def test_seeded_key_is_the_exact_key_pyicloud_reads(self):
        """pyicloud does ``self.data.get("shareID")``; the spelling is the fix."""
        child = node()
        apply(ShareContext("SHARE-ABC"), child)
        assert child.data.get("shareID") == "SHARE-ABC"

    def test_never_overwrites_an_identifier_the_node_already_owns(self):
        child = node(share_id="SHARE-INNER")
        assert apply(ShareContext("SHARE-OUTER"), child) is False
        assert child.data[SHARE_ID_KEY] == "SHARE-INNER"

    def test_no_context_writes_nothing(self):
        child = node()
        assert apply(None, child) is False
        assert SHARE_ID_KEY not in child.data

    def test_node_with_no_writable_mapping_is_refused_not_crashed(self):
        assert apply(ShareContext("SHARE-ABC"), object()) is False

    def test_zone_fills_a_gap_but_does_not_replace_one(self):
        gap = node()
        apply(ShareContext("SHARE-ABC", "Z"), gap)
        assert gap.data["zone"] == "Z"

        existing = node(zone="OWN-ZONE")
        apply(ShareContext("SHARE-ABC", "Z"), existing)
        assert existing.data["zone"] == "OWN-ZONE"

    def test_context_without_a_zone_adds_no_zone_key(self):
        child = node()
        apply(ShareContext("SHARE-ABC"), child)
        assert "zone" not in child.data

    def test_applying_twice_is_idempotent(self):
        child = node()
        ctx = ShareContext("SHARE-ABC", None, SOURCE_INHERITED, 1)
        apply(ctx, child)
        first = dict(child.data)
        apply(ctx, child)
        assert child.data == first


# ---------------------------------------------------------------------------
# provenance — the detail a failed request needs
# ---------------------------------------------------------------------------


class TestProvenance:
    def test_inherited_identifier_reports_inherited(self):
        child = node()
        apply(ShareContext("SHARE-ABC", None, SOURCE_INHERITED, 2), child)
        assert provenance(child) == SOURCE_INHERITED

    def test_owned_identifier_reports_own(self):
        assert provenance(node(share_id="SHARE-ABC")) == SOURCE_OWN

    def test_no_identifier_reports_nothing(self):
        assert provenance(node()) is None
        assert provenance(object()) is None

    def test_unrecognised_marker_falls_back_to_own(self):
        child = node(share_id="SHARE-ABC", **{PROVENANCE_KEY: "garbage"})
        assert provenance(child) == SOURCE_OWN

    def test_describe_names_the_evidence_not_just_the_id(self):
        assert "itself" in ShareContext("S", None, SOURCE_OWN, 0).describe()
        assert "inherited" in ShareContext("S", None, SOURCE_INHERITED, 1).describe()

    @pytest.mark.parametrize("depth,word", [(1, "1 level"), (2, "2 levels")])
    def test_describe_agrees_with_itself_on_number(self, depth, word):
        assert word in ShareContext("S", None, SOURCE_INHERITED, depth).describe()

    def test_to_dict_round_trips_the_fields(self):
        assert ShareContext("S", "Z", SOURCE_INHERITED, 3).to_dict() == {
            "share_id": "S",
            "zone": "Z",
            "source": SOURCE_INHERITED,
            "depth": 3,
        }


# ---------------------------------------------------------------------------
# adopt — the regression these tests exist for
# ---------------------------------------------------------------------------


class TestAdoptAcrossARealisticTree:
    """Walk the exact tree shape from issue #15 and rclone #9477."""

    @staticmethod
    def build_share():
        """Share root -> subfolder -> file. Only the root carries the shareID.

        This is what Apple actually returns, and it is why the naive client
        gets HTTP 400 on the subfolder and 404 on the file.
        """
        leaf = {"drivewsid": "FILE::JAB-007", "docwsid": "DOC-JAB", "name": "JAB-007.jxl"}
        sub = {"drivewsid": "FOLDER::Photoshoot", "docwsid": "DOC-SUB", "name": "Photoshoot"}
        root = {
            "drivewsid": "FOLDER::Oeuvres",
            "docwsid": "DOC-ROOT",
            "name": "Oeuvres",
            SHARE_ID_KEY: "SHARE-ABC",
            "zone": "com.apple.CloudDocs",
        }
        return FakeDriveNode(root, items=[sub]), sub, leaf

    def test_without_inheritance_the_descendants_carry_nothing(self):
        """Pins the bug: the fake reproduces pyicloud, so this must hold."""
        root, _, _ = self.build_share()
        (child,) = root.children()
        assert read_share_id(root) == "SHARE-ABC"
        assert read_share_id(child) is None

    def test_adopt_carries_the_share_to_every_descendant(self):
        root, sub_data, leaf_data = self.build_share()

        root_ctx = context_of(root)
        sub = FakeDriveNode(sub_data)
        sub_ctx = adopt(root_ctx, sub)
        leaf = FakeDriveNode(leaf_data)
        leaf_ctx = adopt(sub_ctx, leaf)

        assert sub_ctx.share_id == "SHARE-ABC"
        assert leaf_ctx.share_id == "SHARE-ABC"
        assert sub.data[SHARE_ID_KEY] == "SHARE-ABC"
        assert leaf.data[SHARE_ID_KEY] == "SHARE-ABC"

    def test_the_leaf_knows_the_identifier_was_not_its_own(self):
        root, sub_data, leaf_data = self.build_share()
        sub = FakeDriveNode(sub_data)
        leaf = FakeDriveNode(leaf_data)
        leaf_ctx = adopt(adopt(context_of(root), sub), leaf)

        assert leaf_ctx.source == SOURCE_INHERITED
        assert leaf_ctx.depth == 2
        assert provenance(leaf) == SOURCE_INHERITED
        assert "2 levels" in leaf_ctx.describe()

    def test_zone_reaches_the_leaf_too(self):
        root, sub_data, leaf_data = self.build_share()
        sub = FakeDriveNode(sub_data)
        leaf = FakeDriveNode(leaf_data)
        leaf_ctx = adopt(adopt(context_of(root), sub), leaf)
        assert leaf_ctx.zone == "com.apple.CloudDocs"
        assert leaf.data["zone"] == "com.apple.CloudDocs"

    def test_owned_content_is_untouched_by_any_of_this(self):
        """The non-shared path must gain nothing - no keys, no context."""
        owned_root = FakeDriveNode({"drivewsid": "FOLDER::Documents", "name": "Documents"})
        child = node("report.pdf")
        ctx = adopt(context_of(owned_root), child)
        assert ctx is None
        assert SHARE_ID_KEY not in child.data
        assert PROVENANCE_KEY not in child.data

    def test_a_nested_share_takes_over_from_its_depth_downwards(self):
        outer = ShareContext("SHARE-OUTER", None, SOURCE_OWN, 0)
        inner = FakeDriveNode({"name": "inner", SHARE_ID_KEY: "SHARE-INNER"})
        inner_ctx = adopt(outer, inner)
        leaf = node("deep.txt")
        leaf_ctx = adopt(inner_ctx, leaf)

        assert inner_ctx.share_id == "SHARE-INNER"
        assert inner_ctx.source == SOURCE_OWN
        assert leaf_ctx.share_id == "SHARE-INNER"
        assert leaf_ctx.depth == 1

    def test_unicode_nfd_names_are_irrelevant_to_share_context(self):
        """Apple returns NFD names; share context must not depend on the name."""
        nfd = FakeDriveNode({"name": "Café.pdf", "docwsid": "DOC-NFD"})
        ctx = adopt(ShareContext("SHARE-ABC"), nfd)
        assert ctx.share_id == "SHARE-ABC"
        assert nfd.data[SHARE_ID_KEY] == "SHARE-ABC"

    def test_a_child_that_cannot_be_written_still_reports_its_context(self):
        """The context is computed even where it cannot be seeded.

        Callers get the answer they need for the request itself; only
        pyicloud's own onward traversal loses the benefit, and ``apply``
        returning False is how that is made countable rather than invisible.
        """
        unwritable = object()
        ctx = adopt(ShareContext("SHARE-ABC"), unwritable)
        assert ctx is not None
        assert ctx.share_id == "SHARE-ABC"
        assert ctx.source == SOURCE_INHERITED
