"""Carrying a folder's share ID down to the files inside it.

The bug this fixes
------------------
A folder shared with you by another Apple ID opens fine and then fails on
everything inside: listing a subfolder returns HTTP 400, downloading a file
returns 404. The login is fine. The request is what Apple rejects.

One missing value causes both. Requests about someone else's share have to name
that share, and every layer reads the name off whatever node it is holding::

    pyicloud DriveNode.get_children()   self.data.get("shareID")
    ifetch  _try_shared_open()          data.get("shareID")

Apple only puts ``shareID`` on the folder that was shared, and leaves it off the
items it returns for that folder's contents. pyicloud builds each child straight
from those items, adding nothing. So the top folder has the ID, everything below
it does not, and from the second level down every request goes out without
saying which share it is about.

The rule
--------
Share context flows down: a node with its own ``shareID`` uses it, a node
without one inherits its parent's.

Inheriting assumes a share covers a whole subtree, which is how Apple's sharing
works. A share nested inside another share is handled by the first half of the
rule — the child's own ID wins, so it overrides rather than being hidden.

:class:`ShareContext` records **where the ID came from**, because an inherited
one is an inference and an owned one is a fact, and a failed download is read
very differently depending on which was in use.

Why this writes into ``node.data``
----------------------------------
:func:`apply` puts the inherited ID into the node's own ``data`` dict. pyicloud
reads that same key before fetching a folder's contents, so writing it once
also fixes pyicloud's subfolder listing, with no patching or subclassing.

An owned ``shareID`` is never overwritten, and a node with no writable ``data``
is reported as untouched rather than assumed done — :func:`apply` returns what
it did so callers can count the refusals.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

#: The node carried the identifier itself. A fact.
SOURCE_OWN = "own"

#: The identifier came from an ancestor. A well-founded inference - see the
#: module docstring for the assumption it rests on.
SOURCE_INHERITED = "inherited"

#: Apple's key, spelled with a capital ``ID``. pyicloud reads this exact key.
SHARE_ID_KEY = "shareID"

#: Where :func:`apply` records that it seeded a value, so a later reader can
#: still tell an inherited identifier from an owned one after the write. Chosen
#: to be unmistakably ours; Apple will never send this key.
PROVENANCE_KEY = "_ifetch_share_source"

#: Companion to :data:`PROVENANCE_KEY`, holding how far below the share root the
#: value travelled. Written for the same reason: once the identifier has been
#: seeded into the payload it is indistinguishable from an owned one, and the
#: distance it came is exactly what a diagnostic needs to quote.
DEPTH_KEY = "_ifetch_share_depth"

_ZONE_KEY = "zone"


def _data_of(node: Any) -> Optional[Dict[str, Any]]:
    """Return the node's mutable payload, or ``None`` if it has none.

    Accepts both pyicloud ``DriveNode`` objects (payload on ``.data``) and the
    bare dicts used by tests and by recorded-response fixtures.
    """
    if isinstance(node, dict):
        return node
    data = getattr(node, "data", None)
    if isinstance(data, dict):
        return data
    return None


def read_share_id(node: Any) -> Optional[Any]:
    """Return the node's own share identifier, or ``None``.

    The value is returned **unchanged**, whatever type it is. pyicloud annotates
    this parameter as a dict while Apple's payloads carry a string, and nothing
    here needs to resolve that disagreement: the identifier is only ever passed
    back to Apple verbatim. Guessing at its shape would be the only way to break
    it.

    Empty values are treated as absent, because an empty ``shareID`` sent to
    Apple produces the same unscoped request as sending none at all - and would
    do so while looking, to every later reader, like a share had been found.
    """
    data = _data_of(node)
    if data is None:
        return None
    value = data.get(SHARE_ID_KEY)
    if value is None or value == "" or value == {}:
        return None
    return value


def read_zone(node: Any) -> Optional[str]:
    """Return the node's zone, or ``None`` if it declares none."""
    data = _data_of(node)
    if data is None:
        return None
    zone = data.get(_ZONE_KEY)
    return zone if isinstance(zone, str) and zone else None


@dataclass(frozen=True)
class ShareContext:
    """Which share a node belongs to, and how confident that answer is.

    ``source`` is the field that matters when something fails. :data:`SOURCE_OWN`
    means Apple named this share on this node. :data:`SOURCE_INHERITED` means it
    was carried down from an ancestor ``depth`` levels above, which is sound
    under the assumption in the module docstring and is still worth saying out
    loud in a diagnostic.
    """

    share_id: Any
    zone: Optional[str] = None
    source: str = SOURCE_OWN
    depth: int = 0

    @property
    def inherited(self) -> bool:
        return self.source == SOURCE_INHERITED

    def describe(self) -> str:
        """A phrase for diagnostics that states the provenance, not just the id."""
        if self.source == SOURCE_OWN:
            return "shareID carried by the item itself"
        levels = "level" if self.depth == 1 else "levels"
        return f"shareID inherited from an ancestor {self.depth} {levels} above"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "share_id": self.share_id,
            "zone": self.zone,
            "source": self.source,
            "depth": self.depth,
        }


def context_of(node: Any) -> Optional[ShareContext]:
    """Build a context from what the node itself declares, or ``None``.

    This is the share-root case: no ancestor has been consulted, so anything
    found here is owned.
    """
    share_id = read_share_id(node)
    if share_id is None:
        return None
    return ShareContext(
        share_id=share_id,
        zone=read_zone(node),
        source=SOURCE_OWN,
        depth=0,
    )


def descend(parent: Optional[ShareContext], child: Any) -> Optional[ShareContext]:
    """Return the context that applies to ``child``.

    The child's own identifier always wins, so a share nested inside another
    share overrides rather than being masked by its parent's. Failing that the
    parent's context is carried down one level. With neither, there is no share
    context and the caller is looking at ordinary owned content.
    """
    own = read_share_id(child)
    if own is not None:
        return ShareContext(
            share_id=own,
            zone=read_zone(child) or (parent.zone if parent else None),
            source=SOURCE_OWN,
            depth=0,
        )
    if parent is None:
        return None
    return ShareContext(
        share_id=parent.share_id,
        zone=read_zone(child) or parent.zone,
        source=SOURCE_INHERITED,
        depth=parent.depth + 1,
    )


def apply(context: Optional[ShareContext], node: Any) -> bool:
    """Seed ``context`` into the node's payload. Return whether it was written.

    Returns ``False`` - meaning *nothing was changed* - when there is no context
    to apply, when the node exposes no writable mapping, or when the node
    already carries its own identifier. That last case is not a failure; it is
    the nested-share rule holding. Callers that need to distinguish "did not
    need it" from "could not do it" should consult :func:`read_share_id` and
    :func:`_data_of` rather than reading a bare ``False`` as success.
    """
    if context is None:
        return False
    data = _data_of(node)
    if data is None:
        return False
    if read_share_id(node) is not None:
        return False
    data[SHARE_ID_KEY] = context.share_id
    data[PROVENANCE_KEY] = context.source
    data[DEPTH_KEY] = context.depth
    if context.zone and not data.get(_ZONE_KEY):
        data[_ZONE_KEY] = context.zone
    return True


def carried_context(node: Any) -> Optional[ShareContext]:
    """Rebuild the context a node is carrying, provenance and all.

    :func:`context_of` answers "what does this node declare?" and always calls
    the answer owned, which is right for a share root and wrong for everything
    below one: after :func:`apply` has seeded an inherited identifier, the
    payload no longer distinguishes it from a value Apple sent. The markers
    written alongside it do, and this is the reader for them.

    A node carrying an identifier with no markers is treated as owning it -
    that is the share root, and the only way to be carrying a value nothing
    seeded.
    """
    share_id = read_share_id(node)
    if share_id is None:
        return None
    data = _data_of(node) or {}
    source = data.get(PROVENANCE_KEY)
    if source not in (SOURCE_OWN, SOURCE_INHERITED):
        source = SOURCE_OWN
    depth = data.get(DEPTH_KEY)
    if not isinstance(depth, int) or depth < 0:
        depth = 0
    return ShareContext(
        share_id=share_id,
        zone=read_zone(node),
        source=source,
        depth=depth,
    )


def inherit(parent: Any, child: Any) -> Optional[ShareContext]:
    """Carry ``parent``'s share context onto ``child``. The traversal hook.

    This is the whole fix expressed as one call. Traversal resolves a child and
    hands both nodes here; the child comes back knowing its share and, where its
    payload allows, carrying it - which is what lets pyicloud scope the request
    when that child is later asked for children of its own.

    Taking the parent *node* rather than a context is deliberate. It means the
    caller does not have to thread state through a recursive walk, and it works
    at any entry point, including resuming a traversal partway down a tree.
    """
    return adopt(carried_context(parent), child)


def adopt(parent_context: Optional[ShareContext], child: Any) -> Optional[ShareContext]:
    """Compute the child's context and seed it into the child in one step.

    This is the call sites' shape: traversal holds the parent's context, gets a
    child back from pyicloud, and needs the child both to *know* its share and
    to *carry* it, because pyicloud will read the carried value when that child
    is asked for its own children.
    """
    context = descend(parent_context, child)
    apply(context, child)
    return context


def provenance(node: Any) -> Optional[str]:
    """Return :data:`SOURCE_OWN`, :data:`SOURCE_INHERITED`, or ``None``.

    Answers "where did this node's shareID come from?" after :func:`apply` has
    written one, which is the question a failed request needs answered.
    """
    data = _data_of(node)
    if data is None or read_share_id(node) is None:
        return None
    recorded = data.get(PROVENANCE_KEY)
    if recorded in (SOURCE_OWN, SOURCE_INHERITED):
        return recorded
    return SOURCE_OWN
