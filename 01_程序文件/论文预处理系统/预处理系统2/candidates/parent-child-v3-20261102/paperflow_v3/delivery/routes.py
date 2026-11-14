"""Resolve model-ranked specialties against the existing formal catalog."""

from paperflow.package import resolve_routes


def select_formal_routes(profile, asset_rows, catalog):
    # The original model contract already orders candidates by relevance.
    # Keep that order and use the existing catalog's dataset identifiers.
    routes, pending = resolve_routes(profile, asset_rows, catalog)
    if pending or not routes:
        raise ValueError('formal specialty mapping needs review')
    return routes[:3]
