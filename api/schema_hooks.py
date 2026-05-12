def keep_v1_only(endpoints):
    """drf-spectacular preprocessing hook: drop the unversioned /api/ aliases
    and the /api/v1/internal/ tree.

    `api.urls` is included twice in the project urlconf (`/api/v1/` canonical
    + `/api/` back-compat alias). Without filtering, every endpoint shows up
    in the schema twice. We keep only the `/api/v1/` paths so the OpenAPI doc
    advertises the canonical surface.

    The `/api/v1/internal/` subtree is auth-gated and used only by peer
    hosts (backfill puller). It does not belong in the public schema.
    """
    return [
        (path, path_regex, method, callback)
        for path, path_regex, method, callback in endpoints
        if path.startswith("/api/v1/") and not path.startswith("/api/v1/internal/")
    ]
