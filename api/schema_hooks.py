def keep_v1_only(endpoints):
    """drf-spectacular preprocessing hook: drop the unversioned /api/ aliases.

    `api.urls` is included twice in the project urlconf (`/api/v1/` canonical
    + `/api/` back-compat alias). Without filtering, every endpoint shows up
    in the schema twice. Keep only the `/api/v1/` paths so the OpenAPI doc
    advertises the canonical surface.
    """
    return [
        (path, path_regex, method, callback)
        for path, path_regex, method, callback in endpoints
        if path.startswith("/api/v1/")
    ]
