"""Utilities for applying environment prefixes to physical asset addresses."""

import posixpath
from pathlib import PurePosixPath


def _is_gcs(location: str) -> bool:
    return location.startswith("gs://")


def _is_url_like(location: str) -> bool:
    """True for any scheme:// address (GCS, S3, Azure, etc.)."""
    return "://" in location


def apply_env_prefix(location: str, env_name: str) -> str:
    """Return the environment-prefixed version of a physical address.

    For SQL-style addresses (schema.table or bare table): prefix the table name.
        schema.table  →  schema.{env}_table
        table         →  {env}_table

    For URL-style addresses (gs://, s3://, etc.): suffix the filename stem.
        gs://bucket/path/file.parquet  →  gs://bucket/path/file_{env}.parquet
        gs://bucket/path/file          →  gs://bucket/path/file_{env}
    """
    if _is_url_like(location):
        # Split off scheme prefix (gs://bucket/...)
        scheme_end = location.index("://") + 3
        scheme = location[:scheme_end]
        rest = location[scheme_end:]
        # Work on the path portion
        parent = posixpath.dirname(rest)
        filename = posixpath.basename(rest)
        if not filename:
            # Ends with slash — nothing sensible to suffix
            return location
        stem, _, ext = filename.rpartition(".")
        if stem:
            new_filename = f"{stem}_{env_name}.{ext}"
        else:
            new_filename = f"{ext}_{env_name}"
        new_rest = posixpath.join(parent, new_filename) if parent else new_filename
        return f"{scheme}{new_rest}"
    else:
        # SQL-style: may have schema qualifier separated by dots
        parts = location.split(".")
        parts[-1] = f"{env_name}_{parts[-1]}"
        return ".".join(parts)


def strip_env_prefix(location: str, env_name: str) -> str:
    """Reverse of apply_env_prefix — strip env prefix/suffix to get base address."""
    if _is_url_like(location):
        scheme_end = location.index("://") + 3
        scheme = location[:scheme_end]
        rest = location[scheme_end:]
        parent = posixpath.dirname(rest)
        filename = posixpath.basename(rest)
        if not filename:
            return location
        stem, _, ext = filename.rpartition(".")
        suffix = f"_{env_name}"
        if stem and stem.endswith(suffix):
            new_filename = f"{stem[:-len(suffix)]}.{ext}"
        elif ext.endswith(suffix):
            new_filename = ext[: -len(suffix)]
        else:
            return location
        new_rest = posixpath.join(parent, new_filename) if parent else new_filename
        return f"{scheme}{new_rest}"
    else:
        parts = location.split(".")
        prefix = f"{env_name}_"
        if parts[-1].startswith(prefix):
            parts[-1] = parts[-1][len(prefix) :]
            return ".".join(parts)
        return location


def env_backup_address(location: str) -> str:
    """Temporary address used during two-phase publish (backup of current prod asset)."""
    if _is_url_like(location):
        scheme_end = location.index("://") + 3
        scheme = location[:scheme_end]
        rest = location[scheme_end:]
        p = PurePosixPath(rest)
        stem = p.stem
        suffix = p.suffix
        new_name = f"{stem}__pub_backup{suffix}"
        new_rest = str(p.parent / new_name) if str(p.parent) != "." else new_name
        return f"{scheme}{new_rest}"
    else:
        parts = location.split(".")
        parts[-1] = f"{parts[-1]}__pub_backup"
        return ".".join(parts)
