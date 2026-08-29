"""Where a bundle of files came from, and what is in it.

Two questions get run together constantly, so this module answers them
separately:

* **What bytes are these?** ``content_digest`` — a hash over the file set and
  config text actually being sent. It moves iff the content moves, which is
  what makes it comparable across machines, checkouts and clock skew.
* **Where did they come from?** ``resolve_origin`` — the repository the
  directory sits in (remote URL, commit, path within the repo) or, with no
  repository, a stable opaque token for the local directory. Answered by an
  ordered registry of ``SourceProvider``s, git first, with the path provider
  as a fallback that always answers.

Two things are derived from an origin and are load-bearing beyond provenance:
``SourceOrigin.source_key()``, the identity a declarative sync upserts jobs
against, and ``SourceOrigin.environment_label()``, the deployment namespace a
non-default branch builds into.

Deliberately stdlib-only and subprocess-free, for the same reason
``project_config`` is: ``trilogy cloud`` must not pay for the executor stack
to push a job, and must work on a machine where ``git`` is not installed. Git
facts are read straight out of ``.git``.

**Absolute paths never leave the machine.** With no git remote to name, a
project's origin is ``local:<label>-<digest>`` — the label is the directory
name, the digest is of the canonicalized path. That is exactly the studio
store id's construction (``path_token``, called by ``serve.build_store_id``),
so the same directory identifies itself consistently to a studio store and to
the cloud, and neither carries a filesystem layout off the box.

This is **not** ``trilogy.core.fingerprint``. That one hashes parsed model
objects to answer "did the semantics change, and which datasources need
rebuild"; it requires a parse, and it deliberately ignores changes that cannot
affect a build. This one is provenance of bytes: it notices a comment edit,
because a comment edit is a different bundle, and it never parses anything.
Nor is it ``serve_helpers.state_cache.fingerprint_directory``, which hashes
size and mtime rather than content on purpose — it runs on every cache read
and must stay cheaper than the probe it guards.
"""

from __future__ import annotations

import hashlib
import os
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from urllib.parse import urlsplit

#: Bumped when the digest construction or the origin encoding changes, so a
#: recorded fingerprint is never compared against one built by different
#: rules. Travels on the wire beside the values it describes.
SOURCE_FINGERPRINT_VERSION = 1

#: How much of the path digest the local token carries. Eight hex characters
#: is what the studio store id has always used; it is a collision guard
#: between a handful of directories on one machine, not a security boundary.
PATH_DIGEST_CHARS = 8

_LABEL_DISALLOWED = re.compile(r"[^a-z0-9._-]+")
_LABEL_MAX_CHARS = 40

#: Remotes are ranked so two clones of the same repo agree on which one names
#: it. "origin" is the convention; past that, alphabetical beats
#: file-order-in-.git/config, which is insertion order and differs per clone.
_PREFERRED_REMOTE = "origin"

#: Branches that mean "this is the mainline" and so map to *no* environment:
#: the default environment builds unprefixed addresses, which is what makes a
#: main-branch deploy write production's own tables.
DEFAULT_BRANCHES: tuple[str, ...] = ("main", "master")

#: An environment label becomes an unquoted SQL identifier prefix and a
#: filename suffix, so it must satisfy ``execution.envs._VALID_ENV_NAME``
#: (``^[A-Za-z_][A-Za-z0-9_]*$``). That rule is *not* imported: this module is
#: deliberately stdlib-only (see the module docstring), and `execution.envs`
#: pulls the datasource/engine stack that `trilogy cloud` must not pay for.
#: `tests/cli/test_source_identity.py` imports both and asserts they agree,
#: which is what keeps the duplication from drifting.
_ENV_LABEL_DISALLOWED = re.compile(r"[^a-z0-9]+")

#: The same rule stated positively, for names that did *not* come from
#: `environment_label` — a `--environment` typed at the command line, which is
#: the only way an unusable name can reach the platform.
_VALID_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

#: Slug length before the disambiguating digest. Kept short because the label
#: is prepended to every managed table name and appended to every managed
#: filename, and some warehouses cap identifiers well below 128 characters.
ENV_LABEL_SLUG_CHARS = 24

#: Two different branches must never share a namespace — `feature/x` and
#: `feature-x` both slugify to `feature_x`, and would otherwise build into each
#: other's tables. The digest is over the *original* branch name, so it
#: separates them; it is a collision guard, not a security boundary.
ENV_LABEL_DIGEST_CHARS = 6


@dataclass(frozen=True)
class SourceOrigin:
    """Where a directory's contents live, in terms that survive the trip.

    ``kind`` is ``"git"`` when a remote could be named and ``"path"``
    otherwise — including for a repository whose only remote is itself a local
    path, which would name a filesystem layout rather than a shared location.
    """

    kind: str
    #: ``host/owner/repo`` for git, ``local:<label>-<digest>`` for a path.
    #: Never an absolute path, and never carries credentials.
    location: str
    #: POSIX path of the directory within the repository, ``"."`` at the root.
    #: ``None`` for path origins, where the enclosing tree is not known.
    subpath: str | None = None
    #: The commit ``HEAD`` resolved to when this was read. ``None`` on an
    #: unborn branch, or when the ref could not be resolved.
    revision: str | None = None
    branch: str | None = None

    @property
    def is_git(self) -> bool:
        return self.kind == "git"

    def describe(self) -> str:
        """One line for a human: the location, plus whatever narrows it."""
        text = self.location
        if self.subpath and self.subpath != ".":
            text += f"/{self.subpath}"
        detail = [part for part in (self.branch, (self.revision or "")[:12]) if part]
        return f"{text} ({' @ '.join(detail)})" if detail else text

    def source_key(self) -> str:
        """Stable identity for the *thing* this directory defines, across every
        branch and checkout of it.

        ``{location}#{subpath}`` — deliberately excluding revision and branch,
        which are what a job varies *over*. This is what lets a branch's job be
        grouped under the mainline job it forked from, and what a declarative
        sync upserts against: a job's display name is derived from its path and
        is free to change, but its identity must not.
        """
        return f"{self.location}#{self.subpath or '.'}"

    def environment_label(
        self, default_branches: Iterable[str] = DEFAULT_BRANCHES
    ) -> str | None:
        """The deployment-environment name this checkout builds into, or
        ``None`` for the mainline (which builds unprefixed production
        addresses). See :func:`environment_label`."""
        return environment_label(self.branch, default_branches)


# ============================================================================
# Path identity
# ============================================================================


def label_token(value: str) -> str:
    """A filesystem- and URL-safe label, or ``""`` if nothing survives."""
    return (
        _LABEL_DISALLOWED.sub("-", value.strip().lower())
        .strip("-.")[:_LABEL_MAX_CHARS]
        .strip("-.")
    )


def environment_label(
    branch: str | None, default_branches: Iterable[str] = DEFAULT_BRANCHES
) -> str | None:
    """The deployment-environment name a branch builds into.

    ``None`` for a default branch, and for no branch at all (a detached HEAD or
    a non-repository directory has no branch to scope to). ``None`` means the
    *production* namespace — unprefixed addresses — so the mapping is
    deliberately conservative: an unrecognized state builds where it always did
    rather than inventing a namespace nobody will think to clean up.

    Everything else becomes ``<slug>_<digest>``, which is guaranteed to satisfy
    the identifier rule the environment machinery enforces:

    * lowercased, with every run of non-alphanumerics collapsed to one ``_``
    * trimmed of leading/trailing ``_`` (so it cannot start with one and read
      as a private identifier) and capped at ``ENV_LABEL_SLUG_CHARS``
    * prefixed ``b_`` when what survives starts with a digit, since an
      identifier may not
    * suffixed with a digest of the *original* branch name, because the
      collapsing is lossy and two branches must never share a namespace

    A branch that sanitizes away entirely (``---``, or a non-Latin name) is not
    an error: it keeps the digest alone, which is still a valid, unique label.
    """
    if branch is None:
        return None
    text = branch.strip()
    if not text or text in set(default_branches):
        return None
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:ENV_LABEL_DIGEST_CHARS]
    slug = _ENV_LABEL_DISALLOWED.sub("_", text.lower()).strip("_")
    slug = slug[:ENV_LABEL_SLUG_CHARS].strip("_")
    if not slug:
        return f"b_{digest}"
    if slug[0].isdigit():
        slug = f"b_{slug}"
    return f"{slug}_{digest}"


def is_valid_environment_name(name: str) -> bool:
    """Whether *name* can be used as an environment.

    Everything :func:`environment_label` builds satisfies this by
    construction; a name a human typed does not, and a rejected one is worth
    catching before it becomes a row nothing can build into.
    """
    return bool(_VALID_ENV_NAME.match(name))


def path_digest(directory: Path) -> str:
    """Digest of the canonicalized directory path.

    ``realpath`` + ``normcase`` so that two spellings of one directory — a
    trailing ``/.``, a symlink, a different case on Windows — are one
    identity, and two same-named directories in different trees are two.
    """
    canonical = os.path.normcase(os.path.realpath(directory))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:PATH_DIGEST_CHARS]


def path_token(directory: Path, label: str | None = None) -> str:
    """``<label>-<digest>`` for a directory: readable, stable, and opaque
    about where the directory actually is.

    The digest rather than the path keeps the filesystem layout out of
    anything that stores this — a studio client's storage keys, a cloud job's
    recorded provenance. The label is a courtesy for humans reading it back
    and carries no identity of its own, so a label that sanitizes away leaves
    the bare digest rather than an empty string.
    """
    slug = label_token(label if label is not None else directory.name)
    digest = path_digest(directory)
    return f"{slug}-{digest}" if slug else digest


# ============================================================================
# Git identity, read from .git rather than from `git`
# ============================================================================


def _resolve_git_dir(start: Path) -> tuple[Path, Path] | None:
    """``(git_dir, common_dir)`` for the repository containing *start*.

    The two differ inside a linked worktree, where ``HEAD`` is per-worktree but
    ``config`` and ``packed-refs`` live in the main repository — reading both
    from the same directory would silently find no remote in every worktree.
    """
    try:
        current = start.resolve()
    except OSError:
        return None
    for candidate in [current, *current.parents]:
        marker = candidate / ".git"
        if marker.is_dir():
            return marker, marker
        if marker.is_file():
            # A worktree or submodule: the file points at the real git dir.
            try:
                content = marker.read_text(encoding="utf-8").strip()
            except OSError:
                return None
            if not content.startswith("gitdir:"):
                return None
            git_dir = Path(content[len("gitdir:") :].strip())
            if not git_dir.is_absolute():
                git_dir = (candidate / git_dir).resolve()
            common = git_dir
            common_file = git_dir / "commondir"
            if common_file.is_file():
                try:
                    rel = common_file.read_text(encoding="utf-8").strip()
                except OSError:
                    rel = ""
                if rel:
                    common = (git_dir / rel).resolve()
            return git_dir, common
    return None


def repository_root(start: Path) -> Path | None:
    """The working-tree root containing *start*, or ``None``."""
    if _resolve_git_dir(start) is None:
        return None
    # The marker itself anchors the working tree — `.git` is a directory in a
    # plain checkout and a file in a linked worktree, and in both cases its
    # parent is the root. The call above already proved one exists.
    try:
        current = start.resolve()
    except OSError:
        return None
    for candidate in [current, *current.parents]:
        if (candidate / ".git").exists():
            return candidate
    return None


def _parse_git_config(path: Path) -> dict[str, dict[str, str]]:
    """``{section: {key: value}}`` from a git config file.

    Hand-rolled rather than ``configparser``: git indents keys with a tab, and
    ``configparser`` reads an indented line as a continuation of the previous
    value — so every remote URL would come back glued to the section before
    it. Only what this module reads is understood; anything else is skipped
    rather than raised on, because an unreadable config means "no git
    identity", never a failed push.
    """
    sections: dict[str, dict[str, str]] = {}
    current: dict[str, str] | None = None
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return sections
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line[0] in "#;":
            continue
        if line.startswith("[") and line.endswith("]"):
            name = line[1:-1].strip()
            # `[remote "origin"]` — subsection quoting normalized to a dot.
            match = re.match(r'^(\S+)\s+"(.*)"$', name)
            if match:
                name = f"{match.group(1)}.{match.group(2)}"
            current = sections.setdefault(name.lower(), {})
            continue
        if current is None or "=" not in line:
            continue
        key, _, value = line.partition("=")
        current[key.strip().lower()] = value.strip()
    return sections


def _remote_url(sections: Mapping[str, Mapping[str, str]]) -> str | None:
    remotes = {
        name[len("remote.") :]: values.get("url")
        for name, values in sections.items()
        if name.startswith("remote.") and values.get("url")
    }
    if not remotes:
        return None
    if _PREFERRED_REMOTE in remotes:
        return remotes[_PREFERRED_REMOTE]
    return remotes[min(remotes)]


def normalize_remote(url: str) -> str | None:
    """``host/owner/repo`` for a remote that names a host, else ``None``.

    Every spelling of one repository collapses to one string — ``ssh://``,
    ``scp``-style ``git@host:path``, and ``https://`` all normalize together,
    so a push from a colleague's clone matches a push from CI.

    Credentials are dropped, not carried: a remote written as
    ``https://user:token@host/repo`` is common in CI checkouts, and a
    fingerprint is a thing that gets stored and displayed. Returning ``None``
    for a hostless remote (``file://``, a bare local path, a relative
    submodule URL, a Windows drive path) is deliberate — those name a
    filesystem, so the caller falls back to path identity instead of
    publishing someone's disk layout.
    """
    text = url.strip()
    if not text:
        return None
    # scp-style: git@host:owner/repo.git — not a URL, and urlsplit reads the
    # whole thing as a path.
    scp = re.match(r"^(?:(?P<user>[^@/]+)@)?(?P<host>[^:/@]+):(?P<path>.+)$", text)
    if scp and "//" not in text:
        host, path = scp.group("host"), scp.group("path")
        # `C:/dev/models` matches that pattern as host `C`; git resolves the
        # same ambiguity in favour of the drive letter, and so must this —
        # reading it as a host would put a local disk layout in the origin.
        if len(host) == 1:
            return None
    else:
        parts = urlsplit(text)
        if parts.scheme in ("", "file") or not parts.hostname:
            return None
        host, path = parts.hostname, parts.path
    host = host.lower().strip("/")
    path = path.strip("/")
    if path.lower().endswith(".git"):
        path = path[: -len(".git")]
    if not host or not path:
        return None
    return f"{host}/{path}"


def _read_head(git_dir: Path, common_dir: Path) -> tuple[str | None, str | None]:
    """``(revision, branch)`` for the checked-out ``HEAD``."""
    try:
        head = (git_dir / "HEAD").read_text(encoding="utf-8").strip()
    except OSError:
        return None, None
    if not head.startswith("ref:"):
        # Detached HEAD: the file is the commit.
        return (head or None), None
    ref = head[len("ref:") :].strip()
    branch = ref.split("/", 2)[-1] if ref.startswith("refs/heads/") else ref
    for base in (git_dir, common_dir):
        loose = base / Path(ref)
        try:
            if loose.is_file():
                return loose.read_text(encoding="utf-8").strip() or None, branch
        except OSError:
            pass
    # Not a loose ref: it may have been packed by `git gc`.
    for base in (common_dir, git_dir):
        packed = base / "packed-refs"
        if not packed.is_file():
            continue
        try:
            lines = packed.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for line in lines:
            # `^<sha>` lines peel an annotated tag; they describe the line
            # above, not a ref of their own.
            if not line or line[0] in "#^":
                continue
            sha, _, name = line.partition(" ")
            if name.strip() == ref:
                return sha.strip() or None, branch
    # An unborn branch (`git init`, nothing committed) reaches here, and is a
    # real state rather than a failure: the branch is known, the commit is not.
    return None, branch


# ============================================================================
# Providers: one per kind of source that can name a directory
# ============================================================================


class SourceProvider(Protocol):
    """One way of answering "where did this directory come from?".

    Providers are consulted in order and the first non-``None`` answer wins.
    A provider **never raises**: an unreadable, half-written or unrecognized
    source of truth means "I cannot name this directory", which is the next
    provider's problem. Provenance is a nice-to-have on a push and must never
    be able to fail one.
    """

    #: Matches ``SourceOrigin.kind`` for the origins this returns.
    kind: str

    def detect(self, directory: Path) -> SourceOrigin | None: ...


class GitSourceProvider:
    """Identity from a git checkout, read straight out of ``.git``.

    Answers ``None`` — rather than a half-populated origin — when the directory
    is not in a repository, or when the repository names no remote that
    identifies a *host*. A remote that names only a filesystem is not an
    identity worth publishing; see :func:`normalize_remote`.
    """

    kind = "git"

    def detect(self, directory: Path) -> SourceOrigin | None:
        resolved = _resolve_git_dir(directory)
        if resolved is None:
            return None
        git_dir, common_dir = resolved
        remote = _remote_url(_parse_git_config(common_dir / "config"))
        location = normalize_remote(remote) if remote else None
        if not location:
            return None
        revision, branch = _read_head(git_dir, common_dir)
        root = repository_root(directory)
        subpath = None
        if root is not None:
            try:
                subpath = directory.resolve().relative_to(root).as_posix() or "."
            except (OSError, ValueError):
                subpath = None
        return SourceOrigin(
            kind=self.kind,
            location=location,
            subpath=subpath or ".",
            revision=revision,
            branch=branch,
        )


class PathSourceProvider:
    """The terminal fallback: a directory always has a path identity.

    Never returns ``None``, which is what makes :func:`resolve_origin` total.
    Kept out of the registry for exactly that reason — a registered provider
    could otherwise be appended after it and never be reached.
    """

    kind = "path"

    def detect(self, directory: Path) -> SourceOrigin:
        return SourceOrigin(kind=self.kind, location=f"local:{path_token(directory)}")


#: Ordered, most specific first. Extend with :func:`register_source_provider`
#: (a mercurial or svn reader, a CI environment that knows its own checkout).
_PROVIDERS: list[SourceProvider] = [GitSourceProvider()]

#: Deliberately not in ``_PROVIDERS``: see PathSourceProvider.
_FALLBACK_PROVIDER = PathSourceProvider()


def register_source_provider(provider: SourceProvider, first: bool = False) -> None:
    """Add a provider ahead of the path fallback, which always stays last."""
    if first:
        _PROVIDERS.insert(0, provider)
    else:
        _PROVIDERS.append(provider)


def source_providers() -> tuple[SourceProvider, ...]:
    """The registered providers, in the order they are consulted. The path
    fallback is not among them — it is not optional and cannot be displaced."""
    return tuple(_PROVIDERS)


def resolve_origin(directory: Path) -> SourceOrigin:
    """Best available identity for *directory*: the first provider that can
    name it, else its path.

    Never raises and never blocks. Total by construction — the path fallback
    always answers.
    """
    for provider in _PROVIDERS:
        origin = provider.detect(directory)
        if origin is not None:
            return origin
    return _FALLBACK_PROVIDER.detect(directory)


# ============================================================================
# Content identity
# ============================================================================


def content_digest(
    config_text: str | None,
    files: Iterable[Mapping[str, str]],
) -> str:
    """Hex digest of exactly the content being pushed.

    Order-independent (entries are sorted by name) and whitespace-exact: the
    point is to answer "is this the same bundle the last push sent", and a
    fingerprint that forgave reformatting would answer a different question
    than the one the server's own content comparison answers.

    Names and contents are length-prefixed so no rename can produce the digest
    of a different file set — ``{"ab": "c"}`` and ``{"a": "bc"}`` are
    different bundles and hash differently.

    *files* are ``{"name", "content"}`` entries, the same shape the cloud API
    takes; entries missing either key are treated as empty rather than
    rejected, since the caller has already validated the bundle it built.
    """
    digest = hashlib.sha256()
    digest.update(f"trilogy-source/{SOURCE_FINGERPRINT_VERSION}\n".encode())
    config = (config_text or "").encode("utf-8")
    digest.update(f"config:{len(config)}:".encode())
    digest.update(config)
    entries = sorted(
        (str(entry.get("name", "")), str(entry.get("content", "")).encode("utf-8"))
        for entry in files
    )
    digest.update(f"\nfiles:{len(entries)}\n".encode())
    for name, content in entries:
        encoded_name = name.encode("utf-8")
        digest.update(f"{len(encoded_name)}:".encode())
        digest.update(encoded_name)
        digest.update(f":{len(content)}:".encode())
        digest.update(content)
        digest.update(b"\n")
    return digest.hexdigest()
