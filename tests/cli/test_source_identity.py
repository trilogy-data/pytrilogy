"""Tests for source identity: what a bundle is, and where it came from.

The git half is exercised against real ``.git`` layouts written by hand rather
than by shelling out to ``git`` — that is exactly what the module under test
does, and a test that needed ``git`` installed would not cover the case the
module exists for.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from trilogy.scripts.serve import build_store_id
from trilogy.scripts.source_identity import (
    SOURCE_FINGERPRINT_VERSION,
    content_digest,
    label_token,
    normalize_remote,
    path_token,
    repository_root,
    resolve_origin,
)


def _git_repo(root: Path, remote: str | None, branch: str = "main") -> Path:
    """A checkout with just the files this module reads."""
    git = root / ".git"
    (git / "refs" / "heads").mkdir(parents=True)
    config = "[core]\n\trepositoryformatversion = 0\n"
    if remote is not None:
        config += f'[remote "origin"]\n\turl = {remote}\n\tfetch = +refs/heads/*\n'
    (git / "config").write_text(config, encoding="utf-8")
    (git / "HEAD").write_text(f"ref: refs/heads/{branch}\n", encoding="utf-8")
    (git / "refs" / "heads" / branch).write_text("a" * 40 + "\n", encoding="utf-8")
    return root


class TestContentDigest:
    def test_the_same_bundle_digests_the_same_in_any_order(self):
        files = [{"name": "a.preql", "content": "x"}, {"name": "b.py", "content": "y"}]
        assert content_digest("cfg", files) == content_digest("cfg", reversed(files))

    def test_the_config_is_part_of_the_bundle(self):
        files = [{"name": "a.preql", "content": "x"}]
        assert content_digest("one", files) != content_digest("two", files)

    def test_a_content_edit_moves_the_digest(self):
        before = content_digest("cfg", [{"name": "a.preql", "content": "x"}])
        after = content_digest("cfg", [{"name": "a.preql", "content": "x "}])
        assert before != after

    def test_a_rename_is_not_a_content_edit_in_disguise(self):
        """Length-prefixing is what stops name and content bleeding together."""
        left = content_digest(None, [{"name": "ab", "content": "c"}])
        right = content_digest(None, [{"name": "a", "content": "bc"}])
        assert left != right

    def test_an_absent_config_and_an_empty_one_agree(self):
        """A job with no trilogy.toml sends no config; the platform treats an
        empty one the same way, and so must this."""
        assert content_digest(None, []) == content_digest("", [])

    def test_a_missing_key_is_treated_as_empty_rather_than_raising(self):
        assert content_digest("cfg", [{"name": "a.preql"}])

    def test_the_version_is_mixed_in(self, monkeypatch):
        first = content_digest("cfg", [])
        monkeypatch.setattr(
            "trilogy.scripts.source_identity.SOURCE_FINGERPRINT_VERSION",
            SOURCE_FINGERPRINT_VERSION + 1,
        )
        assert content_digest("cfg", []) != first


class TestRemoteNormalization:
    @pytest.mark.parametrize(
        "url",
        [
            "git@github.com:trilogy-data/pytrilogy.git",
            "https://github.com/trilogy-data/pytrilogy.git",
            "https://github.com/trilogy-data/pytrilogy",
            "ssh://git@github.com/trilogy-data/pytrilogy.git",
            "git://github.com/trilogy-data/pytrilogy.git",
        ],
    )
    def test_every_spelling_of_one_repository_collapses(self, url):
        assert normalize_remote(url) == "github.com/trilogy-data/pytrilogy"

    def test_credentials_are_dropped(self):
        """CI checkouts routinely embed a token; a fingerprint gets stored and
        displayed, so it must not carry one."""
        normalized = normalize_remote("https://user:ghp_secret@github.com/acme/etl.git")
        assert normalized == "github.com/acme/etl"
        assert "ghp_secret" not in normalized

    def test_the_host_is_case_folded(self):
        assert normalize_remote("git@GitHub.com:Acme/ETL.git") == "github.com/Acme/ETL"

    @pytest.mark.parametrize(
        "url",
        ["file:///srv/repos/etl.git", "/srv/repos/etl.git", "../sibling.git", ""],
    )
    def test_a_hostless_remote_names_a_filesystem_and_is_refused(self, url):
        assert normalize_remote(url) is None

    @pytest.mark.parametrize(
        "url", ["C:/dev/models", "c:/dev/models.git", r"D:\repos\etl", "Z:/x"]
    )
    def test_a_windows_drive_path_is_not_a_host(self, url):
        """`C:/dev/models` is scp-style shaped, with `C` where the host goes.
        Reading it as one would publish a disk layout as the origin — the thing
        this module exists to keep on the machine."""
        assert normalize_remote(url) is None

    def test_a_single_letter_host_is_still_a_host_when_a_scheme_says_so(self):
        """The drive-letter rule is scoped to the ambiguous spelling; an
        explicit scheme is not ambiguous."""
        assert normalize_remote("ssh://git@h/acme/etl.git") == "h/acme/etl"


class TestGitOrigin:
    def test_a_checkout_reports_its_remote_commit_and_branch(self, tmp_path):
        _git_repo(tmp_path, "git@github.com:acme/etl.git", branch="release")
        origin = resolve_origin(tmp_path)
        assert origin.kind == "git"
        assert origin.location == "github.com/acme/etl"
        assert origin.revision == "a" * 40
        assert origin.branch == "release"
        assert origin.subpath == "."

    def test_a_subdirectory_is_located_within_the_repository(self, tmp_path):
        _git_repo(tmp_path, "git@github.com:acme/etl.git")
        job_dir = tmp_path / "demo_models" / "user_analytics"
        job_dir.mkdir(parents=True)
        origin = resolve_origin(job_dir)
        assert origin.location == "github.com/acme/etl"
        assert origin.subpath == "demo_models/user_analytics"

    def test_a_packed_ref_still_resolves(self, tmp_path):
        _git_repo(tmp_path, "git@github.com:acme/etl.git")
        (tmp_path / ".git" / "refs" / "heads" / "main").unlink()
        (tmp_path / ".git" / "packed-refs").write_text(
            "# pack-refs with: peeled fully-peeled sorted\n"
            f"{'b' * 40} refs/heads/main\n"
            f"^{'c' * 40}\n",
            encoding="utf-8",
        )
        assert resolve_origin(tmp_path).revision == "b" * 40

    def test_a_detached_head_reports_the_commit_and_no_branch(self, tmp_path):
        _git_repo(tmp_path, "git@github.com:acme/etl.git")
        (tmp_path / ".git" / "HEAD").write_text("d" * 40 + "\n", encoding="utf-8")
        origin = resolve_origin(tmp_path)
        assert origin.revision == "d" * 40 and origin.branch is None

    def test_an_unborn_branch_is_a_state_not_a_failure(self, tmp_path):
        _git_repo(tmp_path, "git@github.com:acme/etl.git")
        (tmp_path / ".git" / "refs" / "heads" / "main").unlink()
        origin = resolve_origin(tmp_path)
        assert origin.kind == "git" and origin.branch == "main"
        assert origin.revision is None

    def test_a_repository_with_no_remote_falls_back_to_the_path(self, tmp_path):
        _git_repo(tmp_path, None)
        assert resolve_origin(tmp_path).kind == "path"

    def test_a_local_only_remote_falls_back_to_the_path(self, tmp_path):
        """A `file://` remote would publish someone's disk layout under the
        name of a shared location."""
        _git_repo(tmp_path, "/srv/mirrors/etl.git")
        assert resolve_origin(tmp_path).kind == "path"

    def test_origin_wins_over_other_remotes(self, tmp_path):
        _git_repo(tmp_path, "git@github.com:acme/etl.git")
        config = tmp_path / ".git" / "config"
        config.write_text(
            config.read_text(encoding="utf-8")
            + '[remote "upstream"]\n\turl = git@github.com:other/etl.git\n',
            encoding="utf-8",
        )
        assert resolve_origin(tmp_path).location == "github.com/acme/etl"

    def test_remotes_without_an_origin_are_ranked_deterministically(self, tmp_path):
        _git_repo(tmp_path, None)
        config = tmp_path / ".git" / "config"
        config.write_text(
            config.read_text(encoding="utf-8")
            + '[remote "zeta"]\n\turl = git@github.com:acme/zeta.git\n'
            + '[remote "alpha"]\n\turl = git@github.com:acme/alpha.git\n',
            encoding="utf-8",
        )
        assert resolve_origin(tmp_path).location == "github.com/acme/alpha"

    def test_a_worktree_reads_config_from_the_main_repository(self, tmp_path):
        main = _git_repo(tmp_path / "main", "git@github.com:acme/etl.git")
        worktree_git = main / ".git" / "worktrees" / "feature"
        worktree_git.mkdir(parents=True)
        (worktree_git / "commondir").write_text("../..\n", encoding="utf-8")
        (worktree_git / "HEAD").write_text(
            "ref: refs/heads/feature\n", encoding="utf-8"
        )
        (main / ".git" / "refs" / "heads" / "feature").write_text(
            "e" * 40, encoding="utf-8"
        )
        linked = tmp_path / "feature"
        linked.mkdir()
        (linked / ".git").write_text(f"gitdir: {worktree_git}\n", encoding="utf-8")

        origin = resolve_origin(linked)
        assert origin.location == "github.com/acme/etl"
        assert origin.revision == "e" * 40 and origin.branch == "feature"

    def test_a_corrupt_git_directory_degrades_to_the_path(self, tmp_path):
        """Provenance is a nice-to-have on a push and must never fail one."""
        (tmp_path / ".git").mkdir()
        (tmp_path / ".git" / "config").write_text("[remote \x00", encoding="utf-8")
        assert resolve_origin(tmp_path).kind == "path"

    def test_the_repository_root_is_the_directory_holding_the_marker(self, tmp_path):
        _git_repo(tmp_path, "git@github.com:acme/etl.git")
        nested = tmp_path / "a" / "b"
        nested.mkdir(parents=True)
        assert repository_root(nested) == tmp_path.resolve()

    def test_a_directory_outside_any_repository_has_no_root(self, tmp_path):
        assert repository_root(tmp_path) is None


class TestPathIdentity:
    def test_a_local_origin_never_carries_the_absolute_path(self, tmp_path):
        project = tmp_path / "user_analytics"
        project.mkdir()
        origin = resolve_origin(project)
        assert origin.kind == "path"
        assert origin.location.startswith("local:user_analytics-")
        assert str(tmp_path) not in origin.location

    def test_two_same_named_directories_are_two_identities(self, tmp_path):
        left = tmp_path / "a" / "etl"
        right = tmp_path / "b" / "etl"
        left.mkdir(parents=True)
        right.mkdir(parents=True)
        assert path_token(left) != path_token(right)

    def test_spellings_of_one_directory_are_one_identity(self, tmp_path):
        project = tmp_path / "etl"
        project.mkdir()
        assert path_token(project) == path_token(tmp_path / "etl" / ".")

    def test_a_label_that_sanitizes_away_leaves_the_bare_digest(self, tmp_path):
        assert path_token(tmp_path, "***") == path_token(tmp_path, "")
        assert "-" not in path_token(tmp_path, "***")

    def test_the_label_is_slugified(self):
        assert label_token("Example BigQuery Model!") == "example-bigquery-model"

    def test_the_studio_store_id_is_the_same_construction(self, tmp_path):
        """Consolidated deliberately: one directory identifies itself the same
        way to a studio store and to a pushed job."""
        assert build_store_id(tmp_path, "analytics") == path_token(
            tmp_path, "analytics"
        )

    def test_the_store_id_stays_path_based_inside_a_repository(self, tmp_path):
        """Two checkouts of one repo are two served projects; merging them into
        one store would collide their files."""
        left = _git_repo(tmp_path / "one", "git@github.com:acme/etl.git")
        right = _git_repo(tmp_path / "two", "git@github.com:acme/etl.git")
        assert build_store_id(left, None) != build_store_id(right, None)
