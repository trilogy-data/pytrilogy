"""Tests for the `trilogy cloud` command's plumbing: environment resolution,
credential storage, project bundling, and response parsing. Network
interactions are exercised against fakes — the HTTP client itself is a thin
urllib wrapper."""

import json
from pathlib import Path

import click
import pytest

from trilogy.scripts import cloud as cloud_mod
from trilogy.scripts.cloud import (
    DEFAULT_API_URL,
    CloudClient,
    CloudError,
    _find_job,
    apply_rewrites,
    check_bundle_size,
    collect_files,
    forget_token,
    parse_rewrite,
    resolve_api_url,
    store_token,
    stored_token,
)
from trilogy.scripts.cloud_models import Job, Me, ScheduleExt

TS = "2026-07-28T12:00:00Z"


@pytest.fixture(autouse=True)
def _clear_config_cache():
    """The [cloud] table is cached per directory; tests chdir between cases."""
    cloud_mod._cloud_table.cache_clear()
    yield
    cloud_mod._cloud_table.cache_clear()


def _job(job_id: str, name: str) -> Job:
    return Job(
        id=job_id,
        org_id="o1",
        name=name,
        operation="run",
        created_at=TS,
        updated_at=TS,
    )


class TestApiUrlResolution:
    def test_default_is_production(self, tmp_path, monkeypatch):
        monkeypatch.delenv("TRILOGY_CLOUD_API", raising=False)
        monkeypatch.chdir(tmp_path)
        assert resolve_api_url(None) == DEFAULT_API_URL

    def test_explicit_flag_wins(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TRILOGY_CLOUD_API", "https://env.example")
        (tmp_path / "trilogy.toml").write_text(
            '[cloud]\napi_url = "https://conf.example"\n', encoding="utf-8"
        )
        monkeypatch.chdir(tmp_path)
        assert resolve_api_url("https://flag.example/") == "https://flag.example"

    def test_env_beats_config(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TRILOGY_CLOUD_API", "https://env.example")
        (tmp_path / "trilogy.toml").write_text(
            '[cloud]\napi_url = "https://conf.example"\n', encoding="utf-8"
        )
        monkeypatch.chdir(tmp_path)
        assert resolve_api_url(None) == "https://env.example"

    def test_trilogy_toml_overrides_default(self, tmp_path, monkeypatch):
        monkeypatch.delenv("TRILOGY_CLOUD_API", raising=False)
        (tmp_path / "trilogy.toml").write_text(
            '[cloud]\napi_url = "https://conf.example"\n', encoding="utf-8"
        )
        monkeypatch.chdir(tmp_path)
        assert resolve_api_url(None) == "https://conf.example"

    def test_toml_is_found_upward_from_a_subdirectory(self, tmp_path, monkeypatch):
        monkeypatch.delenv("TRILOGY_CLOUD_API", raising=False)
        (tmp_path / "trilogy.toml").write_text(
            '[cloud]\napi_url = "https://conf.example"\n', encoding="utf-8"
        )
        nested = tmp_path / "models" / "deep"
        nested.mkdir(parents=True)
        monkeypatch.chdir(nested)
        assert resolve_api_url(None) == "https://conf.example"

    def test_org_is_read_from_the_cloud_table(self, tmp_path, monkeypatch):
        (tmp_path / "trilogy.toml").write_text(
            '[cloud]\norg = "acme"\n', encoding="utf-8"
        )
        monkeypatch.chdir(tmp_path)
        assert cloud_mod._project_cloud_config().get("org") == "acme"

    def test_unparseable_toml_warns_and_falls_back(self, tmp_path, monkeypatch):
        monkeypatch.delenv("TRILOGY_CLOUD_API", raising=False)
        (tmp_path / "trilogy.toml").write_text("not = = toml", encoding="utf-8")
        monkeypatch.chdir(tmp_path)
        assert resolve_api_url(None) == DEFAULT_API_URL


class TestCredentials:
    def test_round_trip_and_forget(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cloud_mod, "CREDENTIALS_PATH", tmp_path / "creds.json")
        url = "https://api.example"
        assert stored_token(url) is None
        store_token(url, "tri_abc", "a@b.co")
        assert stored_token(url) == "tri_abc"
        assert forget_token(url) is True
        assert stored_token(url) is None
        assert forget_token(url) is False

    def test_tokens_are_stored_per_api_url(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cloud_mod, "CREDENTIALS_PATH", tmp_path / "creds.json")
        store_token("https://dev.example", "tri_dev", None)
        store_token("https://prod.example", "tri_prod", None)
        assert stored_token("https://dev.example") == "tri_dev"
        assert stored_token("https://prod.example") == "tri_prod"


class TestBundling:
    def _project(self, tmp_path: Path) -> Path:
        (tmp_path / "model.preql").write_text("key id int;", encoding="utf-8")
        (tmp_path / "nested").mkdir()
        (tmp_path / "nested" / "helper.py").write_text("x = 1", encoding="utf-8")
        (tmp_path / "tests").mkdir()
        (tmp_path / "tests" / "test_x.preql").write_text("skip", encoding="utf-8")
        (tmp_path / "data.parquet").write_bytes(b"\x00binary")
        return tmp_path

    def test_directory_shape_survives(self, tmp_path):
        files = collect_files(self._project(tmp_path))
        names = [f["name"] for f in files]
        assert "model.preql" in names
        assert "nested/helper.py" in names

    def test_default_excludes_apply(self, tmp_path):
        files = collect_files(self._project(tmp_path))
        names = [f["name"] for f in files]
        assert not any(n.startswith("tests/") for n in names)
        assert not any(n.endswith(".parquet") for n in names)

    def test_root_tests_dir_is_excluded_via_the_leading_slash_form(self, tmp_path):
        (tmp_path / "tests").mkdir()
        (tmp_path / "tests" / "keep.preql").write_text("x", encoding="utf-8")
        (tmp_path / "keep.preql").write_text("x", encoding="utf-8")
        names = [f["name"] for f in collect_files(tmp_path)]
        assert names == ["keep.preql"]

    def test_oversized_bundle_is_refused(self):
        payload = {"files": [{"name": "big", "content": "x" * (9 * 1024 * 1024)}]}
        with pytest.raises(CloudError, match="budget"):
            check_bundle_size(payload)

    def test_encoded_bundle_is_returned_for_reuse(self):
        payload = {"files": []}
        assert check_bundle_size(payload) == json.dumps(payload).encode("utf-8")


class TestRewrites:
    def test_literal_substitution_ignores_regex_metacharacters(self):
        assert apply_rewrites("gs://bucket/a.b/c", [("a.b", "X")]) == "gs://bucket/X/c"

    def test_rewrites_apply_in_order(self):
        assert apply_rewrites("a", [("a", "b"), ("b", "c")]) == "c"

    def test_parse_splits_on_the_first_equals_only(self):
        assert parse_rewrite(None, None, ("k=v=w",)) == [("k", "v=w")]

    def test_parse_allows_an_empty_replacement(self):
        assert parse_rewrite(None, None, ("drop=",)) == [("drop", "")]

    def test_parse_rejects_a_missing_equals(self):
        with pytest.raises(click.BadParameter, match="OLD=NEW"):
            parse_rewrite(None, None, ("nope",))

    def test_parse_rejects_an_empty_old(self):
        with pytest.raises(click.BadParameter, match="must not be empty"):
            parse_rewrite(None, None, ("=new",))


class TestFindJob:
    def test_finds_by_exact_id(self):
        assert _find_job([_job("j1", "a"), _job("j2", "b")], "org", "j2").name == "b"

    def test_finds_by_unique_name(self):
        assert _find_job([_job("j1", "a"), _job("j2", "b")], "org", "a").id == "j1"

    def test_id_wins_over_a_name_collision(self):
        assert _find_job([_job("j1", "j2"), _job("j2", "b")], "org", "j2").name == "b"

    def test_ambiguous_name_is_an_error_listing_ids(self):
        with pytest.raises(CloudError, match="j1"):
            _find_job([_job("j1", "a"), _job("j2", "a")], "org", "a")

    def test_unknown_job_is_an_error(self):
        with pytest.raises(CloudError, match="nope"):
            _find_job([], "org", "nope")


class TestResponseParsing:
    """The client validates against the models rather than indexing raw dicts."""

    def _client(self, payload):
        client = CloudClient(api_url="https://x", token="t")
        client.request = lambda *a, **k: payload  # type: ignore[method-assign]
        return client

    def test_get_many_parses_each_row(self):
        rows = self._client(
            [
                {
                    "id": "j1",
                    "org_id": "o1",
                    "name": "nightly",
                    "operation": "refresh",
                    "timeout_seconds": 60,
                    "created_at": TS,
                    "updated_at": TS,
                }
            ]
        ).get_many("/orgs/o/jobs", Job)
        assert rows[0].operation == "refresh" and rows[0].timeout_seconds == 60

    def test_get_many_treats_an_empty_body_as_no_rows(self):
        assert self._client(None).get_many("/orgs/o/jobs", Job) == []

    def test_unexpected_shape_names_the_field(self):
        with pytest.raises(CloudError, match="operation"):
            self._client(
                [
                    {
                        "id": "j1",
                        "org_id": "o1",
                        "name": "n",
                        "created_at": TS,
                        "updated_at": TS,
                    }
                ]
            ).get_many("/orgs/o/jobs", Job)

    def test_a_non_list_body_for_a_list_route_is_an_error(self):
        with pytest.raises(CloudError, match="expected a list"):
            self._client({"nope": 1}).get_many("/orgs/o/jobs", Job)

    def test_unknown_fields_are_ignored_so_a_newer_api_still_parses(self):
        me = self._client(
            {
                "id": "u1",
                "email": "a@b.co",
                "provider": "google",
                "orgs": [],
                "field_from_the_future": True,
            }
        ).get_one("/auth/me", Me)
        assert me.email == "a@b.co"

    def test_schedule_ext_defaults_job_names_when_absent(self):
        sched = self._client(
            {
                "id": "s1",
                "org_id": "o1",
                "name": "n",
                "cron_expr": "0 3 * * *",
                "is_active": True,
                "next_run_at": TS,
                "created_at": TS,
                "updated_at": TS,
            }
        ).get_one("/orgs/o/schedules/s1", ScheduleExt)
        assert sched.job_names == []


class TestLoginLoopback:
    """The loopback handler is the CSRF boundary: only a callback echoing the
    nonce we sent out may plant a credential."""

    def _serve(self):
        import threading
        from http.server import HTTPServer

        from trilogy.scripts.cloud import _LoginResult, _make_login_handler

        result = _LoginResult()
        nonce = "abc123XYZ-_abc123XYZ"
        server = HTTPServer(("127.0.0.1", 0), _make_login_handler(result, nonce))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        port = server.server_address[1]
        return server, thread, result, nonce, port

    def _get(self, port, path):
        import urllib.error
        import urllib.request

        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{port}{path}", timeout=5
            ) as resp:
                return resp.status
        except urllib.error.HTTPError as exc:
            return exc.code

    def _shutdown(self, server, thread):
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()

    def test_matching_state_delivers_the_token(self):
        server, thread, result, nonce, port = self._serve()
        try:
            status = self._get(port, f"/callback?token=tri_good&state={nonce}")
            assert status == 200
            assert result.token == "tri_good"
            assert result.event.is_set()
        finally:
            self._shutdown(server, thread)

    def test_missing_or_wrong_state_is_rejected_and_keeps_listening(self):
        server, thread, result, nonce, port = self._serve()
        try:
            assert self._get(port, "/callback?token=tri_evil") == 403
            assert self._get(port, "/callback?token=tri_evil&state=wrong") == 403
            assert result.token is None
            assert not result.event.is_set()
            # The genuine redirect still lands afterwards.
            assert self._get(port, f"/callback?token=tri_good&state={nonce}") == 200
            assert result.token == "tri_good"
        finally:
            self._shutdown(server, thread)

    def test_non_callback_paths_are_404(self):
        server, thread, result, nonce, port = self._serve()
        try:
            assert self._get(port, f"/?token=tri_evil&state={nonce}") == 404
            assert self._get(port, "/favicon.ico") == 404
            assert result.token is None
        finally:
            self._shutdown(server, thread)
