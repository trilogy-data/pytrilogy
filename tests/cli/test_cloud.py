"""Tests for the `trilogy cloud` command: environment resolution, credential
storage, project bundling, response parsing, and every subcommand end to end.

Commands run in-process against the fake API in ``conftest.py``, which replaces
the module's ``urlopen`` — so a command test covers request construction, auth
headers, status handling, model validation, and rendering, rather than only the
command body."""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import ClassVar

import click
import pytest

from trilogy.scripts import cloud as cloud_mod
from trilogy.scripts.cloud import (
    DEFAULT_API_URL,
    DEPLOY_KEYS,
    JOB_ARRAY_KEY,
    JOB_DECLARING_KEYS,
    CloudClient,
    CloudError,
    DeploySettings,
    _find_job,
    _fmt_job,
    _fmt_run,
    _fmt_schedule,
    _fmt_secret,
    _fmt_token,
    _ts,
    apply_rewrites,
    check_bundle_size,
    collect_files,
    forget_token,
    parse_rewrite,
    resolve_api_url,
    resolve_token,
    store_token,
    stored_token,
)
from trilogy.scripts.cloud_models import (
    Job,
    JobRunExt,
    Me,
    ScheduleExt,
    SecretMeta,
    TokenSummary,
)
from trilogy.scripts.source_identity import (
    SOURCE_FINGERPRINT_VERSION,
    SourceOrigin,
    content_digest,
    environment_label,
)

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


class TestTokenResolution:
    URL = "https://api.example"

    @pytest.fixture(autouse=True)
    def _isolate(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cloud_mod, "CREDENTIALS_PATH", tmp_path / "creds.json")
        monkeypatch.delenv(cloud_mod.ENV_TOKEN, raising=False)

    def test_nothing_configured_resolves_to_none(self):
        assert resolve_token(None, self.URL) is None

    def test_env_is_used_when_there_is_no_credentials_file(self, monkeypatch):
        monkeypatch.setenv(cloud_mod.ENV_TOKEN, "tri_env")
        assert resolve_token(None, self.URL) == "tri_env"

    def test_env_beats_stored_credentials(self, monkeypatch):
        store_token(self.URL, "tri_stored", None)
        monkeypatch.setenv(cloud_mod.ENV_TOKEN, "tri_env")
        assert resolve_token(None, self.URL) == "tri_env"

    def test_explicit_flag_beats_env(self, monkeypatch):
        monkeypatch.setenv(cloud_mod.ENV_TOKEN, "tri_env")
        assert resolve_token("tri_flag", self.URL) == "tri_flag"

    def test_an_empty_env_var_counts_as_unset(self, monkeypatch):
        store_token(self.URL, "tri_stored", None)
        monkeypatch.setenv(cloud_mod.ENV_TOKEN, "")
        assert resolve_token(None, self.URL) == "tri_stored"

    def test_the_env_var_is_not_scoped_per_api_url(self, monkeypatch):
        monkeypatch.setenv(cloud_mod.ENV_TOKEN, "tri_env")
        assert resolve_token(None, "https://other.example") == "tri_env"


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
            # Waited for, not asserted outright: the handler sets the event
            # after writing the response, so the browser gets a complete page
            # before browser_login's shutdown() runs. The client can therefore
            # return before the server thread reaches set().
            assert result.event.wait(timeout=5)
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


def _complete_signin(monkeypatch, token: str = "tri_browser", opened: bool = True):
    """Stand in for the browser: read port and nonce out of the URL the CLI is
    about to open, and hit the loopback callback with them."""
    import urllib.request
    from urllib.parse import parse_qs, urlparse

    def fake_open(url: str) -> bool:
        redirect = parse_qs(urlparse(url).query)["redirect_to"][0]
        _, port, nonce = redirect.split(":", 2)
        with urllib.request.urlopen(
            f"http://127.0.0.1:{port}/callback?token={token}&state={nonce}", timeout=5
        ):
            pass
        return opened

    monkeypatch.setattr(cloud_mod.webbrowser, "open", fake_open)


class TestBrowserLogin:
    def test_the_callback_token_is_returned(self, monkeypatch):
        _complete_signin(monkeypatch)
        assert cloud_mod.browser_login("https://api.test") == "tri_browser"

    def test_a_browser_that_will_not_open_still_accepts_a_manual_visit(
        self, monkeypatch, capsys
    ):
        _complete_signin(monkeypatch, opened=False)
        assert cloud_mod.browser_login("https://api.test") == "tri_browser"
        assert "visit the URL above manually" in capsys.readouterr().err

    def test_no_redirect_before_the_deadline_is_an_error(self, monkeypatch):
        monkeypatch.setattr(cloud_mod, "LOGIN_TIMEOUT_SECONDS", 0.05)
        monkeypatch.setattr(cloud_mod.webbrowser, "open", lambda url: True)
        with pytest.raises(CloudError, match="Timed out"):
            cloud_mod.browser_login("https://api.test")

    def test_a_signalled_result_with_no_token_is_an_error(self, monkeypatch):
        class PreSignalled(cloud_mod._LoginResult):
            def __init__(self) -> None:
                super().__init__()
                self.event.set()

        monkeypatch.setattr(cloud_mod, "_LoginResult", PreSignalled)
        monkeypatch.setattr(cloud_mod.webbrowser, "open", lambda url: True)
        with pytest.raises(CloudError, match="without returning a token"):
            cloud_mod.browser_login("https://api.test")


class TestHttpClient:
    """Exercised through the fake transport, so header construction, status
    handling, and body decoding are all real code paths."""

    def test_auth_and_content_headers_are_sent(self, cloud_api):
        CloudClient(cloud_api.url, "tri_abc").post("/auth/tokens", {"name": "ci"})
        call = cloud_api.call_for("POST", "/auth/tokens")
        assert call.headers["authorization"] == "Bearer tri_abc"
        assert call.headers["content-type"] == "application/json"
        assert call.headers["user-agent"] == "trilogy-cli"

    def test_an_unauthenticated_client_sends_no_bearer(self, cloud_api):
        CloudClient(cloud_api.url, None).get("/auth/me")
        assert "authorization" not in cloud_api.call_for("GET", "/auth/me").headers

    def test_a_bodyless_request_declares_no_content_type(self, cloud_api):
        CloudClient(cloud_api.url, "t").get("/auth/me")
        assert "content-type" not in cloud_api.call_for("GET", "/auth/me").headers

    def test_pre_encoded_bytes_are_sent_verbatim(self, cloud_api):
        raw = b'{"name": "spaced   out"}'
        CloudClient(cloud_api.url, "t").post("/orgs/acme/jobs", raw)
        assert cloud_api.call_for("POST", "/orgs/acme/jobs").data == raw

    def test_a_401_points_at_the_login_command(self, cloud_api):
        cloud_api.fail("GET", "/auth/me", 401, "token expired")
        with pytest.raises(CloudError, match=r"trilogy cloud login"):
            CloudClient(cloud_api.url, "t").get("/auth/me")

    def test_a_401_on_an_env_token_points_at_the_env_var(self, cloud_api, monkeypatch):
        monkeypatch.setenv(cloud_mod.ENV_TOKEN, "tri_ci")
        cloud_api.fail("GET", "/auth/me", 401, "token expired")
        with pytest.raises(CloudError, match=r"check \$TRILOGY_CLOUD_TOKEN"):
            CloudClient(cloud_api.url, "tri_ci").get("/auth/me")

    def test_a_401_on_a_flag_token_still_points_at_login(self, cloud_api, monkeypatch):
        monkeypatch.setenv(cloud_mod.ENV_TOKEN, "tri_ci")
        cloud_api.fail("GET", "/auth/me", 401, "token expired")
        with pytest.raises(CloudError, match=r"trilogy cloud login"):
            CloudClient(cloud_api.url, "tri_flag").get("/auth/me")

    def test_an_error_body_is_surfaced(self, cloud_api):
        cloud_api.fail("GET", "/auth/me", 500, "database is on fire")
        with pytest.raises(CloudError, match="database is on fire"):
            CloudClient(cloud_api.url, "t").get("/auth/me")

    def test_an_empty_error_body_falls_back_to_the_reason(self, cloud_api):
        cloud_api.fail("GET", "/auth/me", 503, "")
        with pytest.raises(CloudError, match="503 Error"):
            CloudClient(cloud_api.url, "t").get("/auth/me")

    def test_an_unreachable_host_names_the_api(self, cloud_api):
        cloud_api.offline = True
        with pytest.raises(CloudError, match="Could not reach https://api.test"):
            CloudClient(cloud_api.url, "t").get("/auth/me")

    def test_an_empty_body_decodes_to_none(self, cloud_api):
        cloud_api.set_raw("GET", "/auth/me", b"")
        assert CloudClient(cloud_api.url, "t").get("/auth/me") is None

    def test_a_non_json_body_is_a_clean_error(self, cloud_api):
        cloud_api.set_raw("GET", "/auth/me", b"<html>gateway timeout</html>")
        with pytest.raises(CloudError, match="was not JSON"):
            CloudClient(cloud_api.url, "t").get("/auth/me")

    def test_delete_discards_the_response(self, cloud_api):
        assert CloudClient(cloud_api.url, "t").delete("/auth/tokens/tok-1") is None

    def test_post_one_validates_the_response(self, cloud_api):
        cloud_api.set("POST", "/auth/tokens", {"id": "tok-2"})
        with pytest.raises(CloudError, match="unexpected response"):
            CloudClient(cloud_api.url, "t").post_one(
                "/auth/tokens", Job, {"name": "ci"}
            )


class TestCredentialFileEdgeCases:
    def test_a_corrupt_credentials_file_reads_as_empty(self, tmp_path, monkeypatch):
        path = tmp_path / "creds.json"
        path.write_text("{not json", encoding="utf-8")
        monkeypatch.setattr(cloud_mod, "CREDENTIALS_PATH", path)
        assert stored_token("https://api.test") is None

    def test_a_non_dict_entry_is_ignored(self, tmp_path, monkeypatch):
        path = tmp_path / "creds.json"
        path.write_text(json.dumps({"https://api.test": "bare"}), encoding="utf-8")
        monkeypatch.setattr(cloud_mod, "CREDENTIALS_PATH", path)
        assert stored_token("https://api.test") is None

    def test_a_platform_without_posix_modes_still_stores(self, tmp_path, monkeypatch):
        path = tmp_path / "creds.json"
        monkeypatch.setattr(cloud_mod, "CREDENTIALS_PATH", path)
        monkeypatch.setattr(
            Path, "chmod", lambda *a, **k: (_ for _ in ()).throw(OSError("no modes"))
        )
        store_token("https://api.test", "tri_abc", None)
        assert stored_token("https://api.test") == "tri_abc"


class TestBundlingEdgeCases:
    def test_a_non_utf8_file_is_skipped_with_a_warning(self, tmp_path, capsys):
        (tmp_path / "keep.preql").write_text("x", encoding="utf-8")
        (tmp_path / "latin.preql").write_bytes(b"\xff\xfe not utf-8")
        names = [f["name"] for f in collect_files(tmp_path)]
        assert names == ["keep.preql"]
        assert "skip (not utf-8)" in capsys.readouterr().err

    def test_files_outside_the_include_globs_are_dropped(self, tmp_path):
        (tmp_path / "keep.preql").write_text("x", encoding="utf-8")
        (tmp_path / "notes.md").write_text("x", encoding="utf-8")
        assert [f["name"] for f in collect_files(tmp_path)] == ["keep.preql"]


class TestFormatters:
    def test_missing_timestamps_render_as_the_fallback(self):
        assert _ts(None) == "-"
        assert _ts(None, "never") == "never"
        assert _ts(datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)).startswith(
            "2026-07-28 12:00:00"
        )

    def test_a_non_expiring_token_says_never(self):
        row = TokenSummary(
            id="tok-1", name="laptop", token_prefix="tri_abc", created_at=TS
        )
        line = _fmt_token(row)
        assert "expires: never" in line and "last used: never" in line

    def test_a_job_without_a_timeout_says_default(self):
        line = _fmt_job(
            Job(
                id="j1",
                org_id="o1",
                name="nightly",
                operation="run",
                created_at=TS,
                updated_at=TS,
            )
        )
        assert "timeout: default" in line

    def test_a_run_line_carries_status_and_timestamps(self):
        line = _fmt_run(
            JobRunExt(
                id="r1",
                job_id="j1",
                job_name="nightly",
                status="failed",
                created_at=TS,
            )
        )
        assert "failed" in line and "finished: -" in line

    def test_a_paused_schedule_with_no_jobs_renders_placeholders(self):
        line = _fmt_schedule(
            ScheduleExt(
                id="s1",
                org_id="o1",
                name="n",
                cron_expr="0 3 * * *",
                is_active=False,
                next_run_at=TS,
                created_at=TS,
                updated_at=TS,
            )
        )
        assert "paused" in line and "jobs: -" in line

    def test_a_secret_line_never_carries_a_value(self):
        line = _fmt_secret(SecretMeta(name="PGPASSWORD", created_at=TS, updated_at=TS))
        assert line.startswith("PGPASSWORD") and "updated:" in line


class TestAuthCommands:
    def test_login_stores_the_token_from_a_browser_signin(
        self, cloud_api, run_cloud, monkeypatch
    ):
        _complete_signin(monkeypatch, token="tri_from_browser")
        result = run_cloud("login")
        assert result.exit_code == 0
        assert "dev@example.com" in result.output
        assert stored_token(cloud_api.url) == "tri_from_browser"

    def test_login_accepts_a_pre_issued_token(self, cloud_api, run_cloud):
        result = run_cloud("login", "--token", "tri_preissued")
        assert result.exit_code == 0
        assert stored_token(cloud_api.url) == "tri_preissued"
        assert cloud_api.call_for("GET", "/auth/me").headers["authorization"] == (
            "Bearer tri_preissued"
        )

    def test_a_group_level_token_is_used_for_login_too(self, cloud_api, run_cloud):
        assert run_cloud("--token", "tri_group", "login").exit_code == 0
        assert stored_token(cloud_api.url) == "tri_group"

    def test_login_emits_a_json_event(self, cloud_api, run_cloud, json_mode):
        result = run_cloud("login", "--token", "tri_x")
        assert json.loads(result.output)["event"] == "login"

    def test_login_reports_a_user_with_no_orgs(self, cloud_api, run_cloud):
        cloud_api.set(
            "GET",
            "/auth/me",
            {"id": "u1", "email": "new@example.com", "provider": "google", "orgs": []},
        )
        assert "orgs: none" in run_cloud("login", "--token", "tri_x").output

    def test_logout_forgets_a_stored_token(self, logged_in, run_cloud):
        assert "Logged out" in run_cloud("logout").output
        assert stored_token(logged_in.url) is None

    def test_logout_without_credentials_says_so(self, cloud_api, run_cloud):
        assert "No stored credentials" in run_cloud("logout").output

    def test_whoami_lists_the_user_and_orgs(self, logged_in, run_cloud):
        output = run_cloud("whoami").output
        assert "dev@example.com" in output and "acme (admin)" in output

    def test_whoami_falls_back_when_the_profile_has_no_name(self, cloud_api, run_cloud):
        cloud_api.set(
            "GET",
            "/auth/me",
            {"id": "u1", "email": "a@b.co", "provider": "google", "orgs": []},
        )
        assert "no name" in run_cloud("--token", "t", "whoami").output

    def test_whoami_emits_a_json_event(self, logged_in, run_cloud, json_mode):
        assert json.loads(run_cloud("whoami").output)["email"] == "dev@example.com"

    def test_an_unauthenticated_command_names_the_fix(self, cloud_api, run_cloud):
        result = run_cloud("whoami")
        assert result.exit_code != 0
        assert "Not logged in to https://api.test" in result.output
        assert "TRILOGY_CLOUD_TOKEN" in result.output


class TestRunWait:
    """``--wait`` is the CI contract: block, then exit non-zero unless the run
    actually succeeded."""

    ROUTE: ClassVar[str] = "/orgs/acme/jobs/runs/*"
    PENDING: ClassVar[tuple[str, ...]] = ("queued", "dispatched", "claimed", "running")

    def _progress(self, cloud_api, *statuses: str, run_id: str = "run-1") -> None:
        cloud_api.set_steps(
            "GET",
            self.ROUTE,
            [
                {
                    "id": run_id,
                    "job_id": "job-1",
                    "job_name": "nightly",
                    "created_at": TS,
                    "status": status,
                    "finished_at": None if status in self.PENDING else TS,
                    "exit_code": None if status in self.PENDING else 0,
                }
                for status in statuses
            ],
        )

    def test_it_returns_once_the_run_finishes(self, logged_in, run_cloud):
        self._progress(logged_in, "queued", "running", "completed")
        result = run_cloud("runs", "wait", "run-1", "--poll-seconds", "0")
        assert result.exit_code == 0
        assert "completed" in result.output
        assert len(logged_in.requests_for("GET", "/orgs/acme/jobs/runs/run-1")) == 3

    def test_an_already_finished_run_is_not_polled_again(self, logged_in, run_cloud):
        self._progress(logged_in, "completed")
        assert run_cloud("runs", "wait", "run-1", "--poll-seconds", "0").exit_code == 0
        assert len(logged_in.requests_for("GET", "/orgs/acme/jobs/runs/run-1")) == 1

    def test_a_failed_run_exits_non_zero(self, logged_in, run_cloud):
        self._progress(logged_in, "running", "failed")
        result = run_cloud("runs", "wait", "run-1", "--poll-seconds", "0")
        assert result.exit_code != 0
        assert "failed" in result.output and "run-1" in result.output

    def test_a_terminal_status_without_a_timestamp_still_ends_the_wait(
        self, logged_in, run_cloud
    ):
        logged_in.set(
            "GET",
            self.ROUTE,
            {
                "id": "run-1",
                "job_id": "job-1",
                "job_name": "nightly",
                "created_at": TS,
                "status": "cancelled",
                "finished_at": None,
            },
        )
        result = run_cloud("runs", "wait", "run-1", "--poll-seconds", "0")
        assert result.exit_code != 0
        assert "cancelled" in result.output

    def test_a_timeout_names_the_status_it_was_stuck_in(self, logged_in, run_cloud):
        self._progress(logged_in, "queued")
        result = run_cloud(
            "runs", "wait", "run-1", "--timeout", "0", "--poll-seconds", "0"
        )
        assert result.exit_code != 0
        assert "queued" in result.output and "not cancelled" in result.output

    def test_jobs_run_wait_polls_the_triggered_run(self, logged_in, run_cloud):
        self._progress(logged_in, "running", "completed", run_id="run-new")
        result = run_cloud("jobs", "run", "nightly", "--wait", "--poll-seconds", "0")
        assert result.exit_code == 0
        assert "Triggered run run-new" in result.output
        assert logged_in.requests_for("GET", "/orgs/acme/jobs/runs/run-new")

    def test_jobs_run_without_wait_never_polls(self, logged_in, run_cloud):
        assert run_cloud("jobs", "run", "nightly").exit_code == 0
        assert not logged_in.requests_for("GET", "/orgs/acme/jobs/runs/run-new")

    def test_jobs_run_wait_exits_non_zero_on_a_failed_run(self, logged_in, run_cloud):
        self._progress(logged_in, "failed")
        result = run_cloud("jobs", "run", "nightly", "--wait", "--poll-seconds", "0")
        assert result.exit_code != 0

    def test_a_failure_prints_the_log_tail_unasked(self, logged_in, run_cloud):
        logged_in.set(
            "GET",
            self.ROUTE,
            {
                "id": "run-1",
                "job_id": "job-1",
                "job_name": "nightly",
                "created_at": TS,
                "status": "failed",
                "finished_at": TS,
                "exit_code": 1,
                "stdout": "loading rows\n",
                "stderr": "boom: table not found\n",
            },
        )
        out = run_cloud("runs", "wait", "run-1", "--poll-seconds", "0").output
        assert "boom: table not found" in out
        assert "loading rows" in out

    def test_a_success_stays_quiet_until_logs_is_asked_for(self, logged_in, run_cloud):
        payload = {
            "id": "run-1",
            "job_id": "job-1",
            "job_name": "nightly",
            "created_at": TS,
            "status": "completed",
            "finished_at": TS,
            "exit_code": 0,
            "stdout": "1 row(s).\n",
        }
        logged_in.set("GET", self.ROUTE, payload)
        quiet = run_cloud("runs", "wait", "run-1", "--poll-seconds", "0").output
        assert "1 row(s)." not in quiet
        loud = run_cloud(
            "runs", "wait", "run-1", "--poll-seconds", "0", "--logs"
        ).output
        assert "1 row(s)." in loud

    def test_the_finished_run_is_a_json_event(self, logged_in, run_cloud, json_mode):
        self._progress(logged_in, "completed")
        result = run_cloud("runs", "wait", "run-1", "--poll-seconds", "0")
        event = json.loads(result.output)
        assert event["event"] == "run_finished"
        assert event["run"]["status"] == "completed"


class TestTokenEnvVarEndToEnd:
    def test_a_ci_runner_authenticates_with_no_credentials_file(
        self, cloud_api, run_cloud, monkeypatch
    ):
        monkeypatch.setenv(cloud_mod.ENV_TOKEN, "tri_ci")
        assert not cloud_mod.CREDENTIALS_PATH.exists()
        result = run_cloud("whoami")
        assert result.exit_code == 0
        assert cloud_api.call_for("GET", "/auth/me").headers["authorization"] == (
            "Bearer tri_ci"
        )

    def test_the_flag_still_beats_the_env_var(self, cloud_api, run_cloud, monkeypatch):
        monkeypatch.setenv(cloud_mod.ENV_TOKEN, "tri_ci")
        assert run_cloud("--token", "tri_flag", "whoami").exit_code == 0
        assert cloud_api.call_for("GET", "/auth/me").headers["authorization"] == (
            "Bearer tri_flag"
        )

    def test_the_env_var_beats_stored_credentials(
        self, logged_in, run_cloud, monkeypatch
    ):
        monkeypatch.setenv(cloud_mod.ENV_TOKEN, "tri_ci")
        assert run_cloud("whoami").exit_code == 0
        assert logged_in.call_for("GET", "/auth/me").headers["authorization"] == (
            "Bearer tri_ci"
        )

    def test_login_warns_that_the_env_var_shadows_what_it_stored(
        self, cloud_api, run_cloud, monkeypatch
    ):
        monkeypatch.setenv(cloud_mod.ENV_TOKEN, "tri_ci")
        result = run_cloud("login", "--token", "tri_mine")
        assert result.exit_code == 0
        assert stored_token(cloud_api.url) == "tri_mine"
        assert "takes precedence" in result.output

    def test_login_with_the_same_token_does_not_warn(
        self, cloud_api, run_cloud, monkeypatch
    ):
        monkeypatch.setenv(cloud_mod.ENV_TOKEN, "tri_same")
        result = run_cloud("login", "--token", "tri_same")
        assert "takes precedence" not in result.output

    def test_logout_warns_that_the_env_var_still_authenticates(
        self, logged_in, run_cloud, monkeypatch
    ):
        monkeypatch.setenv(cloud_mod.ENV_TOKEN, "tri_ci")
        result = run_cloud("logout")
        assert "Logged out" in result.output
        assert "still set" in result.output


class TestTokenCommands:
    def test_list_renders_a_row_per_token(self, logged_in, run_cloud):
        assert "tri_abc" in run_cloud("tokens", "list").output

    def test_list_has_a_dedicated_empty_message(self, logged_in, run_cloud):
        logged_in.set("GET", "/auth/tokens", [])
        assert "No API tokens." in run_cloud("tokens", "list").output

    def test_list_emits_a_json_event(self, logged_in, run_cloud, json_mode):
        payload = json.loads(run_cloud("tokens", "list").output)
        assert payload["event"] == "tokens" and payload["tokens"][0]["id"] == "tok-1"

    def test_create_prints_the_value_once(self, logged_in, run_cloud):
        output = run_cloud("tokens", "create", "ci").output
        assert "tri_secret_value" in output
        assert logged_in.body_for("POST", "/auth/tokens") == {"name": "ci"}

    def test_create_forwards_an_expiry(self, logged_in, run_cloud):
        run_cloud("tokens", "create", "ci", "--expires-in-days", "30")
        assert logged_in.body_for("POST", "/auth/tokens")["expires_in_days"] == 30

    def test_create_emits_a_json_event(self, logged_in, run_cloud, json_mode):
        payload = json.loads(run_cloud("tokens", "create", "ci").output)
        assert payload["event"] == "token_created"
        assert payload["token"] == "tri_secret_value"

    def test_revoke_deletes_by_id(self, logged_in, run_cloud):
        assert "Revoked token tok-1" in run_cloud("tokens", "revoke", "tok-1").output
        assert logged_in.call_for("DELETE", "/auth/tokens/tok-1")


class TestOrgResolution:
    def test_orgs_lists_memberships(self, logged_in, run_cloud):
        assert "acme" in run_cloud("orgs").output

    def test_orgs_has_a_dedicated_empty_message(self, logged_in, run_cloud):
        logged_in.set("GET", "/orgs", [])
        assert "not a member of any org" in run_cloud("orgs").output

    def test_a_sole_membership_is_used_implicitly(self, logged_in, run_cloud):
        assert run_cloud("jobs", "list").exit_code == 0
        assert logged_in.call_for("GET", "/orgs/acme/jobs")

    def test_the_org_flag_skips_the_lookup(self, logged_in, run_cloud):
        logged_in.set("GET", "/orgs/other/jobs", [])
        assert run_cloud("--org", "other", "jobs", "list").exit_code == 0
        assert not any(c.path == "/auth/me" for c in logged_in.calls)

    def test_the_config_org_is_used_when_no_flag_is_given(self, logged_in, run_cloud):
        Path("trilogy.toml").write_text('[cloud]\norg = "conf"\n', encoding="utf-8")
        cloud_mod._cloud_table.cache_clear()
        logged_in.set("GET", "/orgs/conf/jobs", [])
        assert run_cloud("jobs", "list").exit_code == 0
        assert logged_in.call_for("GET", "/orgs/conf/jobs")

    def test_no_memberships_is_an_error(self, logged_in, run_cloud):
        logged_in.set(
            "GET",
            "/auth/me",
            {"id": "u1", "email": "a@b.co", "provider": "google", "orgs": []},
        )
        result = run_cloud("jobs", "list")
        assert result.exit_code != 0
        assert "not a member of any org" in result.output

    def test_several_memberships_require_the_flag(self, logged_in, run_cloud):
        me = dict(logged_in.routes[("GET", "/auth/me")])
        me["orgs"] = [
            {**me["orgs"][0], "slug": "acme"},
            {**me["orgs"][0], "id": "org-b", "slug": "beta"},
        ]
        logged_in.set("GET", "/auth/me", me)
        result = run_cloud("jobs", "list")
        assert result.exit_code != 0
        assert "Multiple orgs (acme, beta)" in result.output


class TestJobCommands:
    def _project(self, tmp_path: Path) -> Path:
        source = tmp_path / "project"
        (source / "nested").mkdir(parents=True)
        (source / "trilogy.toml").write_text("[trilogy]\n", encoding="utf-8")
        (source / "model.preql").write_text(
            "datasource x (id:id) address gs://old/x;", encoding="utf-8"
        )
        (source / "nested" / "helper.py").write_text("URL = 'gs://old/y'", "utf-8")
        return source

    def test_list_renders_a_row_per_job(self, logged_in, run_cloud):
        assert "nightly" in run_cloud("jobs", "list").output

    def test_list_has_a_dedicated_empty_message(self, logged_in, run_cloud):
        logged_in.set("GET", f"/orgs/{logged_in.org}/jobs", [])
        assert "No jobs in org 'acme'." in run_cloud("jobs", "list").output

    def test_list_emits_a_json_event_tagged_with_the_org(
        self, logged_in, run_cloud, json_mode
    ):
        payload = json.loads(run_cloud("jobs", "list").output)
        assert payload["event"] == "jobs" and payload["org"] == "acme"

    def test_push_bundles_the_directory_and_sends_what_it_measured(
        self, logged_in, run_cloud, tmp_path
    ):
        source = self._project(tmp_path)
        result = run_cloud("jobs", "push", "--source", str(source), "--name", "fresh")
        assert result.exit_code == 0, result.output
        call = logged_in.call_for("POST", f"/orgs/{logged_in.org}/jobs")
        names = [f["name"] for f in call.body["files"]]
        assert names == ["model.preql", "nested/helper.py"]
        assert call.data == json.dumps(call.body).encode("utf-8")
        assert call.body["config"] == "[trilogy]\n"
        assert "Created job 'fresh'" in result.output

    def test_push_forwards_the_optional_job_settings(
        self, logged_in, run_cloud, tmp_path
    ):
        source = self._project(tmp_path)
        run_cloud(
            "jobs",
            "push",
            "--source",
            str(source),
            "--name",
            "fresh",
            "--description",
            "nightly refresh",
            "--operation",
            "refresh",
            "--timeout-seconds",
            "900",
            "--memory-mb",
            "2048",
            "--cpus",
            "1.5",
            "--secret-env",
            "PGPASSWORD",
        )
        body = logged_in.body_for("POST", f"/orgs/{logged_in.org}/jobs")
        assert body["description"] == "nightly refresh"
        assert body["operation"] == "refresh"
        assert body["timeout_seconds"] == 900
        assert body["memory_mb"] == 2048
        assert body["cpus"] == 1.5
        assert body["secret_env"] == ["PGPASSWORD"]

    def test_push_applies_rewrites_to_contents_and_the_config(
        self, logged_in, run_cloud, tmp_path
    ):
        source = self._project(tmp_path)
        (source / "trilogy.toml").write_text("root = 'gs://old'\n", encoding="utf-8")
        result = run_cloud(
            "jobs",
            "push",
            "--source",
            str(source),
            "--name",
            "fresh",
            "--rewrite",
            "gs://old=gs://new",
        )
        body = logged_in.body_for("POST", f"/orgs/{logged_in.org}/jobs")
        assert body["config"] == "root = 'gs://new'\n"
        assert all("gs://new" in f["content"] for f in body["files"])
        assert "Applied rewrites to 2 file(s)" in result.output

    def test_a_rewrite_glob_narrows_which_files_are_touched(
        self, logged_in, run_cloud, tmp_path
    ):
        source = self._project(tmp_path)
        run_cloud(
            "jobs",
            "push",
            "--source",
            str(source),
            "--name",
            "fresh",
            "--rewrite",
            "gs://old=gs://new",
            "--rewrite-glob",
            "*.preql",
        )
        files = {
            f["name"]: f["content"]
            for f in logged_in.body_for("POST", f"/orgs/{logged_in.org}/jobs")["files"]
        }
        assert "gs://new" in files["model.preql"]
        assert "gs://old" in files["nested/helper.py"]

    def test_push_honours_a_custom_include(self, logged_in, run_cloud, tmp_path):
        source = self._project(tmp_path)
        run_cloud(
            "jobs",
            "push",
            "--source",
            str(source),
            "--name",
            "fresh",
            "--include",
            "*.preql",
        )
        body = logged_in.body_for("POST", f"/orgs/{logged_in.org}/jobs")
        assert [f["name"] for f in body["files"]] == ["model.preql"]

    def test_push_honours_an_extra_exclude(self, logged_in, run_cloud, tmp_path):
        source = self._project(tmp_path)
        run_cloud(
            "jobs",
            "push",
            "--source",
            str(source),
            "--name",
            "fresh",
            "--exclude",
            "nested/*",
        )
        body = logged_in.body_for("POST", f"/orgs/{logged_in.org}/jobs")
        assert [f["name"] for f in body["files"]] == ["model.preql"]

    def test_push_accepts_a_config_outside_the_source(
        self, logged_in, run_cloud, tmp_path
    ):
        source = self._project(tmp_path)
        (source / "trilogy.toml").unlink()
        external = tmp_path / "other.toml"
        external.write_text("[trilogy]\nfrom = 'outside'\n", encoding="utf-8")
        result = run_cloud(
            "jobs",
            "push",
            "--source",
            str(source),
            "--config",
            str(external),
            "--name",
            "fresh",
        )
        assert result.exit_code == 0, result.output
        body = logged_in.body_for("POST", f"/orgs/{logged_in.org}/jobs")
        assert "outside" in body["config"]

    def test_push_without_a_config_is_refused(self, logged_in, run_cloud, tmp_path):
        source = self._project(tmp_path)
        (source / "trilogy.toml").unlink()
        result = run_cloud("jobs", "push", "--source", str(source), "--name", "fresh")
        assert result.exit_code != 0
        assert "pass --config" in result.output

    def test_push_with_no_matching_files_is_refused(
        self, logged_in, run_cloud, tmp_path
    ):
        source = tmp_path / "docs_only"
        source.mkdir()
        (source / "README.md").write_text("nothing to bundle", encoding="utf-8")
        config = tmp_path / "trilogy.toml"
        config.write_text("[trilogy]\n", encoding="utf-8")
        result = run_cloud(
            "jobs",
            "push",
            "--source",
            str(source),
            "--config",
            str(config),
            "--name",
            "fresh",
        )
        assert result.exit_code != 0
        assert "No files matched" in result.output

    def test_push_updates_an_existing_job_in_place(
        self, logged_in, run_cloud, tmp_path
    ):
        """The whole point of the upsert: a POST here would mint a second
        'nightly' and leave every schedule bound to the first."""
        source = self._project(tmp_path)
        result = run_cloud("jobs", "push", "--source", str(source), "--name", "nightly")
        assert result.exit_code == 0, result.output
        put = logged_in.call_for("PUT", f"/orgs/{logged_in.org}/jobs/job-1")
        assert put.body["config"] == "[trilogy]\n"
        assert not any(
            c.method == "POST" and c.path == f"/orgs/{logged_in.org}/jobs"
            for c in logged_in.calls
        )
        assert "Updated job 'nightly'" in result.output

    def test_push_keeps_settings_it_was_not_told_to_change(
        self, logged_in, run_cloud, tmp_path
    ):
        """PUT clears what it omits, so a push that only ships an edited file
        must carry the job's operation, timeouts and secrets back with it."""
        source = self._project(tmp_path)
        configured = {
            **logged_in.routes[("GET", f"/orgs/{logged_in.org}/jobs")][0],
            "operation": "refresh",
            "timeout_seconds": 14400,
            "secret_env": ["GOATCOUNTER_API_TOKEN", "GOOGLE_HMAC_KEY"],
            "parameters": {"site": "example"},
            "vm_class": "shared",
        }
        logged_in.set("GET", f"/orgs/{logged_in.org}/jobs", [configured])
        result = run_cloud("jobs", "push", "--source", str(source), "--name", "nightly")
        assert result.exit_code == 0, result.output
        body = logged_in.body_for("PUT", f"/orgs/{logged_in.org}/jobs/job-1")
        assert body["operation"] == "refresh"
        assert body["timeout_seconds"] == 14400
        assert body["secret_env"] == ["GOATCOUNTER_API_TOKEN", "GOOGLE_HMAC_KEY"]
        assert body["parameters"] == {"site": "example"}
        assert body["vm_class"] == "shared"

    def test_push_flags_override_what_the_job_holds(
        self, logged_in, run_cloud, tmp_path
    ):
        source = self._project(tmp_path)
        configured = {
            **logged_in.routes[("GET", f"/orgs/{logged_in.org}/jobs")][0],
            "operation": "refresh",
            "timeout_seconds": 14400,
        }
        logged_in.set("GET", f"/orgs/{logged_in.org}/jobs", [configured])
        run_cloud(
            "jobs",
            "push",
            "--source",
            str(source),
            "--name",
            "nightly",
            "--operation",
            "run",
            "--timeout-seconds",
            "60",
        )
        body = logged_in.body_for("PUT", f"/orgs/{logged_in.org}/jobs/job-1")
        assert body["operation"] == "run" and body["timeout_seconds"] == 60

    def test_a_new_job_gets_the_platform_default_operation(
        self, logged_in, run_cloud, tmp_path
    ):
        source = self._project(tmp_path)
        run_cloud("jobs", "push", "--source", str(source), "--name", "fresh")
        body = logged_in.body_for("POST", f"/orgs/{logged_in.org}/jobs")
        assert body["operation"] == "run"
        assert "timeout_seconds" not in body and "secret_env" not in body

    def test_push_reports_matching_content_as_a_no_op(
        self, logged_in, run_cloud, tmp_path
    ):
        source = self._project(tmp_path)
        # A PUT whose content matched mints nothing, so the job comes back on
        # the version it already held.
        unmoved = logged_in.routes[("GET", f"/orgs/{logged_in.org}/jobs")][0]
        logged_in.set("PUT", f"/orgs/{logged_in.org}/jobs/*", unmoved)
        result = run_cloud("jobs", "push", "--source", str(source), "--name", "nightly")
        assert "already matches this content" in result.output

    def test_push_refuses_to_guess_between_duplicate_names(
        self, logged_in, run_cloud, tmp_path
    ):
        source = self._project(tmp_path)
        listed = logged_in.routes[("GET", f"/orgs/{logged_in.org}/jobs")]
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/jobs",
            [listed[0], {**listed[0], "id": "job-2"}],
        )
        result = run_cloud("jobs", "push", "--source", str(source), "--name", "nightly")
        assert result.exit_code != 0
        assert "2 jobs in 'acme' are named 'nightly'" in result.output
        assert "job-1, job-2" in result.output

    def test_push_create_forces_a_second_job(self, logged_in, run_cloud, tmp_path):
        source = self._project(tmp_path)
        result = run_cloud(
            "jobs", "push", "--source", str(source), "--name", "nightly", "--create"
        )
        assert result.exit_code == 0, result.output
        assert logged_in.call_for("POST", f"/orgs/{logged_in.org}/jobs")

    def test_push_create_still_warns_that_the_name_is_taken(
        self, logged_in, run_cloud, tmp_path
    ):
        """Creating the duplicate is the point of the flag; that the org's
        schedules keep naming the *other* job is the part worth saying."""
        source = self._project(tmp_path)
        result = run_cloud(
            "jobs", "push", "--source", str(source), "--name", "nightly", "--create"
        )
        assert "already named 'nightly'" in result.output

    def test_push_create_is_silent_for_an_unused_name(
        self, logged_in, run_cloud, tmp_path
    ):
        source = self._project(tmp_path)
        result = run_cloud(
            "jobs", "push", "--source", str(source), "--name", "fresh", "--create"
        )
        assert "already named" not in result.output

    def test_push_carries_a_source_fingerprint(self, logged_in, run_cloud, tmp_path):
        source = self._project(tmp_path)
        run_cloud("jobs", "push", "--source", str(source), "--name", "fresh")
        sent = logged_in.body_for("POST", f"/orgs/{logged_in.org}/jobs")
        fingerprint = sent["source_fingerprint"]
        assert fingerprint["version"] == SOURCE_FINGERPRINT_VERSION
        assert fingerprint["content"] == content_digest(
            "[trilogy]\n",
            [f for f in sent["files"]],
        )
        # tmp_path is not a repository, so the origin is the opaque local
        # token — never the absolute path it was built from.
        assert fingerprint["origin_kind"] == "path"
        assert fingerprint["origin"].startswith("local:project-")
        assert str(source) not in json.dumps(sent)

    def test_the_fingerprint_follows_rewrites_not_the_files_on_disk(
        self, logged_in, run_cloud, tmp_path
    ):
        source = self._project(tmp_path)
        plain = run_cloud("jobs", "push", "--source", str(source), "--name", "fresh")
        before = logged_in.body_for("POST", f"/orgs/{logged_in.org}/jobs")
        rewritten = run_cloud(
            "jobs",
            "push",
            "--source",
            str(source),
            "--name",
            "fresh",
            "--rewrite",
            "gs://old=gs://new",
        )
        after = logged_in.body_for("POST", f"/orgs/{logged_in.org}/jobs")
        assert plain.exit_code == 0 and rewritten.exit_code == 0
        assert (
            before["source_fingerprint"]["content"]
            != after["source_fingerprint"]["content"]
        )

    def test_push_with_a_cron_also_creates_a_schedule(
        self, logged_in, run_cloud, tmp_path
    ):
        source = self._project(tmp_path)
        result = run_cloud(
            "jobs",
            "push",
            "--source",
            str(source),
            "--name",
            "fresh",
            "--cron",
            "0 3 * * *",
        )
        body = logged_in.body_for("POST", f"/orgs/{logged_in.org}/schedules")
        assert body == {
            "name": "fresh-schedule",
            "cron_expr": "0 3 * * *",
            "job_ids": ["job-new"],
        }
        assert "Scheduled 'nightly-schedule'" in result.output

    def test_push_emits_json_events(self, logged_in, run_cloud, tmp_path, json_mode):
        source = self._project(tmp_path)
        result = run_cloud(
            "jobs",
            "push",
            "--source",
            str(source),
            "--name",
            "fresh",
            "--cron",
            "0 3 * * *",
        )
        events = [obj["event"] for obj in _json_stream(result.output)]
        assert events[-2:] == ["job_created", "schedule_created"]

    def test_versions_lists_history_newest_first(self, logged_in, run_cloud):
        output = run_cloud("jobs", "versions", "nightly").output
        assert output.index("v2") < output.index("v1")
        assert logged_in.call_for("GET", f"/orgs/{logged_in.org}/jobs/job-1/versions")

    def test_versions_marks_the_one_the_job_currently_holds(self, logged_in, run_cloud):
        lines = run_cloud("jobs", "versions", "nightly").output.splitlines()
        current = [line for line in lines if "(current)" in line]
        assert len(current) == 1 and current[0].startswith("v1")

    def test_versions_shows_recorded_push_provenance(self, logged_in, run_cloud):
        output = run_cloud("jobs", "versions", "nightly").output
        assert "github.com/acme/models" in output

    def test_versions_says_so_when_a_job_predates_versioning(
        self, logged_in, run_cloud
    ):
        logged_in.set("GET", f"/orgs/{logged_in.org}/jobs/*/versions", [])
        assert "predates versioning" in run_cloud("jobs", "versions", "nightly").output

    def test_delete_removes_a_job_by_name(self, logged_in, run_cloud):
        logged_in.set("DELETE", f"/orgs/{logged_in.org}/jobs/*", {})
        logged_in.set("GET", f"/orgs/{logged_in.org}/schedules", [])
        result = run_cloud("jobs", "delete", "nightly", "--yes")
        assert result.exit_code == 0, result.output
        assert logged_in.requests_for("DELETE", f"/orgs/{logged_in.org}/jobs/job-1")

    def test_delete_confirms_first(self, logged_in, run_cloud):
        logged_in.set("DELETE", f"/orgs/{logged_in.org}/jobs/*", {})
        result = run_cloud("jobs", "delete", "nightly", input="n\n")
        assert result.exit_code != 0
        assert not logged_in.requests_for("DELETE", f"/orgs/{logged_in.org}/jobs/job-1")

    def test_delete_takes_several_and_deletes_each_once(self, logged_in, run_cloud):
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/jobs",
            [_job_payload("job-1", "nightly"), _job_payload("job-2", "publish")],
        )
        logged_in.set("DELETE", f"/orgs/{logged_in.org}/jobs/*", {})
        logged_in.set("GET", f"/orgs/{logged_in.org}/schedules", [])
        result = run_cloud("jobs", "delete", "nightly", "job-1", "publish", "--yes")
        assert result.exit_code == 0, result.output
        assert (
            len(logged_in.requests_for("DELETE", f"/orgs/{logged_in.org}/jobs/job-1"))
            == 1
        )
        assert logged_in.requests_for("DELETE", f"/orgs/{logged_in.org}/jobs/job-2")

    def test_delete_of_an_ambiguous_name_names_the_ids(self, logged_in, run_cloud):
        """A name cannot address either of them."""
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/jobs",
            [_job_payload("job-1", "nightly"), _job_payload("job-2", "nightly")],
        )
        result = run_cloud("jobs", "delete", "nightly", "--yes")
        assert result.exit_code != 0
        assert "job-1" in result.output and "job-2" in result.output
        assert not logged_in.requests_for("DELETE", f"/orgs/{logged_in.org}/jobs/job-1")

    def test_delete_removes_a_schedule_left_bound_to_nothing(
        self, logged_in, run_cloud
    ):
        logged_in.set("DELETE", f"/orgs/{logged_in.org}/jobs/*", {})
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/schedules",
            [_schedule_payload("nightly-schedule", "0 3 * * *", job_ids=["job-1"])],
        )
        result = run_cloud("jobs", "delete", "nightly", "--yes")
        assert result.exit_code == 0, result.output
        assert logged_in.requests_for(
            "DELETE", f"/orgs/{logged_in.org}/schedules/sched-1"
        )

    def test_delete_leaves_a_schedule_that_still_binds_a_live_job(
        self, logged_in, run_cloud
    ):
        logged_in.set("DELETE", f"/orgs/{logged_in.org}/jobs/*", {})
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/schedules",
            [
                _schedule_payload(
                    "nightly-schedule", "0 3 * * *", job_ids=["job-1", "job-2"]
                )
            ],
        )
        assert run_cloud("jobs", "delete", "nightly", "--yes").exit_code == 0
        assert not logged_in.requests_for(
            "DELETE", f"/orgs/{logged_in.org}/schedules/sched-1"
        )

    def test_run_triggers_by_name(self, logged_in, run_cloud):
        result = run_cloud("jobs", "run", "nightly")
        assert "Triggered run run-new" in result.output
        assert logged_in.call_for("POST", f"/orgs/{logged_in.org}/jobs/job-1/run")

    def test_run_rejects_an_unknown_job_before_calling_the_api(
        self, logged_in, run_cloud
    ):
        result = run_cloud("jobs", "run", "missing")
        assert result.exit_code != 0
        assert "No job named 'missing'" in result.output

    def test_run_emits_a_json_event(self, logged_in, run_cloud, json_mode):
        payload = json.loads(run_cloud("jobs", "run", "nightly").output)
        assert payload["event"] == "run_triggered"


def _json_stream(output: str) -> list:
    """Successive events are newline-separated JSON objects, not JSON lines."""
    decoder = json.JSONDecoder()
    objects, index = [], 0
    while index < len(output):
        if output[index].isspace():
            index += 1
            continue
        obj, index = decoder.raw_decode(output, index)
        objects.append(obj)
    return objects


class TestRunCommands:
    def test_list_renders_recent_runs(self, logged_in, run_cloud):
        output = run_cloud("runs", "list").output
        assert "run-1" in output and "run-2" in output

    def test_list_filters_server_side(self, logged_in, run_cloud):
        """A newest-N window filtered after the fact shows nothing a burst of
        successes has already pushed out of it."""
        run_cloud("runs", "list", "--limit", "1", "--status", "failed")
        call = logged_in.call_for("GET", f"/orgs/{logged_in.org}/jobs/runs")
        assert call.query == {"limit": ["1"], "status": ["failed"]}

    def test_list_clamps_a_limit_past_the_server_cap(self, logged_in, run_cloud):
        run_cloud("runs", "list", "--limit", "5000")
        call = logged_in.call_for("GET", f"/orgs/{logged_in.org}/jobs/runs")
        assert call.query["limit"] == [str(cloud_mod.RUNS_MAX_LIMIT)]

    def test_list_forwards_the_source_filter(self, logged_in, run_cloud):
        run_cloud("runs", "list", "--source", "scheduled")
        call = logged_in.call_for("GET", f"/orgs/{logged_in.org}/jobs/runs")
        assert call.query["source"] == ["scheduled"]

    def test_list_has_a_dedicated_empty_message(self, logged_in, run_cloud):
        logged_in.set("GET", f"/orgs/{logged_in.org}/jobs/runs", [])
        assert "No runs in org 'acme'." in run_cloud("runs", "list").output

    def test_an_empty_filtered_list_says_what_it_filtered_on(
        self, logged_in, run_cloud
    ):
        """'no runs at all' and 'none that match' are different answers, and
        only one of them should worry anyone."""
        logged_in.set("GET", f"/orgs/{logged_in.org}/jobs/runs", [])
        output = run_cloud("runs", "list", "--status", "failed").output
        assert "matching status 'failed'" in output

    def test_show_renders_the_timeline_files_and_logs(self, logged_in, run_cloud):
        output = run_cloud("runs", "show", "run-1").output
        assert "Run run-1 of 'nightly': succeeded" in output
        assert "Exit code: 0" in output
        assert "started: worker picked up the run" in output
        assert "succeeded: model.preql" in output
        assert "--- stdout (tail) ---" in output
        assert "hello from the worker" in output
        # stderr was whitespace only — no empty section for it
        assert "stderr" not in output

    def test_show_tails_a_long_log(self, logged_in, run_cloud, monkeypatch):
        monkeypatch.setattr(cloud_mod, "RUN_LOG_TAIL_CHARS", 11)
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/jobs/runs/*",
            {
                "id": "run-1",
                "job_id": "job-1",
                "job_name": "nightly",
                "status": "failed",
                "created_at": TS,
                "stderr": "A" * 50 + "TAIL_MARKER",
            },
        )
        output = run_cloud("runs", "show", "run-1").output
        assert "TAIL_MARKER" in output
        assert "AAAAAAAAAAAAAAAAAAAA" not in output
        assert "Exit code" not in output

    def test_show_emits_a_json_event(self, logged_in, run_cloud, json_mode):
        payload = json.loads(run_cloud("runs", "show", "run-1").output)
        assert payload["event"] == "run" and payload["run"]["id"] == "run-1"


class TestScheduleCommands:
    def test_list_renders_a_row_per_schedule(self, logged_in, run_cloud):
        output = run_cloud("schedules", "list").output
        assert "0 3 * * *" in output and "active" in output and "nightly" in output

    def test_list_has_a_dedicated_empty_message(self, logged_in, run_cloud):
        logged_in.set("GET", f"/orgs/{logged_in.org}/schedules", [])
        assert "No schedules in org 'acme'." in run_cloud("schedules", "list").output

    def test_create_resolves_every_job_reference_from_one_fetch(
        self, logged_in, run_cloud
    ):
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/jobs",
            [
                {
                    "id": "job-1",
                    "org_id": "org-acme",
                    "name": "nightly",
                    "operation": "run",
                    "created_at": TS,
                    "updated_at": TS,
                },
                {
                    "id": "job-2",
                    "org_id": "org-acme",
                    "name": "hourly",
                    "operation": "run",
                    "created_at": TS,
                    "updated_at": TS,
                },
            ],
        )
        result = run_cloud(
            "schedules",
            "create",
            "nightly",
            "job-2",
            "--name",
            "combined",
            "--cron",
            "0 * * * *",
        )
        assert result.exit_code == 0, result.output
        body = logged_in.body_for("POST", f"/orgs/{logged_in.org}/schedules")
        assert body["job_ids"] == ["job-1", "job-2"]
        assert sum(1 for c in logged_in.calls if c.path.endswith("/jobs")) == 1
        assert "Created schedule" in result.output

    def test_create_rejects_an_unknown_job(self, logged_in, run_cloud):
        result = run_cloud(
            "schedules", "create", "missing", "--name", "n", "--cron", "0 * * * *"
        )
        assert result.exit_code != 0
        assert "No job named 'missing'" in result.output

    def test_create_emits_a_json_event(self, logged_in, run_cloud, json_mode):
        payload = json.loads(
            run_cloud(
                "schedules", "create", "nightly", "--name", "n", "--cron", "0 * * * *"
            ).output
        )
        assert payload["event"] == "schedule_created"

    def test_delete_removes_by_id(self, logged_in, run_cloud):
        assert "Deleted schedule sched-1" in (
            run_cloud("schedules", "delete", "sched-1").output
        )
        assert logged_in.call_for("DELETE", f"/orgs/{logged_in.org}/schedules/sched-1")


class TestSecretCommands:
    def test_list_shows_names_never_values(self, logged_in, run_cloud):
        assert "SNOWFLAKE_PASSWORD" in run_cloud("secrets", "list").output

    def test_list_has_a_dedicated_empty_message(self, logged_in, run_cloud):
        logged_in.set("GET", f"/orgs/{logged_in.org}/secrets", [])
        assert "No secrets in org 'acme'." in run_cloud("secrets", "list").output

    def test_set_sends_the_value_from_the_flag(self, logged_in, run_cloud):
        result = run_cloud("secrets", "set", "PGPASSWORD", "--value", "hunter2")
        assert result.exit_code == 0
        assert logged_in.body_for("POST", f"/orgs/{logged_in.org}/secrets") == {
            "name": "PGPASSWORD",
            "value": "hunter2",
        }

    def test_set_prompts_when_no_value_is_given(self, logged_in, run_cloud):
        result = run_cloud("secrets", "set", "PGPASSWORD", input="hunter2\n")
        assert result.exit_code == 0, result.output
        body = logged_in.body_for("POST", f"/orgs/{logged_in.org}/secrets")
        assert body["value"] == "hunter2"
        assert "hunter2" not in result.output

    def test_delete_removes_by_name(self, logged_in, run_cloud):
        assert "Deleted secret 'PGPASSWORD'" in (
            run_cloud("secrets", "delete", "PGPASSWORD").output
        )
        assert logged_in.call_for("DELETE", f"/orgs/{logged_in.org}/secrets/PGPASSWORD")


class TestDeploySettings:
    """The `[cloud]` block that makes a project directory a deployable job."""

    SOURCE = Path("models/etl/trilogy.toml")

    def _parse(self, table):
        return DeploySettings.from_table(table, self.SOURCE)

    def test_a_full_block_parses(self):
        settings = self._parse(
            {
                "schedule": "0 0 7 * * *",
                "operation": "refresh",
                "timeout_seconds": 1800,
                "secret_env": ["GOOGLE_HMAC_KEY"],
            }
        )
        assert settings.operation == "refresh"
        assert settings.timeout_seconds == 1800
        assert settings.secret_env == ("GOOGLE_HMAC_KEY",)

    def test_pointing_at_an_api_is_configuration_not_a_job(self):
        """A repo-root toml naming an org must not deploy the whole repository
        as one job."""
        assert not self._parse({"api_url": "https://x", "org": "acme"}).declared

    @pytest.mark.parametrize("key", JOB_DECLARING_KEYS)
    def test_any_deployment_key_makes_a_project_deployable(self, key):
        value = {
            "secret_env": ["A"],
            "cpus": 1.0,
            "schedule": "* * * * * *",
            "entrypoint": "main.preql",
        }.get(key, 1)
        if key in ("operation", "vm_class"):
            value = {"operation": "run", "vm_class": "shared"}[key]
        assert self._parse({key: value}).declared

    def test_a_name_alone_does_not_make_a_project_deployable(self):
        """It says what to call a job, not that there is one — a toml naming a
        directory and nothing else describes nothing to run."""
        assert not self._parse({"name": "reports"}).declared

    def test_a_declared_name_is_not_a_content_field(self):
        """It feeds the `name` argument. In `job_fields()` it would ride the
        content payload as a setting, and a PUT would carry it like one."""
        settings = self._parse({"name": "urban-tree-data", "operation": "refresh"})
        assert settings.name == "urban-tree-data"
        assert "name" not in settings.job_fields()

    def test_an_empty_name_is_refused_where_it_was_written(self):
        with pytest.raises(CloudError, match="name"):
            self._parse({"name": "  ", "operation": "run"})

    def test_unspecified_stays_unspecified(self):
        """`None` has to mean "carry what the job has", not "reset to default"
        — a PUT replaces content wholesale."""
        settings = self._parse({"schedule": "0 0 7 * * *"})
        assert settings.operation is None
        assert settings.job_fields() == {}

    def test_schedule_is_not_a_job_field(self):
        """It binds a schedule to the job; it is not part of the job's content."""
        assert "schedule" not in self._parse({"schedule": "0 0 7 * * *"}).job_fields()

    def test_secret_env_reaches_the_payload_as_a_list(self):
        """Tuples are for immutability here; JSON has no such thing."""
        assert self._parse({"secret_env": ["A", "B"]}).job_fields()["secret_env"] == [
            "A",
            "B",
        ]

    @pytest.mark.parametrize(
        "table",
        [
            {"operation": "refreshh"},
            {"vm_class": "dedicated"},
            {"priority": 5},
            {"priority": -1},
            {"timeout_seconds": 0},
            {"timeout_seconds": "1800"},
            {"cpus": 0},
            {"cpus": "two"},
            {"secret_env": "GOOGLE_HMAC_KEY"},
            {"secret_env": [1, 2]},
        ],
    )
    def test_a_bad_value_is_refused(self, table):
        with pytest.raises(CloudError):
            self._parse(table)

    def test_a_boolean_is_not_an_integer(self):
        """`timeout_seconds = true` is a typo, not one second."""
        with pytest.raises(CloudError):
            self._parse({"timeout_seconds": True})

    def test_the_error_names_the_file_and_the_key(self):
        """A sync walks a tree; an error has to say which toml holds the
        problem, since the author never wrote a job name to look for."""
        with pytest.raises(CloudError) as exc:
            self._parse({"operation": "nope"})
        assert str(self.SOURCE) in str(exc.value)
        assert "[cloud].operation" in str(exc.value)


class TestJobFetch:
    """`jobs fetch` is the inverse of `jobs push`, and the one command that
    writes attacker-influenced names to the filesystem."""

    def _seed(self, cloud_api, files):
        cloud_api.set(
            "GET",
            f"/orgs/{cloud_api.org}/jobs",
            [
                {
                    "id": "job-1",
                    "org_id": "org-acme",
                    "name": "nightly",
                    "operation": "refresh",
                    "config": '[engine]\ndialect = "duck_db"\n',
                    "files": files,
                    "current_version_id": "job-1-v1",
                    "created_at": TS,
                    "updated_at": TS,
                }
            ],
        )

    def test_the_bundle_layout_round_trips(self, logged_in, run_cloud, tmp_path):
        self._seed(
            logged_in,
            [
                {"name": "model.preql", "content": "key id int;"},
                {"name": "nested/helper.py", "content": "x = 1"},
            ],
        )
        dest = tmp_path / "out"
        result = run_cloud("jobs", "fetch", "nightly", "--dest", str(dest))
        assert result.exit_code == 0, result.output
        assert (
            (dest / "trilogy.toml").read_text(encoding="utf-8").startswith("[engine]")
        )
        assert (dest / "model.preql").read_text(encoding="utf-8") == "key id int;"
        assert (dest / "nested" / "helper.py").read_text(encoding="utf-8") == "x = 1"

    @pytest.mark.parametrize(
        "name",
        [
            "../escaped.preql",
            "nested/../../escaped.preql",
            "/escaped.preql",
            "C:/escaped.preql",
            "..\\escaped.preql",
            "\\\\server\\share\\escaped.preql",
        ],
    )
    def test_a_name_that_escapes_the_destination_is_refused(
        self, logged_in, run_cloud, tmp_path, name
    ):
        """A stored bundle is data, not a promise. Writing these would turn a
        fetch into an arbitrary file write."""
        self._seed(logged_in, [{"name": name, "content": "pwned"}])
        dest = tmp_path / "out"
        result = run_cloud("jobs", "fetch", "nightly", "--dest", str(dest))
        assert result.exit_code != 0
        assert "escapes" in result.output
        assert not (tmp_path / "escaped.preql").exists()

    def test_a_refused_bundle_leaves_nothing_behind(
        self, logged_in, run_cloud, tmp_path
    ):
        """Names are all resolved before anything is written, so the refusal is
        something to act on rather than something to clean up after."""
        self._seed(
            logged_in,
            [
                {"name": "good.preql", "content": "key id int;"},
                {"name": "../escaped.preql", "content": "pwned"},
            ],
        )
        dest = tmp_path / "out"
        assert run_cloud("jobs", "fetch", "nightly", "--dest", str(dest)).exit_code != 0
        assert not dest.exists()

    def test_a_non_empty_destination_is_refused_without_force(
        self, logged_in, run_cloud, tmp_path
    ):
        """Fetching over a checkout would silently revert local edits."""
        self._seed(logged_in, [])
        dest = tmp_path / "out"
        dest.mkdir()
        (dest / "mine.preql").write_text("local work", encoding="utf-8")
        result = run_cloud("jobs", "fetch", "nightly", "--dest", str(dest))
        assert result.exit_code != 0 and "--force" in result.output
        assert (dest / "mine.preql").read_text(encoding="utf-8") == "local work"

    def test_force_writes_into_a_non_empty_destination(
        self, logged_in, run_cloud, tmp_path
    ):
        self._seed(logged_in, [{"name": "model.preql", "content": "new"}])
        dest = tmp_path / "out"
        dest.mkdir()
        (dest / "model.preql").write_text("old", encoding="utf-8")
        result = run_cloud("jobs", "fetch", "nightly", "--dest", str(dest), "--force")
        assert result.exit_code == 0, result.output
        assert (dest / "model.preql").read_text(encoding="utf-8") == "new"

    def test_the_fetched_job_reports_where_it_came_from(
        self, logged_in, run_cloud, tmp_path
    ):
        """The point of fetching: knowing whether the cloud's copy is the one
        your checkout claims to have pushed."""
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/jobs",
            [
                {
                    "id": "job-1",
                    "org_id": "org-acme",
                    "name": "nightly",
                    "operation": "run",
                    "config": "[engine]\n",
                    "files": [],
                    "source_fingerprint": {
                        "version": SOURCE_FINGERPRINT_VERSION,
                        "content": "abc123",
                        "origin": "github.com/acme/models",
                        "origin_kind": "git",
                        "path": "etl",
                    },
                    "created_at": TS,
                    "updated_at": TS,
                }
            ],
        )
        dest = tmp_path / "out"
        result = run_cloud("jobs", "fetch", "nightly", "--dest", str(dest))
        assert result.exit_code == 0, result.output
        assert "github.com/acme/models" in result.output

    def test_fetch_emits_a_json_event(self, logged_in, run_cloud, tmp_path, json_mode):
        self._seed(logged_in, [{"name": "model.preql", "content": "key id int;"}])
        dest = tmp_path / "out"
        result = run_cloud("jobs", "fetch", "nightly", "--dest", str(dest))
        event = [e for e in _json_stream(result.output) if e["event"] == "job_fetched"][
            -1
        ]
        assert event["files"] == 1 and event["dest"] == str(dest)

    def test_stored_bytes_land_verbatim(self, logged_in, run_cloud, tmp_path):
        """Python's text mode rewrites every \\n as \\r\\n on Windows, so a
        fetch produced a file differing from the platform's copy on every line.
        A push back hides it — it reads with universal newlines and normalizes
        again — but dropping the fetch over a checkout with --force turns a
        two-line edit into a whole-file diff."""
        self._seed(logged_in, [{"name": "model.preql", "content": "key id int;\n"}])
        dest = tmp_path / "out"
        assert run_cloud("jobs", "fetch", "nightly", "--dest", str(dest)).exit_code == 0
        assert (dest / "model.preql").read_bytes() == b"key id int;\n"

    def test_an_object_config_is_written_as_readable_toml_text(
        self, logged_in, run_cloud, tmp_path
    ):
        """The column is JSON; a server that stored an object must not produce
        a trilogy.toml holding a Python-repr dict."""
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/jobs",
            [
                {
                    "id": "job-1",
                    "org_id": "org-acme",
                    "name": "nightly",
                    "operation": "run",
                    "config": {"engine": {"dialect": "duck_db"}},
                    "files": [],
                    "created_at": TS,
                    "updated_at": TS,
                }
            ],
        )
        dest = tmp_path / "out"
        assert run_cloud("jobs", "fetch", "nightly", "--dest", str(dest)).exit_code == 0
        assert "'" not in (dest / "trilogy.toml").read_text(encoding="utf-8")


def _workspace(workspace_id: str = "ws-1", name: str = "space", **over) -> dict:
    """A workspace as the API serializes it — see conftest's copy; repeated
    here so a fetch test can declare the row it is fetching inline."""
    return {
        "id": workspace_id,
        "org_id": "org-acme",
        "name": name,
        "files": [{"name": "model.preql", "content": "key id int;"}],
        "current_version_id": f"{workspace_id}-v1",
        **over,
    }


class TestWorkspaceListing:
    """Finding a workspace, and finding out what runs out of it — the two
    questions that precede a fetch."""

    def test_list_renders_a_row_per_workspace(self, logged_in, run_cloud):
        output = run_cloud("workspaces", "list").output
        assert "'space'" in output and "files: 1" in output

    def test_list_names_the_workspace_a_row_extends(self, logged_in, run_cloud):
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/workspaces",
            [_workspace(parent_workspace_id="ws-base")],
        )
        assert "extends: ws-base" in run_cloud("workspaces", "list").output

    def test_a_row_without_files_counts_zero_rather_than_failing(
        self, logged_in, run_cloud
    ):
        """`files` is an untyped blob on both sides of the wire, so a workspace
        holding `null` — or an object — has to render a count, not a traceback."""
        logged_in.set(
            "GET", f"/orgs/{logged_in.org}/workspaces", [_workspace(files=None)]
        )
        result = run_cloud("workspaces", "list")
        assert result.exit_code == 0, result.output
        assert "files: 0" in result.output

    def test_list_has_a_dedicated_empty_message(self, logged_in, run_cloud):
        logged_in.set("GET", f"/orgs/{logged_in.org}/workspaces", [])
        assert "No workspaces in org 'acme'." in run_cloud("workspaces", "list").output

    def test_list_emits_a_json_event_tagged_with_the_org(
        self, logged_in, run_cloud, json_mode
    ):
        payload = json.loads(run_cloud("workspaces", "list").output)
        assert payload["event"] == "workspaces" and payload["org"] == "acme"

    def test_jobs_reports_each_job_with_the_script_it_runs(self, logged_in, run_cloud):
        result = run_cloud("workspaces", "jobs", "space")
        assert result.exit_code == 0, result.output
        assert "nightly" in result.output and "entrypoint: a.preql" in result.output

    def test_jobs_names_a_job_that_runs_the_whole_directory(self, logged_in, run_cloud):
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/workspaces/ws-1/jobs",
            [
                {
                    "id": "job-1",
                    "org_id": "org-acme",
                    "name": "nightly",
                    "operation": "run",
                    "workspace_id": "ws-1",
                    "created_at": TS,
                    "updated_at": TS,
                }
            ],
        )
        assert (
            "entrypoint: whole directory"
            in run_cloud("workspaces", "jobs", "space").output
        )

    def test_jobs_resolves_the_workspace_by_id_as_well_as_name(
        self, logged_in, run_cloud
    ):
        assert run_cloud("workspaces", "jobs", "ws-1").exit_code == 0

    def test_jobs_has_a_dedicated_empty_message(self, logged_in, run_cloud):
        logged_in.set("GET", f"/orgs/{logged_in.org}/workspaces/ws-1/jobs", [])
        assert (
            "No jobs use workspace 'space'."
            in run_cloud("workspaces", "jobs", "space").output
        )

    def test_an_unknown_workspace_is_named_in_the_error(self, logged_in, run_cloud):
        result = run_cloud("workspaces", "jobs", "ghost")
        assert result.exit_code != 0
        assert "No workspace named 'ghost'" in result.output


class TestWorkspaceFetch:
    """Exporting the shared tree, which is where a multi-job project's files
    actually live — the jobs above it carry none."""

    def test_the_tree_lands_at_its_bundled_paths(self, logged_in, run_cloud, tmp_path):
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/workspaces",
            [
                _workspace(
                    files=[
                        {"name": "model.preql", "content": "key id int;"},
                        {"name": "nested/helper.py", "content": "x = 1"},
                    ]
                )
            ],
        )
        dest = tmp_path / "out"
        result = run_cloud("workspaces", "fetch", "space", "--dest", str(dest))
        assert result.exit_code == 0, result.output
        assert (dest / "model.preql").read_text(encoding="utf-8") == "key id int;"
        assert (dest / "nested" / "helper.py").read_text(encoding="utf-8") == "x = 1"

    def test_a_workspace_without_a_config_writes_no_trilogy_toml(
        self, logged_in, run_cloud, tmp_path
    ):
        """Which is every workspace `cloud sync` deploys: config layering needs
        pytrilogy's --config-overlay, so config stays on the jobs. A file
        holding the four bytes `null` is not something anyone can push back."""
        dest = tmp_path / "out"
        assert (
            run_cloud("workspaces", "fetch", "space", "--dest", str(dest)).exit_code
            == 0
        )
        assert not (dest / "trilogy.toml").exists()

    def test_a_workspace_config_is_written_when_it_has_one(
        self, logged_in, run_cloud, tmp_path
    ):
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/workspaces",
            [_workspace(config="[engine]\ndialect = 'duck_db'\n")],
        )
        dest = tmp_path / "out"
        assert (
            run_cloud("workspaces", "fetch", "space", "--dest", str(dest)).exit_code
            == 0
        )
        assert (
            (dest / "trilogy.toml").read_text(encoding="utf-8").startswith("[engine]")
        )

    def test_the_jobs_that_run_out_of_it_are_reported(
        self, logged_in, run_cloud, tmp_path
    ):
        """The tree on disk is shared, so what each job does with it is the
        entrypoint it names and nothing else."""
        dest = tmp_path / "out"
        result = run_cloud("workspaces", "fetch", "space", "--dest", str(dest))
        assert result.exit_code == 0, result.output
        assert "nightly" in result.output and "a.preql" in result.output

    def test_a_non_empty_destination_is_refused_without_force(
        self, logged_in, run_cloud, tmp_path
    ):
        dest = tmp_path / "out"
        dest.mkdir()
        (dest / "mine.preql").write_text("local work", encoding="utf-8")
        result = run_cloud("workspaces", "fetch", "space", "--dest", str(dest))
        assert result.exit_code != 0 and "--force" in result.output
        assert (dest / "mine.preql").read_text(encoding="utf-8") == "local work"

    def test_an_escaping_name_is_refused(self, logged_in, run_cloud, tmp_path):
        """A stored bundle is data wherever it is stored; the workspace routes
        are no more trusted than the job ones."""
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/workspaces",
            [_workspace(files=[{"name": "../escaped.preql", "content": "pwned"}])],
        )
        dest = tmp_path / "out"
        result = run_cloud("workspaces", "fetch", "space", "--dest", str(dest))
        assert result.exit_code != 0 and "escapes" in result.output
        assert not (tmp_path / "escaped.preql").exists()

    def test_an_unknown_workspace_is_named_in_the_error(self, logged_in, run_cloud):
        result = run_cloud("workspaces", "fetch", "ghost", "--dest", "out")
        assert result.exit_code != 0 and "ghost" in result.output

    def test_resolved_adds_the_parent_tree_and_the_child_wins(
        self, logged_in, run_cloud, tmp_path
    ):
        """Nearest to the job wins on a collision — the platform's own
        resolution rule, so the merged directory is what an executor sees."""
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/workspaces",
            [
                _workspace(
                    parent_workspace_id="ws-base",
                    files=[{"name": "model.preql", "content": "child"}],
                ),
                _workspace(
                    "ws-base",
                    "base",
                    files=[
                        {"name": "model.preql", "content": "parent"},
                        {"name": "shared.preql", "content": "from the parent"},
                    ],
                ),
            ],
        )
        dest = tmp_path / "out"
        result = run_cloud(
            "workspaces", "fetch", "space", "--dest", str(dest), "--resolved"
        )
        assert result.exit_code == 0, result.output
        assert (dest / "model.preql").read_text(encoding="utf-8") == "child"
        assert (dest / "shared.preql").read_text(encoding="utf-8") == "from the parent"

    def test_without_resolved_only_its_own_files_are_written(
        self, logged_in, run_cloud, tmp_path
    ):
        """A fetch writes what a push would send back, so an unedited round
        trip mints nothing. Inheriting the parent's files here would push them
        into the child on the way back."""
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/workspaces",
            [
                _workspace(parent_workspace_id="ws-base"),
                _workspace(
                    "ws-base",
                    "base",
                    files=[{"name": "shared.preql", "content": "from the parent"}],
                ),
            ],
        )
        dest = tmp_path / "out"
        result = run_cloud("workspaces", "fetch", "space", "--dest", str(dest))
        assert result.exit_code == 0, result.output
        assert not (dest / "shared.preql").exists()
        assert "--resolved" in result.output

    def test_a_parent_cycle_terminates(self, logged_in, run_cloud, tmp_path):
        """The platform refuses cycles at the write; a client that trusted that
        alone would spin forever on a database repaired by hand."""
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/workspaces",
            [
                _workspace(parent_workspace_id="ws-2"),
                _workspace("ws-2", "other", parent_workspace_id="ws-1"),
            ],
        )
        dest = tmp_path / "out"
        result = run_cloud(
            "workspaces", "fetch", "space", "--dest", str(dest), "--resolved"
        )
        assert result.exit_code == 0, result.output

    def test_fetch_emits_a_json_event(self, logged_in, run_cloud, tmp_path, json_mode):
        dest = tmp_path / "out"
        result = run_cloud("workspaces", "fetch", "space", "--dest", str(dest))
        event = [
            e for e in _json_stream(result.output) if e["event"] == "workspace_fetched"
        ][-1]
        assert event["files"] == 1 and event["dest"] == str(dest)
        assert [j["name"] for j in event["jobs"]] == ["nightly"]


class TestJobFetchWithWorkspace:
    """A job in a workspace carries no files of its own, which is the whole
    point of the arrangement and used to make `jobs fetch` write an empty
    directory with nothing to say about it."""

    def _seed(self, api, job_files=None, **workspace_over):
        api.set(
            "GET",
            f"/orgs/{api.org}/jobs",
            [
                {
                    "id": "job-1",
                    "org_id": "org-acme",
                    "name": "nightly",
                    "operation": "refresh",
                    "config": "[engine]\n",
                    "files": job_files if job_files is not None else [],
                    "workspace_id": "ws-1",
                    "entrypoint": "refresh.preql",
                    "created_at": TS,
                    "updated_at": TS,
                }
            ],
        )
        api.set("GET", f"/orgs/{api.org}/workspaces", [_workspace(**workspace_over)])

    def test_an_unresolved_fetch_says_where_the_files_are(
        self, logged_in, run_cloud, tmp_path
    ):
        self._seed(logged_in)
        dest = tmp_path / "out"
        result = run_cloud("jobs", "fetch", "nightly", "--dest", str(dest))
        assert result.exit_code == 0, result.output
        assert not (dest / "model.preql").exists()
        assert "space" in result.output and "--resolved" in result.output

    def test_resolved_materializes_the_workspace_tree(
        self, logged_in, run_cloud, tmp_path
    ):
        self._seed(logged_in)
        dest = tmp_path / "out"
        result = run_cloud(
            "jobs", "fetch", "nightly", "--dest", str(dest), "--resolved"
        )
        assert result.exit_code == 0, result.output
        assert (dest / "model.preql").read_text(encoding="utf-8") == "key id int;"
        assert (
            (dest / "trilogy.toml").read_text(encoding="utf-8").startswith("[engine]")
        )

    def test_the_jobs_own_file_shadows_the_workspaces(
        self, logged_in, run_cloud, tmp_path
    ):
        self._seed(logged_in, job_files=[{"name": "model.preql", "content": "mine"}])
        dest = tmp_path / "out"
        result = run_cloud(
            "jobs", "fetch", "nightly", "--dest", str(dest), "--resolved"
        )
        assert result.exit_code == 0, result.output
        assert (dest / "model.preql").read_text(encoding="utf-8") == "mine"

    def test_a_resolved_fetch_warns_that_it_is_not_a_bundle_to_push_back(
        self, logged_in, run_cloud, tmp_path
    ):
        """Pushing it as the job would install a private copy of the shared
        tree: the edit lands, the siblings never see it, and the two drift."""
        self._seed(logged_in)
        dest = tmp_path / "out"
        result = run_cloud(
            "jobs", "fetch", "nightly", "--dest", str(dest), "--resolved"
        )
        assert "workspaces push" in result.output

    def test_a_self_contained_job_asks_the_workspace_route_nothing(
        self, logged_in, run_cloud, tmp_path
    ):
        """The chain read is worth one request, and only when there is a chain."""
        dest = tmp_path / "out"
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/jobs",
            [
                {
                    "id": "job-1",
                    "org_id": "org-acme",
                    "name": "nightly",
                    "operation": "run",
                    "config": "[engine]\n",
                    "files": [{"name": "model.preql", "content": "key id int;"}],
                    "created_at": TS,
                    "updated_at": TS,
                }
            ],
        )
        assert run_cloud("jobs", "fetch", "nightly", "--dest", str(dest)).exit_code == 0
        assert not logged_in.requests_for("GET", f"/orgs/{logged_in.org}/workspaces")


class TestWorkspacePush:
    """The write half of the loop: edits to a shared tree belong to the
    workspace, because the jobs above it carry no files."""

    def _source(self, tmp_path: Path, **files: str) -> Path:
        source = tmp_path / "src"
        source.mkdir()
        for name, content in (files or {"model.preql": "key id int;"}).items():
            path = source / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
        return source

    def test_a_missing_workspace_is_created(self, logged_in, run_cloud, tmp_path):
        logged_in.set("GET", f"/orgs/{logged_in.org}/workspaces", [])
        result = run_cloud(
            "workspaces",
            "push",
            "--source",
            str(self._source(tmp_path)),
            "--name",
            "fresh",
        )
        assert result.exit_code == 0, result.output
        body = logged_in.body_for("POST", f"/orgs/{logged_in.org}/workspaces")
        assert body["name"] == "fresh"
        assert {f["name"] for f in body["files"]} == {"model.preql"}

    def test_an_existing_workspace_is_replaced_in_place(
        self, logged_in, run_cloud, tmp_path
    ):
        """By name, which the platform makes unique per org — the same key
        `cloud sync` deploys against, so the two write one row."""
        result = run_cloud(
            "workspaces",
            "push",
            "--source",
            str(self._source(tmp_path, **{"model.preql": "edited"})),
            "--name",
            "space",
        )
        assert result.exit_code == 0, result.output
        assert not logged_in.requests_for("POST", f"/orgs/{logged_in.org}/workspaces")
        body = logged_in.body_for("PUT", f"/orgs/{logged_in.org}/workspaces/ws-1")
        assert body["files"] == [{"name": "model.preql", "content": "edited"}]

    @pytest.mark.parametrize(
        "field,value",
        [
            ("parameters", {"region": "eu"}),
            ("secret_env", ["SNOWFLAKE_PASSWORD"]),
            ("timeout_seconds", 1800),
            ("memory_mb", 4096),
            ("cpus", 2.0),
            ("vm_class", "shared"),
            ("parent_workspace_id", "ws-base"),
            ("description", "the shared tree"),
        ],
    )
    def test_an_unnamed_setting_survives_the_push(
        self, logged_in, run_cloud, tmp_path, field, value
    ):
        """A PUT replaces a workspace wholesale, so anything the caller was not
        told about has to be read off the workspace and resent."""
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/workspaces",
            [_workspace(**{field: value})],
        )
        result = run_cloud(
            "workspaces",
            "push",
            "--source",
            str(self._source(tmp_path)),
            "--name",
            "space",
        )
        assert result.exit_code == 0, result.output
        body = logged_in.body_for("PUT", f"/orgs/{logged_in.org}/workspaces/ws-1")
        assert body[field] == value

    def test_a_named_setting_wins_over_the_carried_one(
        self, logged_in, run_cloud, tmp_path
    ):
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/workspaces",
            [_workspace(timeout_seconds=60)],
        )
        run_cloud(
            "workspaces",
            "push",
            "--source",
            str(self._source(tmp_path)),
            "--name",
            "space",
            "--timeout-seconds",
            "1800",
        )
        body = logged_in.body_for("PUT", f"/orgs/{logged_in.org}/workspaces/ws-1")
        assert body["timeout_seconds"] == 1800

    def test_every_content_field_on_the_model_is_carried(self):
        """The pin behind all of the above: a content field `Workspace` models
        but the payload omits is cleared on every push, and nothing else in the
        suite would notice."""
        from trilogy.scripts.cloud_models import Workspace

        content_fields = set(Workspace.model_fields) - {
            "id",
            "org_id",
            "name",
            "files",
            # Carried by its own branch: a config is replaced, not merged, and
            # a push only supplies one deliberately.
            "config",
            "current_version_id",
        }
        assert content_fields == set(cloud_mod.WORKSPACE_CARRIED_FIELDS)
        # And the reader that supplies them, so neither half can drift alone.
        assert content_fields == set(
            cloud_mod._carried_workspace_settings(Workspace(**_workspace()))
        )

    def test_a_content_no_op_is_reported_as_one(self, logged_in, run_cloud, tmp_path):
        """Re-pushing an unedited fetch must not claim to have changed
        anything — the version pointer is what says whether it did."""
        logged_in.set("PUT", f"/orgs/{logged_in.org}/workspaces/*", _workspace())
        result = run_cloud(
            "workspaces",
            "push",
            "--source",
            str(self._source(tmp_path)),
            "--name",
            "space",
        )
        assert result.exit_code == 0, result.output
        assert "no new version" in result.output

    def test_a_trilogy_toml_in_the_tree_is_not_bundled_as_a_file(
        self, logged_in, run_cloud, tmp_path
    ):
        """A copy in the tree would shadow the config in the executor's
        workdir — the same reason `jobs push` keeps it out of the file list."""
        source = self._source(
            tmp_path, **{"model.preql": "key id int;", "trilogy.toml": "[engine]\n"}
        )
        result = run_cloud(
            "workspaces", "push", "--source", str(source), "--name", "space"
        )
        assert result.exit_code == 0, result.output
        body = logged_in.body_for("PUT", f"/orgs/{logged_in.org}/workspaces/ws-1")
        assert [f["name"] for f in body["files"]] == ["model.preql"]

    def test_a_config_is_not_invented_for_a_workspace_that_has_none(
        self, logged_in, run_cloud, tmp_path
    ):
        """Storing one would break every job under it on today's CLI, which
        fails on the unknown --config-overlay flag."""
        source = self._source(
            tmp_path, **{"model.preql": "key id int;", "trilogy.toml": "[engine]\n"}
        )
        result = run_cloud(
            "workspaces", "push", "--source", str(source), "--name", "space"
        )
        body = logged_in.body_for("PUT", f"/orgs/{logged_in.org}/workspaces/ws-1")
        assert "config" not in body
        assert "Ignoring trilogy.toml" in result.output

    def test_an_edited_config_round_trips_when_the_workspace_has_one(
        self, logged_in, run_cloud, tmp_path
    ):
        """It is the file a fetch wrote, so dropping the edit would break the
        loop this command exists to close."""
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/workspaces",
            [_workspace(config="[engine]\n")],
        )
        source = self._source(
            tmp_path,
            **{"model.preql": "key id int;", "trilogy.toml": "[engine]\nedited = 1\n"},
        )
        run_cloud("workspaces", "push", "--source", str(source), "--name", "space")
        body = logged_in.body_for("PUT", f"/orgs/{logged_in.org}/workspaces/ws-1")
        assert body["config"] == "[engine]\nedited = 1\n"

    def test_an_explicit_config_is_stored_even_without_one_already(
        self, logged_in, run_cloud, tmp_path
    ):
        source = self._source(tmp_path)
        config = tmp_path / "other.toml"
        config.write_text("[engine]\ndeliberate = 1\n", encoding="utf-8")
        run_cloud(
            "workspaces",
            "push",
            "--source",
            str(source),
            "--name",
            "space",
            "--config",
            str(config),
        )
        body = logged_in.body_for("PUT", f"/orgs/{logged_in.org}/workspaces/ws-1")
        assert body["config"] == "[engine]\ndeliberate = 1\n"

    def test_an_empty_source_is_refused(self, logged_in, run_cloud, tmp_path):
        empty = tmp_path / "empty"
        empty.mkdir()
        result = run_cloud(
            "workspaces", "push", "--source", str(empty), "--name", "space"
        )
        assert result.exit_code != 0 and "No files matched" in result.output

    def test_rewrites_apply_to_the_tree(self, logged_in, run_cloud, tmp_path):
        source = self._source(tmp_path, **{"model.preql": "address prod.orders;"})
        run_cloud(
            "workspaces",
            "push",
            "--source",
            str(source),
            "--name",
            "space",
            "--rewrite",
            "prod.=dev.",
        )
        body = logged_in.body_for("PUT", f"/orgs/{logged_in.org}/workspaces/ws-1")
        assert body["files"][0]["content"] == "address dev.orders;"

    def test_push_emits_a_json_event(self, logged_in, run_cloud, tmp_path, json_mode):
        result = run_cloud(
            "workspaces",
            "push",
            "--source",
            str(self._source(tmp_path)),
            "--name",
            "space",
        )
        event = [
            e
            for e in _json_stream(result.output)
            if e["event"].startswith("workspace_")
        ][-1]
        assert event["outcome"] == "updated"


class TestPushingATreeIntoAWorkspaceBoundJob:
    def test_it_warns_and_names_the_command_that_accepts_it(
        self, logged_in, run_cloud, tmp_path
    ):
        """The natural way to reach here is `jobs fetch --resolved` followed by
        a push: the job gets a private copy of the shared tree, so the edit
        lands, the siblings never see it, and the two copies drift."""
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/jobs",
            [
                {
                    "id": "job-1",
                    "org_id": "org-acme",
                    "name": "nightly",
                    "operation": "run",
                    "config": "[engine]\n",
                    "files": [],
                    "workspace_id": "ws-1",
                    "created_at": TS,
                    "updated_at": TS,
                }
            ],
        )
        source = tmp_path / "src"
        source.mkdir()
        (source / "trilogy.toml").write_text("[engine]\n", encoding="utf-8")
        (source / "model.preql").write_text("key id int;", encoding="utf-8")
        result = run_cloud("jobs", "push", "--source", str(source), "--name", "nightly")
        assert result.exit_code == 0, result.output
        assert "workspaces push" in result.output


class TestDeriveJobName:
    def test_the_path_under_the_root_becomes_the_name(self, tmp_path):
        root = tmp_path / "models"
        directory = root / "duckdb" / "covid19_open_data" / "data"
        directory.mkdir(parents=True)
        assert (
            cloud_mod.derive_job_name(root, directory)
            == "duckdb-covid19_open_data-data"
        )

    def test_leaf_names_alone_would_collide_and_the_full_path_does_not(self, tmp_path):
        """Half the directories in a models repo are called `data`."""
        root = tmp_path / "models"
        left = root / "covid" / "data"
        right = root / "gcat" / "data"
        left.mkdir(parents=True)
        right.mkdir(parents=True)
        assert cloud_mod.derive_job_name(root, left) != cloud_mod.derive_job_name(
            root, right
        )

    def test_the_root_itself_is_named_for_the_root(self, tmp_path):
        root = tmp_path / "etl"
        root.mkdir()
        assert cloud_mod.derive_job_name(root, root) == "etl"


class TestDiscoverProjects:
    def _project(self, directory: Path, body: str) -> Path:
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "trilogy.toml").write_text(body, encoding="utf-8")
        (directory / "model.preql").write_text("key id int;", encoding="utf-8")
        return directory

    def test_a_declared_project_is_found(self, tmp_path):
        self._project(tmp_path / "etl", '[cloud]\noperation = "refresh"\n')
        found = cloud_mod.discover_projects(tmp_path)
        assert [p.name for p in found] == ["etl"]
        assert found[0].settings.operation == "refresh"

    def test_a_config_only_toml_is_not_a_job(self, tmp_path):
        """The repo-root toml names an org; deploying the whole repository as
        one job because of it would be a surprising way to find out."""
        self._project(tmp_path, '[cloud]\napi_url = "https://x"\norg = "acme"\n')
        assert cloud_mod.discover_projects(tmp_path) == []

    def test_a_toml_without_a_cloud_block_is_not_a_job(self, tmp_path):
        self._project(tmp_path / "etl", '[engine]\ndialect = "duck_db"\n')
        assert cloud_mod.discover_projects(tmp_path) == []

    def test_several_projects_come_back_in_a_stable_order(self, tmp_path):
        self._project(tmp_path / "b", '[cloud]\noperation = "run"\n')
        self._project(tmp_path / "a", '[cloud]\noperation = "run"\n')
        assert [p.name for p in cloud_mod.discover_projects(tmp_path)] == ["a", "b"]

    def test_a_bad_value_names_its_own_file(self, tmp_path):
        self._project(tmp_path / "etl", '[cloud]\noperation = "nope"\n')
        with pytest.raises(CloudError, match="etl"):
            cloud_mod.discover_projects(tmp_path)

    def test_unparseable_toml_names_its_own_file(self, tmp_path):
        self._project(tmp_path / "etl", "[cloud\noperation =\n")
        with pytest.raises(CloudError, match="Could not parse"):
            cloud_mod.discover_projects(tmp_path)

    def test_a_non_table_cloud_key_is_not_a_job(self, tmp_path):
        self._project(tmp_path / "etl", 'cloud = "nonsense"\n')
        assert cloud_mod.discover_projects(tmp_path) == []

    def test_a_move_changes_the_identity_as_well_as_the_name(self, tmp_path):
        """Identity is path-derived too, so a move is a *new* job.

        The docstrings once claimed the opposite. It matters because it is the
        difference between "renamed" and "orphaned plus duplicated", and
        `--prune` is the only thing that clears up after it.
        """
        first = self._project(tmp_path / "old", '[cloud]\noperation = "run"\n')
        before = cloud_mod.discover_projects(tmp_path)[0]
        first.rename(tmp_path / "new")
        after = cloud_mod.discover_projects(tmp_path)[0]
        assert (before.name, after.name) == ("old", "new")
        assert before.source_key != after.source_key

    def test_identity_survives_what_it_is_supposed_to_survive(self, tmp_path):
        """What identity actually buys: the same directory keys the same job
        across every branch and commit of it, which is what groups a branch's
        job under the mainline job it forked from."""
        base = {"kind": "git", "location": "github.com/acme/models", "subpath": "etl"}
        main = SourceOrigin(**base, branch="main", revision="a" * 40)
        feature = SourceOrigin(**base, branch="feature/x", revision="b" * 40)
        assert main.source_key() == feature.source_key()

    # -- [[cloud.job]]: several jobs over one directory ---------------------
    #
    # A directory holds exactly one trilogy.toml, so "one toml, one job" made
    # a pipeline — a refresh plus the jobs that publish its output — something
    # you could not declare at all. These pin the shape of the way out.

    TWO_JOBS = """
[cloud]
secret_env = ["SHARED_KEY"]
exclude = ["*_local.preql"]

[[cloud.job]]
key = "refresh"
name = "space-refresh"
entrypoint = "refresh.preql"
operation = "refresh"
schedule = "0 0 6 * * *"
timeout_seconds = 1800

[[cloud.job]]
key = "publish"
name = "space-publish"
entrypoint = "publish.preql"
operation = "run"
schedule = "0 0 6 * * *"
"""

    def test_one_toml_can_declare_several_jobs(self, tmp_path):
        self._project(tmp_path / "data", self.TWO_JOBS)
        found = cloud_mod.discover_projects(tmp_path)
        assert [p.name for p in found] == ["space-refresh", "space-publish"]
        assert [p.settings.operation for p in found] == ["refresh", "run"]

    def test_each_declared_job_gets_its_own_identity(self, tmp_path):
        """`::{key}`, so the two never collide and neither is the directory's
        bare key — which would make one of them indistinguishable from a
        single-job deploy of the same folder."""
        self._project(tmp_path / "data", self.TWO_JOBS)
        keys = [p.source_key for p in cloud_mod.discover_projects(tmp_path)]
        assert len(set(keys)) == 2
        assert all("::" in k for k in keys)
        assert keys[0].endswith("::refresh") and keys[1].endswith("::publish")

    def test_identity_follows_the_key_not_the_name(self, tmp_path):
        """The whole point of splitting them: renaming a job keeps its history,
        the same guarantee `[cloud] name` bought for the single-job form."""
        directory = self._project(tmp_path / "data", self.TWO_JOBS)
        before = cloud_mod.discover_projects(tmp_path)[0].source_key
        (directory / "trilogy.toml").write_text(
            self.TWO_JOBS.replace('name = "space-refresh"', 'name = "gcat-refresh"'),
            encoding="utf-8",
        )
        after = cloud_mod.discover_projects(tmp_path)[0]
        assert after.name == "gcat-refresh"
        assert after.source_key == before

    def test_block_settings_are_defaults_the_entries_override(self, tmp_path):
        self._project(tmp_path / "data", self.TWO_JOBS)
        found = cloud_mod.discover_projects(tmp_path)
        # Inherited by both...
        assert all(p.settings.secret_env == ("SHARED_KEY",) for p in found)
        assert all("*_local.preql" in p.exclude for p in found)
        # ...and the entry's own value wins where it says one.
        assert found[0].settings.timeout_seconds == 1800
        assert found[1].settings.timeout_seconds is None

    def test_a_declared_exclude_adds_to_the_defaults(self, tmp_path):
        """Nobody excluding a debug script means to let `.venv` back in."""
        self._project(tmp_path / "data", self.TWO_JOBS)
        found = cloud_mod.discover_projects(tmp_path)
        assert set(cloud_mod.DEFAULT_EXCLUDE).issubset(set(found[0].exclude))

    def test_per_job_filters_slice_the_shared_tree(self, tmp_path):
        """The reason filters are not optional: several jobs over one directory
        means every file reaching every job unless something says otherwise,
        which is how a stray debug script ends up running in production."""
        directory = self._project(
            tmp_path / "data",
            """
[cloud]

[[cloud.job]]
key = "a"
name = "job-a"
entrypoint = "debug.preql"
operation = "run"
exclude = ["debug.preql"]

[[cloud.job]]
key = "b"
name = "job-b"
entrypoint = "only.preql"
operation = "run"
include = ["only.preql"]
""",
        )
        (directory / "debug.preql").write_text("key x int;", encoding="utf-8")
        (directory / "only.preql").write_text("key y int;", encoding="utf-8")
        a, b = cloud_mod.discover_projects(tmp_path)
        names_a = {
            f["name"] for f in cloud_mod.collect_files(directory, a.include, a.exclude)
        }
        names_b = {
            f["name"] for f in cloud_mod.collect_files(directory, b.include, b.exclude)
        }
        assert "debug.preql" not in names_a and "model.preql" in names_a
        assert names_b == {"only.preql"}

    def test_an_entry_without_a_key_is_refused(self, tmp_path):
        self._project(
            tmp_path / "data",
            '[cloud]\n\n[[cloud.job]]\nname = "x"\noperation = "run"\n',
        )
        with pytest.raises(CloudError, match="must declare a key"):
            cloud_mod.discover_projects(tmp_path)

    def test_an_entry_without_a_name_is_refused(self, tmp_path):
        """Names are how a deploy is read, and a directory with several jobs
        has no path to derive one from."""
        self._project(
            tmp_path / "data",
            '[cloud]\n\n[[cloud.job]]\nkey = "x"\noperation = "run"\n',
        )
        with pytest.raises(CloudError, match="must declare a name"):
            cloud_mod.discover_projects(tmp_path)

    def test_two_entries_sharing_a_key_are_refused(self, tmp_path):
        self._project(
            tmp_path / "data",
            '[cloud]\n\n[[cloud.job]]\nkey = "x"\nname = "a"\n'
            'entrypoint = "m.preql"\noperation = "run"\n'
            '\n[[cloud.job]]\nkey = "x"\nname = "b"\n'
            'entrypoint = "m.preql"\noperation = "run"\n',
        )
        with pytest.raises(CloudError, match="reuses key"):
            cloud_mod.discover_projects(tmp_path)

    def test_a_key_outside_a_job_entry_is_refused(self, tmp_path):
        """Silently ignoring it would leave someone believing they had declared
        an identity they had not."""
        self._project(tmp_path / "data", '[cloud]\nkey = "x"\noperation = "run"\n')
        with pytest.raises(CloudError, match="only meaningful inside"):
            cloud_mod.discover_projects(tmp_path)

    def test_an_entry_deploys_on_identity_alone(self, tmp_path):
        """Being *in* the array is the declaration — unlike a bare [cloud]
        block, where a lone name describes nothing to run. Identity here is
        key + name + entrypoint: what it is, what to call it, what it runs."""
        self._project(
            tmp_path / "data",
            '[cloud]\n\n[[cloud.job]]\nkey = "x"\nname = "a"\n'
            'entrypoint = "model.preql"\n',
        )
        assert [p.name for p in cloud_mod.discover_projects(tmp_path)] == ["a"]

    def test_an_entry_without_an_entrypoint_is_refused(self, tmp_path):
        """Jobs sharing a workspace are distinguished by the script they run
        and nothing else, so an entry that does not say is not a job."""
        self._project(
            tmp_path / "data", '[cloud]\n\n[[cloud.job]]\nkey = "x"\nname = "a"\n'
        )
        with pytest.raises(CloudError, match="must declare an entrypoint"):
            cloud_mod.discover_projects(tmp_path)

    def test_an_empty_job_array_is_refused(self, tmp_path):
        self._project(tmp_path / "data", "[cloud]\njob = []\n")
        with pytest.raises(CloudError, match="declares no jobs"):
            cloud_mod.discover_projects(tmp_path)

    def test_siblings_from_one_toml_are_not_nested_projects(self, tmp_path):
        """They share a directory on purpose; the nesting guard is about one
        project's bundle swallowing another's."""
        self._project(tmp_path / "data", self.TWO_JOBS)
        assert len(cloud_mod.discover_projects(tmp_path)) == 2

    def test_a_tests_directory_inside_the_root_is_excluded(self, tmp_path):
        self._project(tmp_path / "tests" / "fixture", '[cloud]\noperation = "run"\n')
        self._project(tmp_path / "etl", '[cloud]\noperation = "run"\n')
        assert [p.name for p in cloud_mod.discover_projects(tmp_path)] == ["etl"]

    def test_an_excluded_project_says_so_rather_than_vanishing(self, tmp_path, capsys):
        """A declared deployment that silently does not deploy is the worst
        outcome available — worse than deploying one that should not."""
        self._project(tmp_path / "tests" / "fixture", '[cloud]\noperation = "run"\n')
        cloud_mod.discover_projects(tmp_path)
        assert "Skipping" in capsys.readouterr().err

    def test_the_checkout_location_does_not_exclude_anything(self, tmp_path):
        """Excludes describe positions *inside* a project. Matched against the
        absolute path they would also match whatever the repo happens to live
        under, so a clone in ~/test_models would deploy nothing."""
        root = tmp_path / "test_models" / "tests"
        self._project(root / "etl", '[cloud]\noperation = "run"\n')
        assert [p.name for p in cloud_mod.discover_projects(root)] == ["etl"]

    def test_a_project_inside_another_project_is_refused(self, tmp_path):
        """The outer bundle would contain the inner tree and both would deploy,
        running the same files twice under two names."""
        self._project(tmp_path / "etl", '[cloud]\noperation = "run"\n')
        self._project(tmp_path / "etl" / "inner", '[cloud]\noperation = "run"\n')
        with pytest.raises(CloudError, match="inside"):
            cloud_mod.discover_projects(tmp_path)

    def test_a_sibling_project_is_not_nesting(self, tmp_path):
        self._project(tmp_path / "etl", '[cloud]\noperation = "run"\n')
        self._project(tmp_path / "etl_two", '[cloud]\noperation = "run"\n')
        assert len(cloud_mod.discover_projects(tmp_path)) == 2

    def test_a_declared_name_wins_over_the_path(self, tmp_path):
        """The path says `data` for a project that is its own repository, which
        is the case the declaration exists for."""
        self._project(
            tmp_path / "data",
            '[cloud]\nname = "urban-tree-data"\noperation = "refresh"\n',
        )
        found = cloud_mod.discover_projects(tmp_path)
        assert [p.name for p in found] == ["urban-tree-data"]

    def test_a_declared_name_does_not_move_the_identity(self, tmp_path):
        """Renaming must be safe: `source_key` is what a sync upserts and
        prunes on, so a rename has to update the job rather than deploy a
        second one."""
        directory = self._project(tmp_path / "data", '[cloud]\noperation = "run"\n')
        before = cloud_mod.discover_projects(tmp_path)[0]
        (directory / "trilogy.toml").write_text(
            '[cloud]\nname = "urban-tree-data"\noperation = "run"\n', encoding="utf-8"
        )
        after = cloud_mod.discover_projects(tmp_path)[0]
        assert (before.name, after.name) == ("data", "urban-tree-data")
        assert before.source_key == after.source_key

    def test_two_projects_may_not_deploy_under_one_name(self, tmp_path):
        """The collision the derived name could not have: both would deploy
        (identity keeps them apart) and be indistinguishable everywhere a name
        is what you read."""
        self._project(tmp_path / "a", '[cloud]\nname = "reports"\noperation = "run"\n')
        self._project(tmp_path / "b", '[cloud]\nname = "reports"\noperation = "run"\n')
        with pytest.raises(CloudError, match="reports"):
            cloud_mod.discover_projects(tmp_path)

    def test_a_declared_name_may_match_another_projects_derived_one(self, tmp_path):
        """Still a collision — where it came from is not the point."""
        self._project(tmp_path / "etl", '[cloud]\noperation = "run"\n')
        self._project(tmp_path / "b", '[cloud]\nname = "etl"\noperation = "run"\n')
        with pytest.raises(CloudError, match="etl"):
            cloud_mod.discover_projects(tmp_path)


class TestCloudSync:
    """`sync` end to end against the fake API."""

    ENV: ClassVar[dict] = {
        "id": "env-1",
        "org_id": "org-acme",
        "name": "feature_x_a1b2c3",
        "is_default": False,
        "created_at": TS,
        "updated_at": TS,
    }

    def _repo(self, tmp_path: Path, **settings: str) -> Path:
        directory = tmp_path / "etl"
        directory.mkdir(parents=True)
        body = "[cloud]\n" + "".join(f"{k} = {v}\n" for k, v in settings.items())
        (directory / "trilogy.toml").write_text(body, encoding="utf-8")
        (directory / "model.preql").write_text("key id int;", encoding="utf-8")
        return tmp_path

    def test_a_new_project_is_created_in_production_on_a_default_branch(
        self, logged_in, run_cloud, tmp_path
    ):
        root = self._repo(tmp_path, operation='"refresh"')
        logged_in.set("GET", f"/orgs/{logged_in.org}/jobs", [])
        result = run_cloud("sync", str(root))
        assert result.exit_code == 0, result.output
        assert "production" in result.output
        created = logged_in.requests_for("POST", f"/orgs/{logged_in.org}/jobs")[0]
        assert created["source_key"] == cloud_mod.discover_projects(root)[0].source_key
        assert created["operation"] == "refresh"
        # No environment row is created or referenced for production.
        assert created["environment_id"] is None

    def test_dry_run_writes_nothing(self, logged_in, run_cloud, tmp_path):
        root = self._repo(tmp_path, operation='"run"')
        logged_in.set("GET", f"/orgs/{logged_in.org}/jobs", [])
        result = run_cloud("sync", str(root), "--dry-run")
        assert result.exit_code == 0, result.output
        assert "Would sync" in result.output
        assert not logged_in.requests_for("POST", f"/orgs/{logged_in.org}/jobs")

    def test_a_matching_source_key_updates_rather_than_duplicating(
        self, logged_in, run_cloud, tmp_path
    ):
        root = self._repo(tmp_path, operation='"run"')
        key = cloud_mod.discover_projects(root)[0].source_key
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/jobs",
            [_job_payload("job-1", "etl", source_key=key)],
        )
        result = run_cloud("sync", str(root))
        assert result.exit_code == 0, result.output
        assert not logged_in.requests_for("POST", f"/orgs/{logged_in.org}/jobs")
        assert logged_in.requests_for("PUT", f"/orgs/{logged_in.org}/jobs/job-1")

    def test_a_declared_name_renames_the_job_it_already_deployed(
        self, logged_in, run_cloud, tmp_path
    ):
        """The rename path end to end: matched by `source_key`, so it is a PUT
        on the existing job carrying the new name — not a second job, and not a
        payload the server has to guess at."""
        root = self._repo(tmp_path, name='"urban-tree-data"', operation='"run"')
        key = cloud_mod.discover_projects(root)[0].source_key
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/jobs",
            [_job_payload("job-1", "etl", source_key=key)],
        )
        assert run_cloud("sync", str(root)).exit_code == 0
        assert not logged_in.requests_for("POST", f"/orgs/{logged_in.org}/jobs")
        put = logged_in.requests_for("PUT", f"/orgs/{logged_in.org}/jobs/job-1")[0]
        assert put["name"] == "urban-tree-data"

    def test_an_api_that_cannot_rename_says_so(self, logged_in, run_cloud, tmp_path):
        """An API older than renameable jobs ignores the name and answers with
        the one it has. Silence there is a sync reporting `updated` forever
        while the name never moves."""
        root = self._repo(tmp_path, name='"urban-tree-data"', operation='"run"')
        key = cloud_mod.discover_projects(root)[0].source_key
        old = _job_payload("job-1", "etl", source_key=key)
        logged_in.set("GET", f"/orgs/{logged_in.org}/jobs", [old])
        logged_in.set("PUT", f"/orgs/{logged_in.org}/jobs/job-1", old)
        result = run_cloud("sync", str(root))
        assert result.exit_code == 0, result.output
        assert "not renamed" in result.output

    def test_a_job_without_a_source_key_is_not_adopted(
        self, logged_in, run_cloud, tmp_path
    ):
        """Hand-created jobs are moved deliberately, not matched by name — an
        automatic one-shot migration is debt that runs once."""
        root = self._repo(tmp_path, operation='"run"')
        logged_in.set(
            "GET", f"/orgs/{logged_in.org}/jobs", [_job_payload("job-1", "etl")]
        )
        assert run_cloud("sync", str(root)).exit_code == 0
        assert logged_in.requests_for("POST", f"/orgs/{logged_in.org}/jobs")

    def test_a_declared_schedule_is_created(self, logged_in, run_cloud, tmp_path):
        root = self._repo(tmp_path, operation='"refresh"', schedule='"0 0 7 * * *"')
        logged_in.set("GET", f"/orgs/{logged_in.org}/jobs", [])
        logged_in.set("GET", f"/orgs/{logged_in.org}/schedules", [])
        assert run_cloud("sync", str(root)).exit_code == 0
        posted = logged_in.requests_for("POST", f"/orgs/{logged_in.org}/schedules")
        assert posted and posted[0]["cron_expr"] == "0 0 7 * * *"

    def test_an_unchanged_schedule_is_left_alone(self, logged_in, run_cloud, tmp_path):
        """Delete+create on every sync would churn next_run_at and could skip a
        tick."""
        root = self._repo(tmp_path, operation='"refresh"', schedule='"0 0 7 * * *"')
        logged_in.set("GET", f"/orgs/{logged_in.org}/jobs", [])
        # The schedule is named after the job the *server* answered with, which
        # is authoritative on the name — so the seed has to agree with it.
        logged_in.set(
            "POST", f"/orgs/{logged_in.org}/jobs", _job_payload("job-new", "etl")
        )
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/schedules",
            [_schedule_payload("etl-schedule", "0 0 7 * * *")],
        )
        assert run_cloud("sync", str(root)).exit_code == 0
        assert not logged_in.requests_for("POST", f"/orgs/{logged_in.org}/schedules")
        assert not logged_in.requests_for(
            "DELETE", f"/orgs/{logged_in.org}/schedules/sched-1"
        )

    def test_an_explicit_environment_is_created_and_used(
        self, logged_in, run_cloud, tmp_path
    ):
        root = self._repo(tmp_path, operation='"run"')
        logged_in.set("GET", f"/orgs/{logged_in.org}/jobs", [])
        logged_in.set("GET", f"/orgs/{logged_in.org}/environments", [])
        logged_in.set("POST", f"/orgs/{logged_in.org}/environments", self.ENV)
        result = run_cloud("sync", str(root), "--environment", "feature_x_a1b2c3")
        assert result.exit_code == 0, result.output
        assert "feature_x_a1b2c3" in result.output
        created = logged_in.requests_for("POST", f"/orgs/{logged_in.org}/jobs")[0]
        assert created["environment_id"] == "env-1"

    def test_an_explicit_environment_that_exists_is_targeted_not_recreated(
        self, logged_in, run_cloud, tmp_path
    ):
        """The create route is idempotent, so an existing environment comes
        back as itself and its jobs are what the sync upserts against."""
        root = self._repo(tmp_path, operation='"run"')
        key = cloud_mod.discover_projects(root)[0].source_key
        logged_in.set("POST", f"/orgs/{logged_in.org}/environments", self.ENV)
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/jobs",
            [_job_payload("job-1", "etl", source_key=key, environment_id="env-1")],
        )
        result = run_cloud("sync", str(root), "--environment", "feature_x_a1b2c3")
        assert result.exit_code == 0, result.output
        assert logged_in.requests_for("PUT", f"/orgs/{logged_in.org}/jobs/job-1")
        assert not logged_in.requests_for("POST", f"/orgs/{logged_in.org}/jobs")

    @pytest.mark.parametrize("flag", [("--production",), ("--environment", "")])
    def test_production_updates_productions_own_jobs(
        self, logged_in, run_cloud, tmp_path, flag
    ):
        """Both spellings of production, from a checkout whose branch would
        otherwise derive an environment of its own."""
        root = self._repo(tmp_path, operation='"run"')
        key = cloud_mod.discover_projects(root)[0].source_key
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/jobs",
            [_job_payload("job-1", "etl", source_key=key)],
        )
        result = run_cloud("sync", str(root), *flag)
        assert result.exit_code == 0, result.output
        assert "production" in result.output
        assert logged_in.requests_for("PUT", f"/orgs/{logged_in.org}/jobs/job-1")
        assert not logged_in.requests_for("POST", f"/orgs/{logged_in.org}/jobs")
        assert not logged_in.requests_for("POST", f"/orgs/{logged_in.org}/environments")

    def test_production_and_an_environment_are_not_both_a_target(
        self, logged_in, run_cloud, tmp_path
    ):
        root = self._repo(tmp_path, operation='"run"')
        result = run_cloud(
            "sync", str(root), "--production", "--environment", "feature_x_a1b2c3"
        )
        assert result.exit_code != 0
        assert not logged_in.requests_for("POST", f"/orgs/{logged_in.org}/environments")

    def test_a_dry_run_against_production_reports_updates_not_creates(
        self, logged_in, run_cloud, tmp_path
    ):
        """The dry run reports the same target the write path resolves."""
        root = self._repo(tmp_path, operation='"run"')
        key = cloud_mod.discover_projects(root)[0].source_key
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/jobs",
            [_job_payload("job-1", "etl", source_key=key)],
        )
        result = run_cloud("sync", str(root), "--production", "--dry-run")
        assert result.exit_code == 0, result.output
        assert "0 to create, 1 to update" in result.output

    @pytest.mark.parametrize("name", ["prod", "production"])
    def test_a_name_that_reads_like_production_is_just_a_name(
        self, logged_in, run_cloud, tmp_path, name
    ):
        """No reserved words: every name `--environment` takes is an ordinary
        environment of that name."""
        root = self._repo(tmp_path, operation='"run"')
        env = {**self.ENV, "name": name}
        logged_in.set("POST", f"/orgs/{logged_in.org}/environments", env)
        logged_in.set("GET", f"/orgs/{logged_in.org}/jobs", [])
        result = run_cloud("sync", str(root), "--environment", name)
        assert result.exit_code == 0, result.output
        assert (
            logged_in.requests_for("POST", f"/orgs/{logged_in.org}/environments")[0][
                "name"
            ]
            == name
        )
        created = logged_in.requests_for("POST", f"/orgs/{logged_in.org}/jobs")[0]
        assert created["environment_id"] == "env-1"

    def test_an_unusable_environment_name_is_refused_not_created(
        self, logged_in, run_cloud, tmp_path
    ):
        """The name prefixes managed tables and suffixes managed files, so it
        has to be an identifier."""
        root = self._repo(tmp_path, operation='"run"')
        logged_in.set("GET", f"/orgs/{logged_in.org}/jobs", [])
        result = run_cloud("sync", str(root), "--environment", "feature/x")
        assert result.exit_code != 0
        assert "env label" in result.output
        assert not logged_in.requests_for("POST", f"/orgs/{logged_in.org}/environments")

    def test_nothing_deployable_is_an_error_not_a_silent_success(
        self, logged_in, run_cloud, tmp_path
    ):
        (tmp_path / "trilogy.toml").write_text("[engine]\n", encoding="utf-8")
        result = run_cloud("sync", str(tmp_path))
        assert result.exit_code != 0
        assert "No deployable projects" in result.output


def _job_payload(job_id: str, name: str, **over) -> dict:
    """The wire shape of a job, for seeding the fake API. `_job` above builds
    the parsed model instead, which the routes cannot serve."""
    return {
        "id": job_id,
        "org_id": "org-acme",
        "name": name,
        "operation": "run",
        "current_version_id": f"{job_id}-v1",
        "environment_id": None,
        "created_at": TS,
        "updated_at": TS,
        **over,
    }


def _schedule_payload(
    name: str,
    cron_expr: str,
    job_names: list[str] | None = None,
    job_ids: list[str] | None = None,
) -> dict:
    """The binding is what matches a schedule to its jobs — by id where the
    API sends them, by name for one that predates the field."""
    return {
        "id": "sched-1",
        "org_id": "org-acme",
        "name": name,
        "cron_expr": cron_expr,
        "is_active": True,
        "next_run_at": TS,
        "created_at": TS,
        "updated_at": TS,
        "job_ids": job_ids or [],
        "job_names": (
            job_names if job_names is not None else [name.removesuffix("-schedule")]
        ),
    }


class TestNonGitProjects:
    """A project with no repository has to work normally.

    Nothing in sync is git-specific: git is the first source provider, the path
    provider is the fallback that always answers, and a directory with no
    branch resolves to production — the same answer main gives.
    """

    def _repo(self, tmp_path: Path) -> Path:
        directory = tmp_path / "etl"
        directory.mkdir(parents=True)
        (directory / "trilogy.toml").write_text(
            '[cloud]\noperation = "run"\n', encoding="utf-8"
        )
        (directory / "model.preql").write_text("key id int;", encoding="utf-8")
        return tmp_path

    def test_a_directory_with_no_repository_still_has_an_identity(self, tmp_path):
        project = cloud_mod.discover_projects(self._repo(tmp_path))[0]
        assert project.origin.kind == "path"
        assert project.source_key.startswith("local:")

    def test_it_syncs_to_production_with_no_environment(
        self, logged_in, run_cloud, tmp_path
    ):
        root = self._repo(tmp_path)
        logged_in.set("GET", f"/orgs/{logged_in.org}/jobs", [])
        result = run_cloud("sync", str(root))
        assert result.exit_code == 0, result.output
        assert "production" in result.output
        # No environment was created: there is no branch to derive one from.
        assert not logged_in.requests_for("POST", f"/orgs/{logged_in.org}/environments")
        assert (
            logged_in.requests_for("POST", f"/orgs/{logged_in.org}/jobs")[0][
                "environment_id"
            ]
            is None
        )

    def test_an_explicit_environment_works_without_any_repository(
        self, logged_in, run_cloud, tmp_path
    ):
        """The manual override: a parallel namespace with no branch behind it."""
        root = self._repo(tmp_path)
        logged_in.set("GET", f"/orgs/{logged_in.org}/jobs", [])
        logged_in.set(
            "POST",
            f"/orgs/{logged_in.org}/environments",
            {
                "id": "env-9",
                "org_id": "org-acme",
                "name": "scratch",
                "is_default": False,
                "created_at": TS,
                "updated_at": TS,
            },
        )
        result = run_cloud("sync", str(root), "--environment", "scratch")
        assert result.exit_code == 0, result.output
        created = logged_in.requests_for("POST", f"/orgs/{logged_in.org}/environments")[
            0
        ]
        # The origin is a path, and that is what gets recorded — not a fake
        # git identity.
        assert created["source_kind"] == "path"
        assert created["source_ref"] is None
        assert (
            logged_in.requests_for("POST", f"/orgs/{logged_in.org}/jobs")[0][
                "environment_id"
            ]
            == "env-9"
        )

    def test_the_label_command_is_silent_outside_a_repository(
        self, run_cloud, tmp_path, monkeypatch
    ):
        monkeypatch.chdir(tmp_path)
        result = run_cloud("env", "label")
        assert result.exit_code == 0
        assert result.output.strip() == ""


class TestScheduleReconciliation:
    """Which schedule a sync owns, and when it owns one at all."""

    def _repo(self, tmp_path: Path, name: str = "etl") -> Path:
        directory = tmp_path / name
        directory.mkdir(parents=True)
        (directory / "trilogy.toml").write_text(
            '[cloud]\noperation = "refresh"\nschedule = "0 0 7 * * *"\n',
            encoding="utf-8",
        )
        (directory / "model.preql").write_text("key id int;", encoding="utf-8")
        return tmp_path

    def _bound(
        self,
        schedule_id: str,
        name: str,
        cron: str,
        job_names: list[str],
        job_ids: list[str] | None = None,
    ):
        payload = {
            "id": schedule_id,
            "org_id": "org-acme",
            "name": name,
            "cron_expr": cron,
            "is_active": True,
            "next_run_at": TS,
            "created_at": TS,
            "updated_at": TS,
            "job_names": job_names,
        }
        # Omitted on purpose by default: that is an API older than `job_ids`,
        # and the name fallback has to keep answering for it.
        if job_ids is not None:
            payload["job_ids"] = job_ids
        return payload

    def test_a_renamed_job_does_not_gain_a_second_schedule(
        self, logged_in, run_cloud, tmp_path
    ):
        """Moving a directory renames the job. Matching schedules by name would
        leave the old one bound to the same job and create another beside it —
        the job would then fire twice a day."""
        root = self._repo(tmp_path, "renamed")
        logged_in.set("GET", f"/orgs/{logged_in.org}/jobs", [])
        logged_in.set(
            "POST", f"/orgs/{logged_in.org}/jobs", _job_payload("job-1", "renamed")
        )
        # The schedule still carries the job's OLD name, but is bound to the job
        # — which the API reports under its current name.
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/schedules",
            [self._bound("sched-1", "oldname-schedule", "0 0 7 * * *", ["renamed"])],
        )
        assert run_cloud("sync", str(root)).exit_code == 0
        assert not logged_in.requests_for("POST", f"/orgs/{logged_in.org}/schedules")

    def test_the_sync_that_renames_a_job_keeps_its_one_schedule(
        self, logged_in, run_cloud, tmp_path
    ):
        """The rename case the binding match exists for. Schedules are listed
        *before* the job is PUT, so on this sync the API still reports the old
        name — matching on it would create a second schedule and fire the job
        twice a day, on the very sync that renamed it."""
        directory = tmp_path / "data"
        directory.mkdir(parents=True)
        (directory / "trilogy.toml").write_text(
            '[cloud]\nname = "urban-tree-data"\noperation = "refresh"\n'
            'schedule = "0 0 7 * * *"\n',
            encoding="utf-8",
        )
        (directory / "model.preql").write_text("key id int;", encoding="utf-8")
        key = cloud_mod.discover_projects(tmp_path)[0].source_key
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/jobs",
            [_job_payload("job-1", "data", source_key=key)],
        )
        logged_in.set(
            "PUT",
            f"/orgs/{logged_in.org}/jobs/job-1",
            _job_payload("job-1", "urban-tree-data", source_key=key),
        )
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/schedules",
            [
                self._bound(
                    "sched-1", "data-schedule", "0 0 7 * * *", ["data"], ["job-1"]
                )
            ],
        )
        assert run_cloud("sync", str(tmp_path)).exit_code == 0
        assert not logged_in.requests_for("POST", f"/orgs/{logged_in.org}/schedules")
        assert not logged_in.requests_for(
            "DELETE", f"/orgs/{logged_in.org}/schedules/sched-1"
        )

    def test_a_schedule_bound_to_another_job_is_not_claimed(
        self, logged_in, run_cloud, tmp_path
    ):
        """The binding is the whole test of ownership: a same-named schedule on
        someone else's job must not be deleted and replaced."""
        root = self._repo(tmp_path)
        logged_in.set("GET", f"/orgs/{logged_in.org}/jobs", [])
        logged_in.set(
            "POST", f"/orgs/{logged_in.org}/jobs", _job_payload("job-1", "etl")
        )
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/schedules",
            [self._bound("sched-1", "etl-schedule", "0 0 7 * * *", ["etl"], ["job-2"])],
        )
        assert run_cloud("sync", str(root)).exit_code == 0
        assert not logged_in.requests_for(
            "DELETE", f"/orgs/{logged_in.org}/schedules/sched-1"
        )
        assert logged_in.requests_for("POST", f"/orgs/{logged_in.org}/schedules")

    def test_a_changed_cron_replaces_the_schedule(self, logged_in, run_cloud, tmp_path):
        """There is no schedule update route, so delete+create is the edit."""
        root = self._repo(tmp_path)
        logged_in.set("GET", f"/orgs/{logged_in.org}/jobs", [])
        logged_in.set(
            "POST", f"/orgs/{logged_in.org}/jobs", _job_payload("job-1", "etl")
        )
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/schedules",
            [self._bound("sched-1", "etl-schedule", "0 0 3 * * *", ["etl"])],
        )
        assert run_cloud("sync", str(root)).exit_code == 0
        assert logged_in.requests_for(
            "DELETE", f"/orgs/{logged_in.org}/schedules/sched-1"
        )
        posted = logged_in.requests_for("POST", f"/orgs/{logged_in.org}/schedules")
        assert posted[0]["cron_expr"] == "0 0 7 * * *"

    def test_a_multi_job_schedule_is_left_alone(self, logged_in, run_cloud, tmp_path):
        """A grouping is someone's deliberate tick identity, not something a
        per-directory config owns."""
        root = self._repo(tmp_path)
        logged_in.set("GET", f"/orgs/{logged_in.org}/jobs", [])
        logged_in.set(
            "POST", f"/orgs/{logged_in.org}/jobs", _job_payload("job-1", "etl")
        )
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/schedules",
            [self._bound("sched-1", "nightly", "0 0 3 * * *", ["etl", "other"])],
        )
        assert run_cloud("sync", str(root)).exit_code == 0
        assert not logged_in.requests_for(
            "DELETE", f"/orgs/{logged_in.org}/schedules/sched-1"
        )
        # It still gets its own schedule, since the grouping is not it.
        assert logged_in.requests_for("POST", f"/orgs/{logged_in.org}/schedules")

    def test_a_branch_environment_gets_no_schedule(
        self, logged_in, run_cloud, tmp_path
    ):
        """Otherwise every open branch runs — and bills — on production's
        cadence from the moment it is created."""
        root = self._repo(tmp_path)
        logged_in.set("GET", f"/orgs/{logged_in.org}/jobs", [])
        logged_in.set("GET", f"/orgs/{logged_in.org}/schedules", [])
        logged_in.set(
            "POST",
            f"/orgs/{logged_in.org}/environments",
            {
                "id": "env-1",
                "org_id": "org-acme",
                "name": "feature_x",
                "is_default": False,
                "created_at": TS,
                "updated_at": TS,
            },
        )
        result = run_cloud("sync", str(root), "--environment", "feature_x")
        assert result.exit_code == 0, result.output
        assert not logged_in.requests_for("POST", f"/orgs/{logged_in.org}/schedules")
        assert "trigger it by hand" in result.output

    def test_removing_the_schedule_from_config_unschedules_the_job(
        self, logged_in, run_cloud, tmp_path
    ):
        directory = tmp_path / "etl"
        directory.mkdir(parents=True)
        (directory / "trilogy.toml").write_text(
            '[cloud]\noperation = "refresh"\n', encoding="utf-8"
        )
        (directory / "model.preql").write_text("key id int;", encoding="utf-8")
        logged_in.set("GET", f"/orgs/{logged_in.org}/jobs", [])
        logged_in.set(
            "POST", f"/orgs/{logged_in.org}/jobs", _job_payload("job-1", "etl")
        )
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/schedules",
            [self._bound("sched-1", "etl-schedule", "0 0 7 * * *", ["etl"])],
        )
        assert run_cloud("sync", str(tmp_path)).exit_code == 0
        assert logged_in.requests_for(
            "DELETE", f"/orgs/{logged_in.org}/schedules/sched-1"
        )
        assert not logged_in.requests_for("POST", f"/orgs/{logged_in.org}/schedules")


class TestSyncPrune:
    """`--prune` is the only thing in sync that deletes, so what it declines to
    touch matters more than what it removes."""

    def _repo(self, root: Path) -> Path:
        directory = root / "etl"
        directory.mkdir(parents=True)
        (directory / "trilogy.toml").write_text(
            '[cloud]\noperation = "run"\n', encoding="utf-8"
        )
        (directory / "model.preql").write_text("key id int;", encoding="utf-8")
        return root

    def _seed(self, api, *jobs: dict) -> None:
        api.set("GET", f"/orgs/{api.org}/jobs", list(jobs))
        api.set("POST", f"/orgs/{api.org}/jobs", _job_payload("job-new", "etl"))
        api.set("GET", f"/orgs/{api.org}/schedules", [])
        api.set("DELETE", f"/orgs/{api.org}/jobs/*", {})

    def _stale_key(self, root: Path) -> str:
        """A key in the same repository as ROOT's project, for a directory that
        no longer exists."""
        live = cloud_mod.discover_projects(root)[0].source_key
        return f"{live.split('#', 1)[0]}#gone"

    def test_a_job_whose_directory_is_gone_is_deleted(
        self, logged_in, run_cloud, tmp_path
    ):
        root = self._repo(tmp_path)
        self._seed(
            logged_in,
            _job_payload("job-gone", "gone", source_key=self._stale_key(root)),
        )
        result = run_cloud("sync", str(root), "--prune")
        assert result.exit_code == 0, result.output
        assert logged_in.requests_for("DELETE", f"/orgs/{logged_in.org}/jobs/job-gone")
        assert "1 pruned" in result.output

    def test_another_repositorys_jobs_are_not_pruned(
        self, logged_in, run_cloud, tmp_path
    ):
        """An org routinely holds jobs synced from several repositories into one
        environment. Pruning on "not in this root" alone deletes all of them."""
        root = self._repo(tmp_path)
        self._seed(
            logged_in,
            _job_payload(
                "job-other", "other", source_key="github.com/acme/other-repo#etl"
            ),
        )
        result = run_cloud("sync", str(root), "--prune")
        assert result.exit_code == 0, result.output
        assert not logged_in.requests_for(
            "DELETE", f"/orgs/{logged_in.org}/jobs/job-other"
        )
        assert "0 pruned" in result.output

    def test_a_job_in_another_environment_is_not_pruned(
        self, logged_in, run_cloud, tmp_path
    ):
        root = self._repo(tmp_path)
        self._seed(
            logged_in,
            _job_payload(
                "job-branch",
                "etl",
                source_key=self._stale_key(root),
                environment_id="env-1",
            ),
        )
        assert run_cloud("sync", str(root), "--prune").exit_code == 0
        assert not logged_in.requests_for(
            "DELETE", f"/orgs/{logged_in.org}/jobs/job-branch"
        )

    def test_nothing_is_pruned_without_the_flag(self, logged_in, run_cloud, tmp_path):
        root = self._repo(tmp_path)
        self._seed(
            logged_in,
            _job_payload("job-gone", "gone", source_key=self._stale_key(root)),
        )
        assert run_cloud("sync", str(root)).exit_code == 0
        assert not logged_in.requests_for(
            "DELETE", f"/orgs/{logged_in.org}/jobs/job-gone"
        )

    def test_a_dry_run_reports_a_prune_without_doing_it(
        self, logged_in, run_cloud, tmp_path
    ):
        root = self._repo(tmp_path)
        self._seed(
            logged_in,
            _job_payload("job-gone", "gone", source_key=self._stale_key(root)),
        )
        result = run_cloud("sync", str(root), "--prune", "--dry-run")
        assert result.exit_code == 0, result.output
        assert "would prune" in result.output
        assert not logged_in.requests_for(
            "DELETE", f"/orgs/{logged_in.org}/jobs/job-gone"
        )


class TestSyncSchedulesDeclaredJobsTogether:
    """Several jobs declared by one toml on one cron share a schedule **row**.

    This is the seam between the CLI and the platform's schedule ordering: a
    schedule is what the platform fires as a single tick, and a tick is what it
    orders by what each job reads and writes. Two rows would put a refresh and
    the jobs that publish its output in separate firings with nothing between
    them but wall clock — the arrangement ordering exists to replace, arrived
    at by accident.
    """

    TOML = """
[cloud]

[[cloud.job]]
key = "refresh"
name = "space-refresh"
entrypoint = "refresh.preql"
operation = "refresh"
schedule = "0 0 6 * * *"

[[cloud.job]]
key = "publish"
name = "space-publish"
entrypoint = "publish.preql"
operation = "run"
schedule = "0 0 6 * * *"
"""

    def _repo(self, root: Path, toml: str | None = None) -> Path:
        directory = root / "data"
        directory.mkdir(parents=True)
        (directory / "trilogy.toml").write_text(toml or self.TOML, encoding="utf-8")
        (directory / "model.preql").write_text("key id int;", encoding="utf-8")
        return root

    def _seed(self, api, schedules: list[dict] | None = None) -> None:
        # A multi-job toml deploys a workspace first, then binds its jobs to
        # it — so a sync of this shape talks to the workspace routes before it
        # touches a job at all.
        api.set("GET", f"/orgs/{api.org}/workspaces", [])
        api.set(
            "POST",
            f"/orgs/{api.org}/workspaces",
            {"id": "ws-1", "org_id": "org-acme", "name": "data"},
        )
        api.set(
            "PUT",
            f"/orgs/{api.org}/workspaces/*",
            {"id": "ws-1", "org_id": "org-acme", "name": "data"},
        )
        api.set("PATCH", f"/orgs/{api.org}/jobs/*", {})
        api.set("GET", f"/orgs/{api.org}/jobs", [])
        # Distinct ids per create, so "which jobs did the schedule bind" is a
        # question the assertions can actually ask.
        api.set_steps(
            "POST",
            f"/orgs/{api.org}/jobs",
            [
                _job_payload("job-refresh", "space-refresh"),
                _job_payload("job-publish", "space-publish"),
            ],
        )
        api.set("GET", f"/orgs/{api.org}/schedules", schedules or [])
        api.set(
            "POST",
            f"/orgs/{api.org}/schedules",
            _schedule_payload("space-refresh-schedule", "0 0 6 * * *"),
        )
        api.set("DELETE", f"/orgs/{api.org}/schedules/*", {})

    def test_the_workspace_holds_the_tree_and_the_jobs_hold_an_entrypoint(
        self, logged_in, run_cloud, tmp_path
    ):
        """The whole point of the arrangement.

        Before entrypoints a job executed its entire workdir, so each one had
        to ship its own disjoint copy of the project and a workspace could not
        hold a shared model layer at all. Now the workspace holds every file
        once and a job carries nothing but the script it runs.
        """
        root = self._repo(tmp_path)
        self._seed(logged_in)
        result = run_cloud("sync", str(root))
        assert result.exit_code == 0, result.output

        workspace = logged_in.body_for("POST", f"/orgs/{logged_in.org}/workspaces")
        assert {f["name"] for f in workspace["files"]} == {"model.preql"}

        jobs = logged_in.requests_for("POST", f"/orgs/{logged_in.org}/jobs")
        assert len(jobs) == 2
        assert all(job["files"] == [] for job in jobs), "jobs ship no files"
        assert {job["entrypoint"] for job in jobs} == {
            "refresh.preql",
            "publish.preql",
        }
        assert all(job["workspace_id"] == "ws-1" for job in jobs)

    def test_an_explicit_production_does_not_namespace_the_workspace(
        self, logged_in, run_cloud, tmp_path
    ):
        """A branch environment suffixes the workspace name so it cannot build
        over production's shared tree. Production takes no suffix."""
        root = self._repo(tmp_path)
        self._seed(logged_in)
        result = run_cloud("sync", str(root), "--production")
        assert result.exit_code == 0, result.output
        assert (
            logged_in.body_for("POST", f"/orgs/{logged_in.org}/workspaces")["name"]
            == "data"
        )

    def test_an_existing_job_is_rebound_into_the_workspace(
        self, logged_in, run_cloud, tmp_path
    ):
        """A job's workspace is identity, so a content PUT leaves it alone —
        which means migrating a project deployed the old way needs an explicit
        rebind, or its jobs stay self-contained beside an unread workspace."""
        root = self._repo(tmp_path)
        self._seed(logged_in)
        live = _job_payload(
            "job-refresh",
            "space-refresh",
            source_key=cloud_mod.discover_projects(root)[0].source_key,
        )
        logged_in.set("GET", f"/orgs/{logged_in.org}/jobs", [live])
        logged_in.set("PUT", f"/orgs/{logged_in.org}/jobs/*", live)

        result = run_cloud("sync", str(root))
        assert result.exit_code == 0, result.output
        assert logged_in.requests_for(
            "PATCH", f"/orgs/{logged_in.org}/jobs/job-refresh"
        )

    def test_a_sync_carries_what_the_workspace_already_holds(
        self, logged_in, run_cloud, tmp_path
    ):
        """A workspace PUT replaces its content wholesale, and this body names
        only the tree — sending it bare cleared the parameters, secrets and
        resource defaults of every workspace a sync touched, silently, on
        every run."""
        root = self._repo(tmp_path)
        self._seed(logged_in)
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/workspaces",
            [
                {
                    "id": "ws-1",
                    "org_id": "org-acme",
                    "name": "data",
                    "parameters": {"region": "eu"},
                    "secret_env": ["SNOWFLAKE_PASSWORD"],
                    "timeout_seconds": 1800,
                }
            ],
        )
        result = run_cloud("sync", str(root))
        assert result.exit_code == 0, result.output
        body = logged_in.body_for("PUT", f"/orgs/{logged_in.org}/workspaces/ws-1")
        assert body["parameters"] == {"region": "eu"}
        assert body["secret_env"] == ["SNOWFLAKE_PASSWORD"]
        assert body["timeout_seconds"] == 1800

    def test_a_single_job_project_still_deploys_self_contained(
        self, logged_in, run_cloud, tmp_path
    ):
        """No siblings to share a tree with, so a workspace would be ceremony
        with no reader — and this is every project deployed before now."""
        root = self._repo(tmp_path, '[cloud]\noperation = "run"\n')
        self._seed(logged_in)
        result = run_cloud("sync", str(root))
        assert result.exit_code == 0, result.output
        assert not logged_in.requests_for("POST", f"/orgs/{logged_in.org}/workspaces")
        job = logged_in.body_for("POST", f"/orgs/{logged_in.org}/jobs")
        assert job.get("entrypoint") is None
        assert {f["name"] for f in job["files"]} == {"model.preql"}

    def test_two_jobs_on_one_cron_get_one_schedule_binding_both(
        self, logged_in, run_cloud, tmp_path
    ):
        root = self._repo(tmp_path)
        self._seed(logged_in)
        result = run_cloud("sync", str(root))
        assert result.exit_code == 0, result.output
        posted = logged_in.requests_for("POST", f"/orgs/{logged_in.org}/schedules")
        assert len(posted) == 1, f"one tick, one schedule row: {posted}"
        assert len(posted[0]["job_ids"]) == 2

    def test_different_crons_stay_different_schedules(
        self, logged_in, run_cloud, tmp_path
    ):
        """Grouping is by declared cadence, not by file: two jobs that asked to
        run at different times are not one tick and must not be forced into
        one."""
        root = self._repo(
            tmp_path,
            """
[cloud]

[[cloud.job]]
key = "refresh"
name = "space-refresh"
entrypoint = "refresh.preql"
operation = "refresh"
schedule = "0 0 6 * * *"

[[cloud.job]]
key = "publish"
name = "space-publish"
entrypoint = "publish.preql"
operation = "run"
schedule = "0 0 7 * * *"
""",
        )
        self._seed(logged_in)
        result = run_cloud("sync", str(root))
        assert result.exit_code == 0, result.output
        posted = logged_in.requests_for("POST", f"/orgs/{logged_in.org}/schedules")
        assert len(posted) == 2
        assert all(len(p["job_ids"]) == 1 for p in posted)

    def test_adding_a_job_replaces_the_schedule_instead_of_duplicating_it(
        self, logged_in, run_cloud, tmp_path
    ):
        """The failure this test exists for was live on dev: yesterday's
        two-job schedule does not match today's three-job group, so with
        exact-set ownership it goes unrecognized, a second schedule is created
        beside it, and the two original jobs fire *twice a tick*.

        Ownership is therefore "binds only jobs this toml declares", which
        recognizes the older row and replaces it.
        """
        root = self._repo(tmp_path)
        yesterday = _schedule_payload(
            "space-refresh-schedule", "0 0 6 * * *", job_names=["space-refresh"]
        )
        yesterday["job_ids"] = ["job-refresh"]
        self._seed(logged_in, schedules=[yesterday])

        result = run_cloud("sync", str(root))
        assert result.exit_code == 0, result.output
        assert logged_in.requests_for(
            "DELETE", f"/orgs/{logged_in.org}/schedules/sched-1"
        ), "the superseded schedule must be removed, not left firing"
        posted = logged_in.requests_for("POST", f"/orgs/{logged_in.org}/schedules")
        assert len(posted) == 1 and len(posted[0]["job_ids"]) == 2

    def test_another_groups_schedule_is_left_alone(
        self, logged_in, run_cloud, tmp_path
    ):
        """Ownership stops at the toml: a schedule binding a job this project
        does not declare is somebody else's grouping."""
        root = self._repo(tmp_path)
        theirs = _schedule_payload(
            "ops-nightly", "0 0 6 * * *", job_names=["space-refresh", "somebody-else"]
        )
        theirs["job_ids"] = ["job-refresh", "job-stranger"]
        self._seed(logged_in, schedules=[theirs])

        result = run_cloud("sync", str(root))
        assert result.exit_code == 0, result.output
        assert not logged_in.requests_for(
            "DELETE", f"/orgs/{logged_in.org}/schedules/sched-1"
        )

    def test_an_unchanged_group_schedule_is_left_alone(
        self, logged_in, run_cloud, tmp_path
    ):
        """A sync that re-posts an identical schedule churns `next_run_at` and
        can skip a tick — the reason the single-job path checks first."""
        root = self._repo(tmp_path)
        existing = _schedule_payload(
            "space-refresh-+1-schedule",
            "0 0 6 * * *",
            job_names=["space-refresh", "space-publish"],
        )
        existing["job_ids"] = ["job-refresh", "job-publish"]
        self._seed(logged_in, schedules=[existing])
        result = run_cloud("sync", str(root))
        assert result.exit_code == 0, result.output
        assert not logged_in.requests_for("POST", f"/orgs/{logged_in.org}/schedules")
        assert not logged_in.requests_for(
            "DELETE", f"/orgs/{logged_in.org}/schedules/sched-1"
        )


class TestSyncCarriesUndeclaredSettings:
    """`None` means "carry what the job has", not "reset to the default".

    A `PUT` replaces content wholesale, so this is the whole reason the
    unspecified state exists — and a field the CLI forgets to carry is cleared
    on every sync, silently, forever.
    """

    def _repo(self, root: Path, body: str) -> Path:
        directory = root / "etl"
        directory.mkdir(parents=True)
        (directory / "trilogy.toml").write_text(body, encoding="utf-8")
        (directory / "model.preql").write_text("key id int;", encoding="utf-8")
        return root

    def _sync_over(self, api, run_cloud, root: Path, **existing) -> dict:
        key = cloud_mod.discover_projects(root)[0].source_key
        api.set(
            "GET",
            f"/orgs/{api.org}/jobs",
            [_job_payload("job-1", "etl", source_key=key, **existing)],
        )
        api.set("PUT", f"/orgs/{api.org}/jobs/job-1", _job_payload("job-1", "etl"))
        api.set("GET", f"/orgs/{api.org}/schedules", [])
        assert run_cloud("sync", str(root)).exit_code == 0
        return api.requests_for("PUT", f"/orgs/{api.org}/jobs/job-1")[0]

    @pytest.mark.parametrize(
        "field,value",
        [
            ("timeout_seconds", 1800),
            ("memory_mb", 2048),
            ("cpus", 2.0),
            ("secret_env", ["GOOGLE_HMAC_KEY"]),
            ("vm_class", "exclusive"),
            ("priority", 1),
            ("deadline_seconds", 600),
            ("description", "the nightly load"),
            ("parameters", {"region": "eu"}),
        ],
    )
    def test_an_undeclared_setting_survives(
        self, logged_in, run_cloud, tmp_path, field, value
    ):
        root = self._repo(tmp_path, '[cloud]\noperation = "run"\n')
        body = self._sync_over(logged_in, run_cloud, root, **{field: value})
        assert body[field] == value

    def test_an_undeclared_operation_survives(self, logged_in, run_cloud, tmp_path):
        """The one field with a platform default, so it is always sent — but
        still never reset to `run` off a job configured to refresh."""
        root = self._repo(tmp_path, "[cloud]\ntimeout_seconds = 60\n")
        body = self._sync_over(logged_in, run_cloud, root, operation="refresh")
        assert body["operation"] == "refresh"

    def test_a_declared_setting_wins_over_the_carried_one(
        self, logged_in, run_cloud, tmp_path
    ):
        root = self._repo(tmp_path, "[cloud]\ntimeout_seconds = 60\n")
        body = self._sync_over(logged_in, run_cloud, root, timeout_seconds=1800)
        assert body["timeout_seconds"] == 60

    def test_every_content_field_on_the_model_is_carried(self):
        """The pin behind all of the above: a content field `Job` models but
        `_carried_settings` omits is cleared on every write, and nothing else
        in the suite would notice."""
        content_fields = set(Job.model_fields) - {
            "id",
            "org_id",
            "name",
            "config",
            "files",
            "schedule",
            "created_at",
            "updated_at",
            "current_version_id",
            "source_fingerprint",
            # Identity, moved by PATCH; a content PUT leaves them alone.
            "workspace_id",
            "environment_id",
            "source_key",
        }
        assert content_fields == set(
            cloud_mod._carried_settings(_job("job-1", "nightly"))
        )


class TestSyncReporting:
    """What the closing line claims happened."""

    def _repo(self, root: Path) -> Path:
        directory = root / "etl"
        directory.mkdir(parents=True)
        (directory / "trilogy.toml").write_text(
            '[cloud]\noperation = "run"\n', encoding="utf-8"
        )
        (directory / "model.preql").write_text("key id int;", encoding="utf-8")
        return root

    def test_a_dry_run_counts_creates_rather_than_calling_them_unchanged(
        self, logged_in, run_cloud, tmp_path
    ):
        """A dry run never sends a PUT, so it cannot know whether one would mint
        a version. Reporting every project as "unchanged" was worse than not
        counting at all."""
        root = self._repo(tmp_path)
        logged_in.set("GET", f"/orgs/{logged_in.org}/jobs", [])
        result = run_cloud("sync", str(root), "--dry-run")
        assert "1 to create, 0 to update" in result.output
        assert "unchanged" not in result.output

    def test_a_dry_run_counts_an_existing_job_as_an_update(
        self, logged_in, run_cloud, tmp_path
    ):
        root = self._repo(tmp_path)
        key = cloud_mod.discover_projects(root)[0].source_key
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/jobs",
            [_job_payload("job-1", "etl", source_key=key)],
        )
        result = run_cloud("sync", str(root), "--dry-run")
        assert "0 to create, 1 to update" in result.output

    def test_an_unchanged_put_is_reported_as_unchanged(
        self, logged_in, run_cloud, tmp_path
    ):
        """The server keeps `current_version_id` when the content matches."""
        root = self._repo(tmp_path)
        key = cloud_mod.discover_projects(root)[0].source_key
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/jobs",
            [_job_payload("job-1", "etl", source_key=key)],
        )
        logged_in.set(
            "PUT", f"/orgs/{logged_in.org}/jobs/job-1", _job_payload("job-1", "etl")
        )
        logged_in.set("GET", f"/orgs/{logged_in.org}/schedules", [])
        result = run_cloud("sync", str(root))
        assert "0 created, 0 updated, 1 unchanged" in result.output

    def test_a_dry_run_still_refuses_an_oversized_bundle(
        self, logged_in, run_cloud, tmp_path, monkeypatch
    ):
        """Exactly what a dry run is for. Skipping the check would let one pass
        cleanly and then fail the sync it was rehearsing."""
        root = self._repo(tmp_path)
        (root / "etl" / "big.preql").write_text("x" * 5000, encoding="utf-8")
        monkeypatch.setattr(cloud_mod, "PUBSUB_MAX_BYTES", 1000)
        logged_in.set("GET", f"/orgs/{logged_in.org}/jobs", [])
        result = run_cloud("sync", str(root), "--dry-run")
        assert result.exit_code != 0
        assert "over the" in result.output

    def test_json_mode_emits_one_sync_event(
        self, logged_in, run_cloud, tmp_path, json_mode
    ):
        root = self._repo(tmp_path)
        logged_in.set("GET", f"/orgs/{logged_in.org}/jobs", [])
        logged_in.set("GET", f"/orgs/{logged_in.org}/schedules", [])
        result = run_cloud("sync", str(root))
        assert result.exit_code == 0, result.output
        event = [e for e in _json_stream(result.output) if e["event"] == "sync"][-1]
        assert "environment" not in event  # production has no row
        assert [j["outcome"] for j in event["jobs"]] == ["created"]
        assert event["jobs"][0]["action"] == "create"


class TestEnvironmentCommands:
    """`cloud env` — the namespace lifecycle around a branch."""

    ENV: ClassVar[dict] = {
        "id": "env-1",
        "org_id": "org-acme",
        "name": "feature_x",
        "is_default": False,
        "source_ref": "feature/x",
        "job_count": 2,
        "created_at": TS,
        "updated_at": TS,
    }

    def test_list_shows_each_environment_and_its_job_count(self, logged_in, run_cloud):
        logged_in.set("GET", f"/orgs/{logged_in.org}/environments", [self.ENV])
        result = run_cloud("env", "list")
        assert result.exit_code == 0, result.output
        assert "feature_x" in result.output and "jobs: 2" in result.output
        assert "feature/x" in result.output

    def test_list_says_where_things_build_when_there_are_none(
        self, logged_in, run_cloud
    ):
        logged_in.set("GET", f"/orgs/{logged_in.org}/environments", [])
        assert "production" in run_cloud("env", "list").output

    def test_list_marks_the_default_environment(self, logged_in, run_cloud):
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/environments",
            [{**self.ENV, "is_default": True, "source_ref": None}],
        )
        assert "(default)" in run_cloud("env", "list").output

    def test_create_is_idempotent_and_reports_what_came_back(
        self, logged_in, run_cloud
    ):
        logged_in.set("POST", f"/orgs/{logged_in.org}/environments", self.ENV)
        result = run_cloud("env", "create", "feature_x", "--source-ref", "feature/x")
        assert result.exit_code == 0, result.output
        body = logged_in.requests_for("POST", f"/orgs/{logged_in.org}/environments")[0]
        assert body == {
            "name": "feature_x",
            "description": None,
            "source_kind": "git",
            "source_location": None,
            "source_ref": "feature/x",
        }
        assert "feature_x" in result.output

    def test_create_without_a_source_claims_no_kind(self, logged_in, run_cloud):
        logged_in.set("POST", f"/orgs/{logged_in.org}/environments", self.ENV)
        run_cloud("env", "create", "scratch")
        body = logged_in.requests_for("POST", f"/orgs/{logged_in.org}/environments")[0]
        assert body["source_kind"] is None

    def test_fork_copies_jobs_and_says_how_many(self, logged_in, run_cloud):
        logged_in.set(
            "POST",
            f"/orgs/{logged_in.org}/environments/default/fork",
            {"jobs_copied": 4},
        )
        result = run_cloud("env", "fork", "default", "feature_x")
        assert result.exit_code == 0, result.output
        assert "4 job(s) copied" in result.output

    def test_delete_resolves_a_name_to_an_id(self, logged_in, run_cloud):
        logged_in.set(
            "GET", f"/orgs/{logged_in.org}/environments", [{**self.ENV, "job_count": 0}]
        )
        logged_in.set("DELETE", f"/orgs/{logged_in.org}/environments/env-1", {})
        result = run_cloud("env", "delete", "feature_x")
        assert result.exit_code == 0, result.output
        assert logged_in.requests_for(
            "DELETE", f"/orgs/{logged_in.org}/environments/env-1"
        )

    def test_delete_of_a_non_empty_environment_needs_a_choice(
        self, logged_in, run_cloud
    ):
        """Deleting the record reparents its jobs into production with their
        schedules, so a non-empty environment asks which is wanted."""
        logged_in.set("GET", f"/orgs/{logged_in.org}/environments", [self.ENV])
        logged_in.set("DELETE", f"/orgs/{logged_in.org}/environments/env-1", {})
        result = run_cloud("env", "delete", "feature_x")
        assert result.exit_code != 0
        assert "--with-jobs" in result.output and "--keep-jobs" in result.output
        assert not logged_in.requests_for(
            "DELETE", f"/orgs/{logged_in.org}/environments/env-1"
        )

    def test_delete_refuses_both_flags_at_once(self, logged_in, run_cloud):
        logged_in.set("GET", f"/orgs/{logged_in.org}/environments", [self.ENV])
        result = run_cloud(
            "env", "delete", "feature_x", "--with-jobs", "--keep-jobs", "--yes"
        )
        assert result.exit_code != 0
        assert not logged_in.requests_for(
            "DELETE", f"/orgs/{logged_in.org}/environments/env-1"
        )

    def test_keep_jobs_confirms_before_moving_them_into_production(
        self, logged_in, run_cloud
    ):
        logged_in.set("GET", f"/orgs/{logged_in.org}/environments", [self.ENV])
        logged_in.set("DELETE", f"/orgs/{logged_in.org}/environments/env-1", {})
        result = run_cloud("env", "delete", "feature_x", "--keep-jobs", input="n\n")
        assert result.exit_code != 0
        assert "production" in result.output
        assert not logged_in.requests_for(
            "DELETE", f"/orgs/{logged_in.org}/environments/env-1"
        )

    def test_keep_jobs_names_the_jobs_it_left_running_in_production(
        self, logged_in, run_cloud
    ):
        """The names collide with the production jobs they were branched from,
        so the report carries ids."""
        logged_in.set("GET", f"/orgs/{logged_in.org}/environments", [self.ENV])
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/jobs",
            [
                _job_payload("job-a", "etl", environment_id="env-1"),
                _job_payload("job-b", "publish", environment_id=None),
            ],
        )
        logged_in.set("DELETE", f"/orgs/{logged_in.org}/environments/env-1", {})
        result = run_cloud("env", "delete", "feature_x", "--keep-jobs", "--yes")
        assert result.exit_code == 0, result.output
        assert "2 job(s) left in production" in result.output
        assert "etl (job-a)" in result.output
        assert "publish" not in result.output
        assert (
            logged_in.call_for(
                "DELETE", f"/orgs/{logged_in.org}/environments/env-1"
            ).query
            == {}
        )

    def test_delete_with_jobs_confirms_first(self, logged_in, run_cloud):
        logged_in.set("GET", f"/orgs/{logged_in.org}/environments", [self.ENV])
        logged_in.set("DELETE", f"/orgs/{logged_in.org}/environments/env-1", {})
        result = run_cloud("env", "delete", "feature_x", "--with-jobs", input="n\n")
        assert result.exit_code != 0
        assert not logged_in.requests_for(
            "DELETE", f"/orgs/{logged_in.org}/environments/env-1"
        )

    def test_delete_with_jobs_and_yes_cascades(self, logged_in, run_cloud):
        logged_in.set("GET", f"/orgs/{logged_in.org}/environments", [self.ENV])
        logged_in.set(
            "DELETE", f"/orgs/{logged_in.org}/environments/env-1", {"jobs_deleted": 2}
        )
        result = run_cloud("env", "delete", "feature_x", "--with-jobs", "--yes")
        assert result.exit_code == 0, result.output
        assert logged_in.call_for(
            "DELETE", f"/orgs/{logged_in.org}/environments/env-1"
        ).query == {"cascade": ["jobs"]}
        assert "and 2 job(s)" in result.output

    def test_an_unknown_environment_lists_the_known_ones(self, logged_in, run_cloud):
        logged_in.set("GET", f"/orgs/{logged_in.org}/environments", [self.ENV])
        result = run_cloud("env", "delete", "nope")
        assert result.exit_code != 0
        assert "feature_x" in result.output

    def test_label_prints_the_label_for_a_named_branch(self, run_cloud):
        """Local, and deliberately unauthenticated: a CI teardown step runs it
        before it has a token, or without needing one."""
        result = run_cloud("env", "label", "feature/x")
        assert result.exit_code == 0
        assert result.output.strip() == environment_label("feature/x")

    def test_label_is_silent_on_a_default_branch(self, run_cloud):
        """ "This is main, there is no environment" is an answer, not a failure —
        a teardown step should not fail the workflow over it."""
        result = run_cloud("env", "label", "main")
        assert result.exit_code == 0
        assert result.output.strip() == ""


class TestDeployKeysArePinned:
    def test_the_config_auditor_accepts_every_deployment_key(self):
        """`execution.config` restates these rather than importing them, to keep
        click and pydantic off the engine's import path. Without this pin, a new
        setting makes a valid trilogy.toml audit as an unknown key."""
        from trilogy.execution.config import _KNOWN_SECTIONS

        assert _KNOWN_SECTIONS["cloud"] == {"api_url", "org", JOB_ARRAY_KEY} | set(
            DEPLOY_KEYS
        )

    def test_a_job_entry_accepts_every_deployment_key_and_no_deployment_key(self):
        """An entry of the `[[cloud.job]]` array is a `[cloud]` block minus the
        two keys that address a *deployment* rather than describe a job."""
        from trilogy.execution.config import _KNOWN_SECTIONS

        # `workspace` is block-level: there is one per toml, holding the tree
        # its jobs share, so an entry declaring its own would be declaring a
        # workspace nobody else is in.
        assert _KNOWN_SECTIONS[f"cloud.{JOB_ARRAY_KEY}"] == set(DEPLOY_KEYS) - {
            "workspace"
        }

    def test_the_keys_are_the_settings(self):
        assert set(DEPLOY_KEYS) == set(
            DeploySettings(schedule="x", operation="run").__dataclass_fields__
        )


class TestDryRunWritesNothing:
    """`--dry-run` must not leave anything behind. Creating the branch
    environment is a write, and it is the easiest one to do by accident —
    resolving the target and creating it are the same call on the live path."""

    def _repo(self, tmp_path: Path) -> Path:
        directory = tmp_path / "etl"
        directory.mkdir(parents=True)
        (directory / "trilogy.toml").write_text(
            '[cloud]\noperation = "run"\n', encoding="utf-8"
        )
        (directory / "model.preql").write_text("key id int;", encoding="utf-8")
        return tmp_path

    def test_it_creates_no_environment(self, logged_in, run_cloud, tmp_path):
        root = self._repo(tmp_path)
        logged_in.set("GET", f"/orgs/{logged_in.org}/jobs", [])
        logged_in.set("GET", f"/orgs/{logged_in.org}/environments", [])
        result = run_cloud("sync", str(root), "--environment", "scratch", "--dry-run")
        assert result.exit_code == 0, result.output
        assert not logged_in.requests_for("POST", f"/orgs/{logged_in.org}/environments")

    def test_an_absent_environment_is_not_compared_against_production(
        self, logged_in, run_cloud, tmp_path
    ):
        """Falling back to `environment_id is None` would diff the branch's
        projects against production's jobs and report updates it would never
        make."""
        root = self._repo(tmp_path)
        logged_in.set("GET", f"/orgs/{logged_in.org}/environments", [])
        # A production job whose source_key matches what this project derives.
        key = cloud_mod.discover_projects(root)[0].source_key
        logged_in.set(
            "GET",
            f"/orgs/{logged_in.org}/jobs",
            [_job_payload("job-1", "etl", source_key=key)],
        )
        result = run_cloud("sync", str(root), "--environment", "scratch", "--dry-run")
        assert result.exit_code == 0, result.output
        assert "would create" in result.output
        assert "would update" not in result.output
