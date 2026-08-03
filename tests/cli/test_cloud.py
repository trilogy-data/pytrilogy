"""Tests for the `trilogy cloud` command: environment resolution, credential
storage, project bundling, response parsing, and every subcommand end to end.

Commands run in-process against the fake API in ``conftest.py``, which replaces
the module's ``urlopen`` — so a command test covers request construction, auth
headers, status handling, model validation, and rendering, rather than only the
command body."""

import json
from datetime import datetime, timezone
from pathlib import Path

import click
import pytest

from trilogy.scripts import cloud as cloud_mod
from trilogy.scripts.cloud import (
    DEFAULT_API_URL,
    CloudClient,
    CloudError,
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
from trilogy.scripts.source_identity import SOURCE_FINGERPRINT_VERSION, content_digest

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
