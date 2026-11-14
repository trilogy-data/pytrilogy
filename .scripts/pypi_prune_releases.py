# /// script
# requires-python = ">=3.10"
# dependencies = ["playwright", "packaging"]
# ///
"""Delete a range of PyPI releases by driving the manage UI (PyPI has no delete API).

You log in (and 2FA / re-auth) by hand in the opened browser; the script does the rest.

    uv run .scripts/pypi_prune_releases.py --first 0.3.289 --last 0.3.394          # dry run
    uv run .scripts/pypi_prune_releases.py --first 0.3.289 --last 0.3.394 --execute
"""

import argparse
import json
import sys
import time
import urllib.request
from pathlib import Path

from packaging.version import Version
from playwright.sync_api import Page, sync_playwright

PYPI = "https://pypi.org"
PROFILE = Path.home() / ".pypi-prune-profile"

FILL_AND_SUBMIT = """(version) => {
  const input = document.querySelector('input[name="confirm_delete_version"]');
  if (!input) return false;
  const form = input.closest('form');
  input.value = version;
  form.querySelectorAll('input[type="checkbox"]').forEach(c => { c.checked = true; });
  form.submit();
  return true;
}"""


def target_versions(project: str, first: str, last: str) -> list[tuple[str, int]]:
    with urllib.request.urlopen(f"{PYPI}/pypi/{project}/json") as resp:
        releases = json.load(resp)["releases"]
    lo, hi = Version(first), Version(last)
    picked = [v for v in releases if lo <= Version(v) <= hi]
    picked.sort(key=Version)
    return [(v, sum(f["size"] for f in releases[v])) for v in picked]


def release_url(project: str, version: str) -> str:
    return f"{PYPI}/manage/project/{project}/release/{version}/"


def is_reauth(page: Page) -> bool:
    return page.locator('input[name="password"]').count() > 0


def await_reauth(page: Page) -> None:
    """Block until the user submits PyPI's password prompt and its redirect lands."""
    if not is_reauth(page):
        return
    print(
        "\n>>> PyPI wants your password again - enter it in the browser (no need to touch the terminal)."
    )
    page.locator('input[name="password"]').wait_for(state="detached", timeout=0)
    page.wait_for_load_state()
    print(">>> Re-auth done, continuing.")


def delete_release(page: Page, project: str, version: str) -> bool:
    url = release_url(project, version)
    resp = page.goto(url)
    if resp is not None and resp.status == 404:
        return True
    await_reauth(page)
    if page.url != url:
        page.goto(url)
    with page.expect_navigation():
        if not page.evaluate(FILL_AND_SUBMIT, version):
            print(f"  ! no delete form on {url}")
            return False
    # a POST re-auth replays the delete itself after the password is accepted
    await_reauth(page)
    return page.request.get(url).status == 404


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--project", default="pytrilogy")
    p.add_argument(
        "--first", required=True, help="lowest version to delete (inclusive)"
    )
    p.add_argument(
        "--last", required=True, help="highest version to delete (inclusive)"
    )
    p.add_argument("--execute", action="store_true", help="actually delete")
    p.add_argument(
        "--channel",
        default="msedge" if sys.platform == "win32" else "chrome",
        help="installed browser to drive (msedge/chrome); '' for bundled chromium",
    )
    p.add_argument("--delay", type=float, default=1.0, help="seconds between deletions")
    args = p.parse_args()

    targets = target_versions(args.project, args.first, args.last)
    total = sum(size for _, size in targets)
    print(
        f"{len(targets)} releases of {args.project} in [{args.first}, {args.last}], "
        f"{total / 1e9:.2f} GB:"
    )
    print("  " + ", ".join(v for v, _ in targets))
    if not targets or not args.execute:
        print("\nDry run. Re-run with --execute to delete.")
        return 0
    if (
        input(f"\nType DELETE to permanently remove these {len(targets)} releases: ")
        != "DELETE"
    ):
        print("Aborted.")
        return 1

    with sync_playwright() as pw:
        ctx = pw.chromium.launch_persistent_context(
            PROFILE, headless=False, channel=args.channel or None
        )
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        releases_path = f"/manage/project/{args.project}/releases/"
        page.goto(f"{PYPI}/account/login/?next={releases_path}")
        print("Log in (incl. 2FA) in the browser window; waiting...")
        page.wait_for_url(f"**{releases_path}", timeout=0)

        failed: list[str] = []
        streak = 0
        for i, (version, size) in enumerate(targets, 1):
            ok = delete_release(page, args.project, version)
            print(
                f"[{i}/{len(targets)}] {version} ({size / 1e6:.0f} MB): "
                f"{'deleted' if ok else 'FAILED'}"
            )
            if not ok:
                failed.append(version)
                streak += 1
                if streak >= 3:
                    print("3 failures in a row - stopping. Check the browser.")
                    break
            else:
                streak = 0
            time.sleep(args.delay)
        ctx.close()

    if failed:
        print(f"\nFailed: {', '.join(failed)} (re-run the same command to retry)")
        return 1
    print("\nAll done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
