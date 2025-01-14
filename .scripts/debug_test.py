import subprocess
import sys


def run_command(command, capture_output=False):
    """Runs a shell command and handles errors."""
    try:
        if capture_output:
            result = subprocess.run(
                command,
                check=True,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            return result.stdout.strip()
        else:
            subprocess.run(command, check=True, text=True)
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {' '.join(command)}")
        print(e.stderr if e.stderr else str(e))
        sys.exit(1)


def revert_changes():
    """Revert any uncommitted changes in the working directory."""
    print("Reverting any uncommitted changes...")
    run_command(["git", "restore", "--staged", "."])  # Unstage any staged changes
    run_command(["git", "restore", "."])  # Restore modified files
    run_command(["git", "clean", "-fd"])  # Remove untracked files and directories


def main():
    if len(sys.argv) != 2:
        print("Usage: python rerun_test.py <test_name>")
        sys.exit(1)

    test_name = sys.argv[1]

    # Get the current branch name
    current_branch = run_command(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"], capture_output=True
    )
    print(f"Current branch: {current_branch}")

    try:
        # Run the specified test on the current branch
        print(f"Running test '{test_name}' on branch '{current_branch}'...")
        run_command(["pytest", test_name])

        # Revert any changes made by the test
        revert_changes()

        # Checkout main branch
        print("Checking out the 'main' branch...")
        run_command(["git", "checkout", "main"])

        # Run the same test on the main branch
        print(f"Running test '{test_name}' on branch 'main'...")
        run_command(["pytest", test_name])

        # Revert any changes made by the test
        revert_changes()

    finally:
        # Checkout back to the original branch
        print(f"Returning to the original branch '{current_branch}'...")
        run_command(["git", "checkout", current_branch])
        revert_changes()  # Ensure the original branch is clean too

    print("Done!")


if __name__ == "__main__":
    main()
