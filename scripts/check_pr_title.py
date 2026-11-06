"""Check that a pull request title is a conventional commit subject.

Squash merges use the pull request title as the commit message, and release-please
reads those messages to pick the version bump and write the changelog.

The title is read from the ``PR_TITLE`` environment variable so it is never
interpolated into a shell command.
"""

import os
import re
import sys

TITLE_PATTERN = re.compile(
    r"(feat|fix|perf|refactor|docs|test|chore|ci|build|revert)(\([a-z0-9._/-]+\))?!?: \S.*"
)


def is_conventional(title: str) -> bool:
    """Return whether ``title`` is a conventional commit subject."""
    return TITLE_PATTERN.fullmatch(title) is not None


def main() -> int:
    title = os.environ.get("PR_TITLE", "")
    if is_conventional(title):
        return 0
    print(f"Pull request title is not a conventional commit subject: {title!r}")
    print("Expected 'type(scope)!: description', for example 'feat: add a Redis store'.")
    print("Types: feat, fix, perf, refactor, docs, test, chore, ci, build, revert.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
