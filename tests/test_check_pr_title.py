import pytest

from scripts.check_pr_title import is_conventional


@pytest.mark.parametrize(
    "title",
    [
        "feat: add a Redis store",
        "feat!: drop the old API",
        "fix(scope): handle empty bodies",
        "fix(worker/retry)!: change the default",
        "chore(master): release 0.1.0",
        "docs: explain releasing",
    ],
)
def test_accepts_conventional_titles(title: str) -> None:
    assert is_conventional(title)


@pytest.mark.parametrize(
    "title",
    [
        "",
        "Update stuff",
        "feat:no space",
        "feat: ",
        "feature: add thing",
        "Feat: capitalised",
        "feat(Scope): uppercase scope",
        "feat: one\nfix: two",
    ],
)
def test_rejects_other_titles(title: str) -> None:
    assert not is_conventional(title)
