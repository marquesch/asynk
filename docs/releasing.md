# Releasing

Releases are automated with [release-please](https://github.com/googleapis/release-please).
Nobody edits the version or `CHANGELOG.md` by hand. Everything is driven by the titles
of merged pull requests.

## Pull request titles and merging

Every pull request is squash merged, so its title becomes the commit message on
`master`. The title must be a [conventional commit](https://www.conventionalcommits.org/)
subject, such as `feat: add a Redis store`, `fix(worker): ack after the handler returns`
or `feat!: rename the envelope field`. The `PR title` check fails otherwise, and it
accepts the types `feat`, `fix`, `perf`, `refactor`, `docs`, `test`, `chore`, `ci`,
`build` and `revert`. A `!` after the type, or a `BREAKING CHANGE:` footer in the commit
body, marks a breaking change.

## The Release PR

After each push to `master`, release-please opens or updates a single standing pull
request titled `chore(master): release X.Y.Z`. It bumps the version in `pyproject.toml`
and `uv.lock`, and prepends the new section to `CHANGELOG.md`. The changelog lists
`feat`, `fix`, `perf`, `refactor` and `docs` changes; `test`, `chore`, `ci` and `build`
changes are left out.

The version bump follows these rules while asynk is below 1.0:

| Change since the last release | Bump |
| --- | --- |
| Breaking change (`!` or `BREAKING CHANGE:`) | minor (`0.1.0` to `0.2.0`) |
| `feat` or `fix` | patch (`0.1.0` to `0.1.1`) |

## Merging the Release PR

Merging the Release PR is the release. The `Release` workflow then:

1. creates the tag `vX.Y.Z` and the GitHub Release,
2. builds the sdist and wheel from the tagged commit,
3. publishes them to TestPyPI, using the `testpypi` environment,
4. publishes them to PyPI, using the `pypi` environment.

Publishing uses PyPI trusted publishing, so there are no API tokens. The trusted
publishers on both indexes are registered against the workflow file name, so the
workflow must stay named `.github/workflows/release.yml`, and it cannot be split into
reusable workflows. The environments must stay named `testpypi` and `pypi`.

## Release notes

The generated changelog lists every change. Anything that needs explaining, such as
breaking changes and how to migrate, goes in a hand-written page under
`docs/release-notes/`. See [Release notes](release-notes/index.md) for how to add one.
Write it in the pull request that makes the change, or before merging the Release PR.

## First release only

`release-please-config.json` contains `"release-as": "0.1.0"`, which forces the first
Release PR to propose `0.1.0`. Remove that line right after the first release is
published. While it stays, release-please keeps proposing `0.1.0` and ignores the
bump rules above.

## The `uv.lock` version

The config updates the `asynk` entry in `uv.lock` with the `toml` updater and the
JSONPath `$.package[?(@.name.value==='asynk')].version`. Without it, `uv sync --locked`
fails after a release because the lockfile still has the old version. This path has not
been exercised yet, so check that the first Release PR changes `uv.lock`. If it does
not, switch that entry to the `generic` updater with an `x-release-please-version`
marker on the version line.
