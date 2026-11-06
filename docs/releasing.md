# Releasing

Releases are automated with [release-please](https://github.com/googleapis/release-please).
Nobody edits the version or `CHANGELOG.md` by hand. Everything is driven by the titles
of merged pull requests.

## Pull request titles and merging

Every pull request is squash merged, so its title becomes the commit message on
`master`. The title must be a [conventional commit](https://www.conventionalcommits.org/)
subject, such as `feat: add a Redis store`, `fix(worker): ack after the handler returns`
or `feat!: rename the envelope field`. The `PR title` check fails otherwise. It runs
[amannn/action-semantic-pull-request](https://github.com/amannn/action-semantic-pull-request)
on `pull_request` (not `pull_request_target`) with read-only `pull-requests`
permission and no checkout, so it never runs code from the pull request. It accepts the
types `feat`, `fix`, `perf`, `refactor`, `docs`, `test`, `chore`, `ci`, `build` and
`revert`; the scope is optional. A `!` after the type, or a `BREAKING CHANGE:` footer in the commit
body, marks a breaking change.

## The Release PR

After each push to `master`, release-please opens or updates a single standing pull
request titled `chore(master): release X.Y.Z`. It bumps the version in `pyproject.toml`
and `uv.lock`, and prepends the new section to `CHANGELOG.md`. The changelog lists
`feat`, `fix`, `perf`, `refactor`, `docs` and `revert` changes; `test`, `chore`, `ci`
and `build` changes are left out. Only listed changes lead to a release: while
`master` has nothing but `test`, `chore`, `ci` or `build` commits since the last
release, there is no Release PR.

The Release PR is opened with the workflow's `GITHUB_TOKEN`, and GitHub does not start
workflows for pull requests opened or updated with that token. The `Tests`, `Docs` and
`PR title` checks therefore do not run on the Release PR. If branch protection requires
them, the Release PR cannot be merged without bypassing the rule. The commits it
releases have already passed those checks in their own pull requests.

The version bump follows these rules while asynk is below 1.0:

| Change since the last release | Bump |
| --- | --- |
| Breaking change (`!` or `BREAKING CHANGE:`) | minor (`0.1.0` to `0.2.0`) |
| Any other listed change (`feat`, `fix`, `perf`, `refactor`, `docs`, `revert`) | patch (`0.1.0` to `0.1.1`) |

## Merging the Release PR

Merging the Release PR is the release. The `Release` workflow then:

1. creates the tag `vX.Y.Z` and the GitHub Release,
2. builds the sdist and wheel from the tagged commit,
3. publishes them to TestPyPI, using the `testpypi` environment,
4. publishes them to PyPI, using the `pypi` environment.

Publishing uses PyPI trusted publishing, so there are no API tokens, through the
official [`pypa/gh-action-pypi-publish`](https://github.com/pypa/gh-action-pypi-publish)
action. Each publish runs in its own job, with only `contents: read` and
`id-token: write`, and does nothing but download the built `dist` artifact and upload
it. No project code or build tool runs in a job that holds the OIDC token, and the
action's default attestations are kept. The trusted
publishers on both indexes are registered against the workflow file name, so the
workflow must stay named `.github/workflows/release.yml`, and it cannot be split into
reusable workflows. The environments must stay named `testpypi` and `pypi`.

## Supply chain pins

- Every action in every workflow is pinned to a full commit SHA, with the release in a
  trailing `# vX.Y.Z` comment. Tags can be moved; SHAs cannot.
- `astral-sh/setup-uv` installs an exact uv (`version: "0.13.7"`), and the build backend
  is pinned exactly in `pyproject.toml` (`uv_build==...`). Keep the uv version at or
  above the `uv_build` version and the `uv.lock` revision, so uv is never older than
  what wrote the lock or the backend it builds with.
- Installs use `uv sync --locked` (and `uv run --locked --group docs`), so CI fails if
  `uv.lock` is out of date instead of silently re-resolving.
- [Dependabot](https://docs.github.com/en/code-security/dependabot) (`.github/dependabot.yml`)
  opens one grouped weekly pull request for GitHub Actions and one for Python
  dependencies (`uv`), titled `chore(deps): ...` so the PR title check passes. Review
  the new SHA and release notes before merging. The `uv_build` pin and the uv version
  in the workflows are not covered by Dependabot; bump them by hand together.

- The Redis service container in `.github/workflows/tests.yml` is pinned to an exact
  tag (`redis:7.4.12`). Dependabot's `github-actions` ecosystem does not update
  workflow service images, so bump the tag by hand, and the `docker run` command in
  [Writing a custom transport](advanced/custom-transport.md#testing-your-transport)
  with it. Streams need Redis 6.2 or later for `XAUTOCLAIM`.

## First release

`0.1.0` was the first release. It was forced with `"release-as": "0.1.0"` in
`release-please-config.json`, which has since been removed, so the bump rules above now
apply to every Release PR. There was no earlier release tag, so the first changelog
section lists every conventional commit on `master` up to that point.

## The `uv.lock` version

The config updates the `asynk` entry in `uv.lock` with the `toml` updater and the
JSONPath `$.package[?(@.name.value==='asynk')].version`. Without it, `uv sync --locked`
fails after a release because the lockfile still has the old version. The path was
tested locally by running release-please's own `toml` updater on this repository's
`uv.lock`: it changed the `asynk` entry's version and nothing else. That test did not
go through GitHub, so check that the first Release PR that changes the version also
changes `uv.lock`. If it does not, switch that entry to the `generic` updater with an
`x-release-please-version` marker on the version line.

The first Release PR proposed `0.1.0`, which `pyproject.toml` and `uv.lock` already
contained, so it only changed `CHANGELOG.md` and `.release-please-manifest.json`. The
`uv.lock` update is first visible in the Release PR after that.

## Repository settings

These are applied by the repository owner in GitHub and PyPI, not by any file here.

- `pypi` environment: required reviewer, and a deployment branch restriction to
  `master` (or the release tags). Same restriction for `testpypi`.
- `master` ruleset: require a pull request and a review, require status checks
  (excluding checks the Release PR cannot run, see above), block force pushes and
  deletion.
- Require approval for fork pull request workflows from outside collaborators.
- Default workflow token permission: read-only.
- Allowed actions: allow GitHub-owned actions plus `astral-sh/*`, `googleapis/*`,
  `pypa/*` and `amannn/*` only.
- Allow GitHub Actions to create and approve pull requests (release-please needs it).
- Secret scanning and push protection on.
- Dependabot alerts and security updates on.
- Private vulnerability reporting on (see `SECURITY.md`).
- 2FA or passkeys on the GitHub and PyPI accounts, and no PyPI API tokens.
- Automatically delete head branches after merge.
- Squash merging only, with the pull request title as the commit message.
