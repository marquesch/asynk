# Release notes

The [changelog](../changelog.md) is generated from the commit history and lists every
change in each release. The release notes are different: contributors write them by
hand to explain what matters in a release, what breaks, and how to migrate.

- [0.1](0.1.md)

## Contributing a release note

A release that changes behavior, removes something or breaks an API gets a page
named after its minor version, such as `docs/release-notes/0.2.md`. Write it in the
pull request that makes the change, or before the release:

1. Create `docs/release-notes/<major>.<minor>.md` and add it to the list above and to
   the "Release notes" entry of the navigation in `zensical.toml`.
2. Put `Breaking:` entries first, each saying what changed and what to write instead.
3. Group the rest under Added, Changed, Removed and Fixed.
4. Run `make docs`, which builds the site in strict mode and fails on broken links.
