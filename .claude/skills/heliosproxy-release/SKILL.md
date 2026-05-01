---
name: heliosproxy-release
description: Cut a HeliosProxy release. Bump version → CHANGELOG → commit → tag → push. The `crates-io.yml` workflow runs `cargo publish` on tag push. Use when the user says "release", "cut a version", "publish to crates.io", "tag a release", "bump version".
allowed-tools: Bash(git *), Bash(cargo *), Bash(gh *), Read, Edit
related: [heliosproxy-overview, heliosproxy-install]
---

# Cut a release

Tag-driven publish. Pushing a `vX.Y.Z` tag whose number matches
`Cargo.toml`'s `version` triggers
[`.github/workflows/crates-io.yml`](../../.github/workflows/crates-io.yml),
which runs `cargo publish --locked`. Repo secret
`CARGO_REGISTRY_TOKEN` (set at the `dimensigon` org level) provides
the crates.io credential.

🟡 Mutating, irreversible — once a version is published, it cannot
be re-uploaded. Only yanked.

## When to use

- New patch release after merging a fix
- Minor / major release with documented changes
- Re-publishing the same tag after a transient registry failure
  (use `workflow_dispatch`)

## Surfaces

| Step | How |
|---|---|
| Bump version in `Cargo.toml` | `Edit` or `cargo set-version` |
| Add CHANGELOG entry         | `Edit CHANGELOG.md` |
| Commit                      | `git commit -am "release: X.Y.Z"` |
| Push commit                 | `git push` |
| Tag                         | `git tag -a vX.Y.Z -m "..."` |
| Push tag (triggers workflow)| `git push origin vX.Y.Z` |
| Watch run                   | `gh run watch` |
| Verify on crates.io         | `curl https://crates.io/api/v1/crates/heliosdb-proxy` |

## Recipes

### Recipe 1: Standard patch release flow

```bash
cd /home/app/Helios/Proxy

# 1. bump version
# In Cargo.toml: version = "0.4.1" → "0.4.2"

# 2. CHANGELOG
# Add a new "## [0.4.2] - YYYY-MM-DD" block at the top with a
# concise summary, sections for Added / Changed / Fixed.

# 3. confirm cargo is happy
cargo build --lib
cargo publish --dry-run        # full verify, including upload abort

# 4. commit
git add Cargo.toml Cargo.lock CHANGELOG.md
git commit -m "release: 0.4.2

Patch release: <one sentence>.

Co-Authored-By: ..."

# 5. push commit
git push

# 6. tag
git tag -a v0.4.2 -m "v0.4.2 — <one sentence>"
git push origin v0.4.2
```

The tag push triggers the workflow. ETA ~1 min.

### Recipe 2: Watch the publish run

```bash
gh run watch --workflow crates-io.yml -R dimensigon/HDB-HeliosDB-Proxy
```

Or list recent runs:

```bash
gh run list --workflow crates-io.yml -R dimensigon/HDB-HeliosDB-Proxy --limit 3
# in_progress    release: 0.4.2  Publish to crates.io  v0.4.2  push
```

### Recipe 3: Verify the crate is live

```bash
curl -s https://crates.io/api/v1/crates/heliosdb-proxy | jq '{
  default_version, num_versions, downloads, max_stable_version
}'
```

```json
{
  "default_version":     "0.4.2",
  "num_versions":        3,
  "downloads":           42,
  "max_stable_version":  "0.4.2"
}
```

`docs.rs` builds asynchronously after publish — typically appears
within 5-30 minutes at <https://docs.rs/heliosdb-proxy/0.4.2>.

### Recipe 4: Re-run the workflow against an existing tag

If the publish failed transiently (network, registry blip), re-run
without re-tagging:

```bash
gh workflow run crates-io.yml \
  -R dimensigon/HDB-HeliosDB-Proxy \
  -f tag=v0.4.2
```

The workflow's `workflow_dispatch.inputs.tag` accepts an existing
tag and re-runs cargo publish on it. The version still has to NOT
exist on crates.io (you can't republish the same version, only
recover from a failed first attempt).

### Recipe 5: Quickly verify version match before tagging

```bash
TAG=v0.4.2
CARGO=$(grep -m1 '^version = ' Cargo.toml | cut -d'"' -f2)
[ "${TAG#v}" = "$CARGO" ] && echo "OK: tag matches Cargo.toml" \
                          || echo "MISMATCH: $TAG vs $CARGO"
```

The workflow does this check before publishing — but better to
catch it locally before pushing the tag.

### Recipe 6: Yank a bad release (don't delete)

If a published version contains a critical bug:

```bash
cargo yank --vers 0.4.2 heliosdb-proxy
```

Yanked versions stay on crates.io but won't be picked up by `cargo
add` / dep solvers as a default. Existing pinned consumers continue
to work. Cut a fixed `0.4.3` ASAP.

To un-yank:

```bash
cargo yank --vers 0.4.2 --undo heliosdb-proxy
```

## Pitfalls

- **Tag and Cargo.toml MUST match.** The workflow fails loudly if
  not. `v0.4.2` ↔ `version = "0.4.2"`. No `v` in Cargo.toml; `v`
  in the tag.
- **Push the commit BEFORE pushing the tag.** Otherwise the tag
  refers to a SHA the workflow can't checkout. The workflow's
  `actions/checkout@v4` resolves the tag's commit; if the commit
  isn't on origin, checkout fails.
- **`cargo publish` is irreversible.** A typo in version number
  burns that number forever. Triple-check before pushing the tag.
- **Don't `--no-verify` cargo publish.** The verify step catches
  most common errors (deps that don't exist on crates.io, bad
  manifest fields). Skip it only if you've already verified
  manually with `--dry-run`.
- **`docs.rs` build can fail** even if `cargo publish` succeeded —
  docs.rs uses `--all-features` per `[package.metadata.docs.rs]`
  in `Cargo.toml`. A feature combination that compiles but has
  doc-comment cycles can break docs.rs while the crate publishes
  fine. Check <https://docs.rs/crate/heliosdb-proxy/latest/builds>.
- **Make sure `CARGO_REGISTRY_TOKEN` is the org-scoped secret
  with `publish-update` scope on the `heliosdb-proxy` crate.**
  A wider-scoped token works but is risky. Generate at
  <https://crates.io/me>.

## See also

- `.github/workflows/crates-io.yml` — the workflow this skill drives
- `heliosproxy-install` — what users get from the published artefact
- `CHANGELOG.md` — the file you'll edit
- crates.io: <https://crates.io/crates/heliosdb-proxy>
- docs.rs: <https://docs.rs/heliosdb-proxy>
- Code: [`Cargo.toml`](../../Cargo.toml) — version + metadata
