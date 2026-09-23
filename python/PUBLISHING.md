# Publishing `agentic-flink` to PyPI

This repo publishes the Python facade via **PyPI Trusted Publishing** (OIDC).
No API tokens are stored in GitHub secrets. PyPI verifies the workflow's
identity directly from GitHub.

The Java framework jar **is** bundled in the wheel. `python/setup.py` copies
`target/agentic-flink-<version>-uber.jar` into `agentic_flink/jars/` when the wheel or
sdist is built (running `./mvnw` first when no jar exists yet), so a clean
`pip install agentic-flink` can start the JVM. `AGENTIC_FLINK_JAR` remains a developer
override at build time (which jar to bundle) and at run time (which jar to load, see
[`docs/python.md`](../docs/python.md)). The uber jar is about 260 MiB, so the wheel is
about 250 MiB; PyPI's default per-file limit is 100 MiB and a project file size limit
increase must be requested at https://pypi.org/help/#file-size-limit before the first
upload succeeds.

`python/pyproject.toml` carries no version. setuptools-scm reads it from the release tag
(`v<version>`), the same mechanism the other three Python distributions use, and the
publish workflow fails before building when the tag is not canonical or any of the four
projects resolves to a different version (`tools/check_release_version.py` at the
repository root). The full release flow, including the other distributions and the
Clojure artifact, is in [`docs/release.md`](../docs/release.md); this page keeps the
PyPI account and trusted publisher setup for `agentic-flink`.

---

## One-time setup (you only do this once)

### 1. Create the accounts

| Service        | URL                                       | What to enable                                            |
|----------------|-------------------------------------------|-----------------------------------------------------------|
| **PyPI**       | https://pypi.org/account/register/        | 2FA (required for new accounts), recovery codes saved     |
| **TestPyPI**   | https://test.pypi.org/account/register/   | 2FA (good practice; separate from PyPI account)           |

Use the **same email** if you like, the accounts are entirely separate but
having a matched pair makes the workflow obvious.

### 2. Register Trusted Publishers (before the package exists: "pending")

PyPI lets you register a trusted publisher for a project *before* the project
exists. The first successful upload creates the project.

#### PyPI (production)

1. Log in to https://pypi.org/manage/account/publishing/
2. Scroll to **Add a new pending publisher**
3. Fill in:
   - **PyPI Project Name:** `agentic-flink`
   - **Owner:** `Ugbot`
   - **Repository name:** `Agentic-Flink`
   - **Workflow name:** `publish-pypi.yml`
   - **Environment name:** `pypi`
4. Click **Add**

#### TestPyPI (rehearsal)

1. Log in to https://test.pypi.org/manage/account/publishing/
2. Same as above, but:
   - **Workflow name:** `publish-testpypi.yml`
   - **Environment name:** `testpypi`

### 3. Create matching GitHub environments

Environments scope OIDC tokens so only specific workflows can mint them.

1. Go to https://github.com/Ugbot/Agentic-Flink/settings/environments
2. Click **New environment** → name it `pypi` → save (no rules needed for now;
   optionally require reviews for prod releases)
3. Repeat for `testpypi`

That's the entire one-time setup.

---

## Routine: publishing a new release

### Rehearse on TestPyPI first (recommended)

1. Tag the commit with a pre-release version and push the tag:
   ```bash
   git tag -a v1.0.0rc1 -m "agentic 1.0.0rc1"
   git push origin v1.0.0rc1
   ```
2. Go to **Actions → Publish Python packages to TestPyPI → Run workflow** and enter the tag
3. After it succeeds, install from TestPyPI to verify:
   ```bash
   pip install -i https://test.pypi.org/simple/ \
     --extra-index-url https://pypi.org/simple/ \
     agentic-flink==<version>
   ```
   (The extra index is needed because TestPyPI doesn't mirror JPype1.)

### Real release

1. Tag the commit with the release version and push the tag; the version is `v` plus a
   canonical PEP 440 version, nothing is edited in `pyproject.toml`:
   ```bash
   git tag -a v1.0.0 -m "agentic 1.0.0"
   git push origin v1.0.0
   ```
2. Cut a GitHub Release pointing at the tag:
   ```bash
   gh release create v1.0.0 --generate-notes
   ```
   (Or use the GitHub UI: Releases → Draft a new release.)
3. The `publish-pypi.yml` workflow fires automatically on release-publish. It checks the
   tag, builds the core and the shaded jar with `./mvnw`, builds all four distributions,
   checks the artifact versions, runs `twine check`, and runs `tools/release_dry_run.sh`
   in a fresh venv (imports, one fixture on the `local` and `local-jvm` runtimes, one
   `agentic-pipeline` turn) before anything is uploaded. Only the distributions named in
   the workflow's `PUBLISH_DISTRIBUTIONS` variable (today `agentic-flink`) are uploaded.
   The manual `workflow_dispatch` run takes the tag as an input.
4. Watch it: https://github.com/Ugbot/Agentic-Streaming/actions

PyPI cannot accept the same `version` twice. If a publish fails partway, bump
to the next pre-release number rather than retrying with the same one.

---

## Local dry-run

Always safe to build locally to check the artifacts before pushing:

```bash
./mvnw -f ports/jagentic-core/pom.xml install -DskipTests
./mvnw -DskipTests package          # optional: setup.py runs these two when target/ has no uber jar
cd python
pip install --upgrade build twine setuptools-scm
rm -rf dist build *.egg-info agentic_flink/jars/*.jar
python -m build
python ../tools/check_release_version.py "$(python -m setuptools_scm)" dist/*
python -m twine check --strict dist/*
unzip -l dist/*.whl | grep uber.jar
```

You should see `agentic_flink/`, `agentic_flink/py.typed`,
`agentic_flink/jars/agentic-flink-<v>-uber.jar`, `agentic_flink-<ver>.dist-info/`, and the
`LICENSE` inside the wheel. On a checkout that is not exactly at a tag the version is a
development version such as `0.1.dev199`; the check above accepts it, the workflows do not.
To prove the wheels work without the checkout, run `tools/release_dry_run.sh` at the
repository root (Podman, `python:3.12`; `RUNNER=venv` for a virtualenv instead), which
installs all four wheels into an empty environment and runs
`python/tests/smoke_clean_install.py` among the other checks.

---

## Versioning

Stick to [PEP 440](https://peps.python.org/pep-0440/):

| Stage              | Example      |
|--------------------|--------------|
| Alpha              | `1.0.0a1`    |
| Beta               | `1.0.0b1`    |
| Release candidate  | `1.0.0rc1`   |
| Stable             | `1.0.0`      |
| Post-release patch | `1.0.0.post1`|

Pre-releases are not installed by default (`pip install agentic-flink`
will skip `1.0.0a1`); users have to pass `--pre` or pin an exact version.

---

## Troubleshooting

- **"Trusted publishing exchange failure"**: the workflow's `environment:`,
  `workflow:`, or repo path doesn't match the pending publisher config on
  PyPI. Recheck the four fields under "Add a new pending publisher".
- **"File already exists"**: you tried to upload a version PyPI has already
  accepted. Bump the version.
- **`twine check` fails on long-description**, usually a Markdown issue in
  `README.md`. PyPI renders CommonMark; avoid raw HTML.
- **First upload missing classifiers / metadata**: make sure `pyproject.toml`
  changes were committed before the release was cut.
