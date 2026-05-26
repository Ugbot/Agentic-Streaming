# Publishing `agentic-flink` to PyPI

This repo publishes the Python facade via **PyPI Trusted Publishing** (OIDC).
No API tokens are stored in GitHub secrets — PyPI verifies the workflow's
identity directly from GitHub.

The Java framework jar is **not** bundled in the wheel. Users obtain it via
their own classpath or the `AGENTIC_FLINK_JAR` env var (see
[`docs/python.md`](../docs/python.md)).

---

## One-time setup (you only do this once)

### 1. Create the accounts

| Service        | URL                                       | What to enable                                            |
|----------------|-------------------------------------------|-----------------------------------------------------------|
| **PyPI**       | https://pypi.org/account/register/        | 2FA (required for new accounts), recovery codes saved     |
| **TestPyPI**   | https://test.pypi.org/account/register/   | 2FA (good practice; separate from PyPI account)           |

Use the **same email** if you like — the accounts are entirely separate but
having a matched pair makes the workflow obvious.

### 2. Register Trusted Publishers (before the package exists — "pending")

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

1. Bump `version` in `python/pyproject.toml`
   (e.g. `1.0.0a1` → `1.0.0a2` or `1.0.0rc1`)
2. Commit and push to `main`
3. Go to **Actions → Publish Python package to TestPyPI → Run workflow**
4. After it succeeds, install from TestPyPI to verify:
   ```bash
   pip install -i https://test.pypi.org/simple/ \
     --extra-index-url https://pypi.org/simple/ \
     agentic-flink==<version>
   ```
   (The extra index is needed because TestPyPI doesn't mirror JPype1.)

### Real release

1. Bump `version` in `python/pyproject.toml` to the real number
   (e.g. `1.0.0`)
2. Commit, push, and tag:
   ```bash
   git tag v1.0.0
   git push origin v1.0.0
   ```
3. Cut a GitHub Release pointing at the tag:
   ```bash
   gh release create v1.0.0 --generate-notes
   ```
   (Or use the GitHub UI: Releases → Draft a new release.)
4. The `publish-pypi.yml` workflow fires automatically on release-publish.
5. Watch it: https://github.com/Ugbot/Agentic-Flink/actions

PyPI cannot accept the same `version` twice. If a publish fails partway, bump
to the next pre-release number rather than retrying with the same one.

---

## Local dry-run

Always safe to build locally to check the artifacts before pushing:

```bash
cd python
pip install --upgrade build twine
rm -rf dist build *.egg-info
python -m build
python -m twine check dist/*
# Optional: inspect the wheel contents
unzip -l dist/*.whl
```

You should see `agentic_flink/`, `agentic_flink-<ver>.dist-info/`, and the
`LICENSE` inside the wheel.

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

- **"Trusted publishing exchange failure"** — the workflow's `environment:`,
  `workflow:`, or repo path doesn't match the pending publisher config on
  PyPI. Recheck the four fields under "Add a new pending publisher".
- **"File already exists"** — you tried to upload a version PyPI has already
  accepted. Bump the version.
- **`twine check` fails on long-description** — usually a Markdown issue in
  `README.md`. PyPI renders CommonMark; avoid raw HTML.
- **First upload missing classifiers / metadata** — make sure `pyproject.toml`
  changes were committed before the release was cut.
