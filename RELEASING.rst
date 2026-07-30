=========
Releasing
=========

For DataMasque staff / maintainers of this project.
Covers production releases to PyPI, dev releases to TestPyPI, and how downstream projects pin ``datamasque-python``.

The assumed starting point is that you have a ``datamasque-python`` feature/fix PR in progress,
and want to know how to get it pushed up to PyPI once merged,
or how to build a test release / pin to a particular commit so you can test before merging the PR.

Version numbers live in two places
==================================

Every release must update both, or CI will reject the tag.
As part of your PR, update:

1. ``pyproject.toml`` — ``[project] version``.
   This is the version the built sdist/wheel carries,
   and the one ``release.yml`` checks the git tag against.
2. ``uv.lock`` — the ``version`` field of the ``datamasque-python`` package entry.
   CI runs ``uv sync --frozen``, so a stale lock fails every job.

Edit ``pyproject.toml`` by hand, then run ``uv lock`` to update the lock entry.
Don't hand-edit ``uv.lock``.

Don't update ``datamasque.client.__version__`` or ``docs/conf.py``.
These read the installed package metadata.

Updating the changelog
======================

Do this as part of your PR.

``HISTORY.rst`` needs a new entry for each release to pypi.
Add a new entry at the top, under the ``History`` heading, with the new version and today's date.
Use short summaries of additions and changes, written with past tense and worded for a customer consumer of this library.
For example::

       1.2.2 (2026-08-05)
       ------------------

       * Added ``get_all_widgets`` API.
       * Fixed issue where ``list_frobs`` would incorrectly raise a ``ValueError`` when using the filtering options.

       Requires server version 3.26.14.

Call out breaking changes explicitly,
and note the minimum DataMasque server version if the release requires one.

Releasing to PyPI
=================

1. Get your PR approved and merged.
2. Tag the merge commit on ``main`` and push the tag::

       git checkout main
       git pull --ff-only
       git tag v1.2.2  # or whatever is your version number
       git push origin v1.2.2

   The tag must be ``v`` + the exact ``pyproject.toml`` version.
   ``release.yml`` triggers on ``v*.*.*`` and fails the build if the two disagree.
   Tag only after the PR has merged, so the tag points at a commit on ``main``.
3. The admins of the repo will get an automated email asking them to approve the deployment.
   They can do this by visiting the Workflows page or by clicking the link in the email.
4. Once approved, the build will appear on PyPI very quickly (around 1 minute).
   You can verify that the release appears at https://pypi.org/project/datamasque-python/,
   and Read the Docs has built the new tag
   at https://datamasque-python.readthedocs.io/.
5. Create the GitHub Release (see below).

Creating the GitHub Release
===========================

Do this after the PyPI publish has succeeded,
so the release never points at a version nobody can install.

1. On the repository home page,
   find **Releases** in the right-hand sidebar under the *About* box,
   and click it,
   then **Draft a new release**.
2. **Choose a tag** — pick the existing tag you pushed as part of releasing to PyPI.
   Don't let GitHub create a new one; the tag should already exist from the release flow.
3. Set the target to ``main``.
4. Title it the same as the tag, e.g. ``v1.2.2``.
5. Paste the ``HISTORY.rst`` entry for this version into the body, converted to Markdown.
   **Generate release notes** is a reasonable starting point for the commit list,
   but the hand-written changelog is what users read.
6. Leave *Set as a pre-release* unticked for a normal release.
7. Click **Publish release**.

Creating releases can also be done from the ``gh`` CLI::

    gh release create v1.2.2 --title v1.2.2 --notes-file notes.md

Releasing a dev build to TestPyPI
=================================

Use this to let a downstream project try an unreleased change.
It builds from any ref and never touches PyPI.

1. Push the branch you want to publish.
   Nothing needs to be merged first.
2. Go to *Actions* → *Release (TestPyPI)* → **Run workflow**.
3. Fill in the inputs:

   - **ref** — the branch to build from.
   - **dev_version** — the version to publish, for example ``1.3.0.dev1``.

   Use a ``.devN`` suffix.
   The input is validated, so a plain release version like ``1.3.0`` is rejected.

   **Note:** Version numbers cannot be reused. Ensure you increment the suffix each time.

4. No approval is needed — unlike a PyPI release, the build publishes as soon as it passes.
5. Verify at https://test.pypi.org/project/datamasque-python/.

**Note:** Do **not** create a git tag or a GitHub Release for a TestPyPI build.

Pointing a project at a release
===============================

Three ways to depend on ``datamasque-python``, depending on what you need.

A released version on PyPI
--------------------------

The usual case.
In the consuming project's ``pyproject.toml``::

    dependencies = [
        "datamasque-python==1.2.2",
    ]

Always give a version specifier.
An unpinned ``"datamasque-python"`` means "whatever is newest at the moment this resolves",
which is prone to breaking when someone releases a ``datamasque-python`` change before the corresponding server release.

Pin exactly (``==1.2.2``) when you want every upgrade to be a deliberate, reviewed change.
A bounded range (``>=1.2.2,<2``) is acceptable where you need a floor for a specific feature,
but note that this project does sometimes ship breaking changes
(e.g. 1.2.0 removed ``config_type`` from the discovery config library APIs),
so read the ``HISTORY.rst`` entries between your current version and the new one before raising the floor.

A dev build on TestPyPI
-----------------------

TestPyPI is a separate index, so it must be declared as well as pinned.
Pin the exact dev version — TestPyPI content is disposable.

**Note:** Always replace a TestPyPI pin with a real PyPI version before merging the consuming project —
TestPyPI is not a durable index,
and the downstream project should always use a "proper" (normal PyPI) version of ``datamasque-python`` once merged.

With ``uv``::

    [project]
    dependencies = [
        "datamasque-python==1.3.0.dev1",
    ]

    [[tool.uv.index]]
    name = "testpypi"
    url = "https://test.pypi.org/simple/"
    explicit = true

    [tool.uv.sources]
    datamasque-python = { index = "testpypi" }

**Note:** Be sure to include the ``explicit = true``:
without it, ``uv`` will resolve *every* dependency against TestPyPI, which mirrors PyPI only partially.

With Poetry::

    [[tool.poetry.source]]
    name = "testpypi"
    url = "https://test.pypi.org/simple/"
    priority = "explicit"

    [tool.poetry.dependencies]
    datamasque-python = { version = "1.3.0.dev1", source = "testpypi" }

Ad hoc, without editing anything::

    pip install \
        --index-url https://test.pypi.org/simple/ \
        --extra-index-url https://pypi.org/simple/ \
        datamasque-python==1.3.0.dev1


A branch or commit on GitHub
----------------------------

For testing an unmerged change without publishing anything at all.
Prefer a commit SHA over a branch name — a branch ref can change under you, and a lockfile pinned to one is not reproducible.

**Note:** Always replace a git pin with a PyPI version before merging the consuming project,
for the same reasons as above, plus the fact git dependencies break any downstream install that lacks repository access.

``uv`` sources form, which keeps the version constraint readable::

    [project]
    dependencies = [
        "datamasque-python",
    ]

    [tool.uv.sources]
    datamasque-python = { git = "https://github.com/datamasque/datamasque-python.git", rev = "ab12cd34" }

With Poetry::

    [tool.poetry.dependencies]
    datamasque-python = { git = "https://github.com/datamasque/datamasque-python.git", rev = "ab12cd34" }

Use ``git+ssh://git@github.com/...`` instead of ``https`` if the consumer builds somewhere without an HTTPS credential for the repository.

PEP 508 direct reference,
which ``pip`` and ``uv`` both understand::

    dependencies = [
        "datamasque-python @ git+https://github.com/datamasque/datamasque-python.git@ab12cd34",
    ]
