============
Contributing
============

Thanks for your interest in contributing to ``datamasque-python``!
Contributions, bug reports, and feature requests are all welcome.

Reporting bugs
==============

File an issue on the `GitHub issue tracker <https://github.com/datamasque/datamasque-python/issues>`_.
Please include:

- the version of ``datamasque-python`` you're using (``pip show datamasque-python``);
- the Python version and operating system;
- a minimal reproducer if possible;
- the full traceback if the bug manifests as an exception.

If the bug concerns a specific DataMasque server API response,
include the status code and (with any sensitive fields redacted) the response body.

Feature requests
================

Open an issue describing what you'd like to do and why.
We're particularly interested in feedback on:

- public API shape (method names, argument names, return types);
- endpoints not yet wrapped by the client;
- improvements to the typed return models.

Development setup
=================

The project uses `uv <https://docs.astral.sh/uv/>`_ for dependency management.
Install dependencies and set up a virtual environment:

.. code-block:: console

    git clone https://github.com/datamasque/datamasque-python.git
    cd datamasque-python
    uv sync

Running the tests
=================

.. code-block:: console

    uv run pytest

The test suite runs entirely against mocked HTTP responses (``requests_mock``),
so no DataMasque server is required.

Linting and type-checking
=========================

.. code-block:: console

    uv run ruff check datamasque tests
    uv run ruff format --check datamasque tests
    uv run mypy datamasque

``ruff check`` enforces import order,
Python style,
and a set of pydocstyle rules (``D101``, ``D102``, ``D204``, ``D205``, ``D213``)
that require docstrings on all public classes and methods.
``ruff format`` applies the project's formatting style.
``mypy`` runs in strict mode with ``disallow_untyped_defs``.

Code style
==========

- **Line length:**
  120 characters.
  Enforced by ``ruff format``.
- **Docstrings and comments:**
  use `semantic line breaks <https://sembr.org/>`_ —
  break at clause boundaries, not column widths.
  This applies to text files (such as this file) as well as Python source.
- **Docstring content:**

  - Write for library consumers, not maintainers.
  - Keep docstrings concise; no internal implementation notes.
  - Multi-line docstrings start on the next line after the opening triple quotes.
  - In Python docstrings,
    use single backticks around anything code-like —
    ``default_role = "any"`` in ``docs/conf.py`` makes Sphinx auto-link Python identifiers
    and render everything else as monospace.
    In top-level ``.rst`` files (``README.rst``, ``CONTRIBUTING.rst``, ``HISTORY.rst``)
    use double backticks instead —
    those are rendered directly by GitHub,
    which doesn't honour the Sphinx role config.

- **Enum member casing:**
  enum members are ``lower_snake_case``, for example, ``DatabaseType.postgres``.
- **Enum comparisons:**
  use ``is`` / ``is not`` when comparing against specific enum members, not ``==`` / ``!=``.
- **String formatting in messages:**
  errors, log lines, and other user-facing messages follow a consistent quoting convention —
  backticks around enum values and code identifiers,
  double quotes around free-form string values.
  Avoid ``!r`` in f-strings;
  it produces Python's default single-quoted ``repr``,
  which conflicts with the convention.
  Use a single-quoted outer f-string
  so double-quoted value literals don't need escaping:

  .. code-block:: python

      raise DataMasqueUserError(
          f'The ruleset "{name}" is in `{state.value}` state.'
      )

  ``__str__`` follows this rule (it is a user-facing representation).
  ``__repr__`` does not —
  it follows Python's native ``repr`` convention,
  where ``!r`` and single quotes are idiomatic.

- **Identifier casing for initialisms:**
  only the first letter of an initialism is capitalised in a camel-case identifier —
  ``DataMasqueApiError``, not ``DataMasqueAPIError``.
  The brand ``DataMasque`` is always spelled out in full.
- **Serialization conventions:**
  API models subclass pydantic ``BaseModel``.

  - Serialise outgoing request bodies with ``model.model_dump(exclude_none=True, mode="json")``;
    add ``by_alias=True`` when the model uses field aliases.
  - Parse incoming responses with ``Model.model_validate(response.json())``.
  - Use ``ConfigDict(extra="forbid")`` on outgoing request models
    so a typo in a field name fails loudly.
  - Use ``ConfigDict(extra="allow")`` on incoming response models
    so unknown fields the server may add in future don't break deserialisation.
- **Imports:**

  - All imports at the top of the file; no inline imports.
  - Absolute imports only; relative imports are not used.

- **Formatting:**
  run ``uv run ruff format`` before committing.

Pull requests
=============

1. Fork the repository and create a feature branch.
2. Add tests for any behavioural change.
3. Run ``uv run pytest``, ``uv run ruff check``, ``uv run ruff format --check``, and ``uv run mypy``
   locally before opening the PR.
4. Keep commits focused; one logical change per commit is easier to review.
5. Open a PR against ``main`` and describe what the change does and why.
6. The maintainers will review and either merge, request changes, or close with an explanation.

Commit messages
===============

Use `conventional commits <https://www.conventionalcommits.org/>`_ format where practical:
``feat: add cancel_run method``,
``fix: handle 401 retry for multipart uploads``,
``docs: clarify make_request exception semantics``,
and so on.

License
=======

By contributing,
you agree that your contributions will be licensed under the Apache License 2.0,
the same license as the rest of the project.
