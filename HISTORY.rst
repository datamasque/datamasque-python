=======
History
=======

1.0.0 (2026-04-21)
------------------

* **First public open-source release.**
* All request and response types are now pydantic v2 models.
* Added support for many new APIs.
* Added ``DataMasqueIfmClient`` for the in-flight masking (IFM) API.
* Overhauled error handling and added new exception types.
* Certain request models now accept either a server-assigned ID or the corresponding object
  (``ConnectionConfig``, ``Ruleset``) for entity-reference fields.
* Added ``token_source`` callable-based authentication
  to both ``DataMasqueInstanceConfig`` and ``DataMasqueIfmInstanceConfig``
  as an alternative to ``password``.
* Ruleset is now mandatory on masking run requests.
* Fixed file data discovery API to accept both JSON path and standard locators.
* Replaced the CSV-only ``get_rulesets_generated_from_csv`` with ``get_generated_rulesets``,
  which handles all three async-ruleset-generation flows (CSV, column selection, file selection).

0.6.3 (2026-04-10)
------------------

* Added ``db2i`` to ``DatabaseType`` enum.

0.6.2 (2026-03-17)
------------------

* Added ``RULESET_LIBRARY_MANAGER`` user role.
* Fixed superuser role value (``admin`` instead of empty string).
* Superusers can now be created via the users API.
* Fixed API field for user roles (``user_roles`` instead of ``roles``/``is_superuser``).

0.6.1 (2026-03-16)
------------------

* Added ``InvalidLibraryError`` exception type.

0.6.0 (2026-03-11)
------------------

* Added support for ruleset libraries.
* Removed ``too_big`` from ruleset validation statuses (no longer used).
* Migrated toolchain to ``uv`` with ``ruff``.
* Added support for ``validating`` run status.

0.5.1 (2026-03-10)
------------------

* Added ``delete_user_by_id_if_exists`` and ``delete_user_by_username_if_exists``.

0.4.12 (2026-01-29)
-------------------

* Added support for downloading files.
* Fixed positional argument call in ``dmclient.py``.

0.4.11 (2025-12-11)
-------------------

* Fixed ``start_async_ruleset_generation_from_csv`` to use new file upload specification.

0.4.10 (2025-12-10)
-------------------

* Fixed issue with file uploads when request was retried after a 401 response.

0.4.9 (2025-11-26)
------------------

* Added ``get_run_report`` and ``start_schema_discovery_run`` endpoints.

0.4.8 (2025-09-19)
------------------

* Updated ``admin_install`` endpoint to support username parameter

0.4.7 (2025-08-29)
------------------

* Added support for Redshift

0.4.6 (2025-07-18)
------------------

* Added support for ``engine_options`` in database connection config
* Updated ``ruleset`` endpoint to use ``upsert`` behaviour
* Updated Snowflake connection handling for encrypted connection strings

0.4.5 (2025-06-30)
------------------

* Added support for ``hash_columns`` in ruleset generator requests.

0.4.4 (2025-06-09)
------------------

* Added support for Azure Blob Storage as a Snowflake staging platform.

0.4.3 (2025-05-16)
------------------

* Added support for specifying Snowflake staging platform.

0.4.2 (2025-04-03)
------------------

* Added support for Snowflake keypair authentication.

0.4.1 (2025-03-25)
------------------

* Made snowflake role field optional.

0.4.0 (2025-03-17)
------------------

* Added support for Snowflake connections.

0.3.0 (2024-10-24)
------------------

* Added support for asynchronous ruleset generation with ``start_async_ruleset_generation``.
* Added support for CSV-based ruleset generation with ``start_async_ruleset_generation_from_csv`` and ``get_rulesets_generated_from_csv``.

0.2.9 (2024-09-27)
------------------

* Added support for the ``dynamo_default_sse`` configuration option on DynamoDB connections.

0.2.7 (2024-08-26)
------------------

* Fixed the user creation API.

0.2.6 (2024-08-09)
------------------

* Removed the ``run_not_started`` pseudo-status from the ``MaskingRunStatus`` enum.
* Added support for the ``data_encoding`` connection parameter on MySQL and MariaDB.

0.2.5 (2024-08-07)
------------------

* Added support for the ``finished_with_warnings`` run status.

0.2.4 (2024-08-01)
------------------

* Added support for MSSQL Linked Server connections.

0.2.3 (2024-07-30)
------------------

* Fixed ``set_locality`` passing in "locality" rather than "region".

0.2.2 (2024-07-29)
------------------

* Add support for passing a filename or StringIO when uploading a license
* Add handling for HTTP 502 errors

0.2.1 (2024-07-23)
------------------

* Add Ruleset model
* Fix numerous issues with the new Connection models
* Introduce a separate model for Dynamo connections

0.2.0 (2024-07-22)
------------------

* Drastic simplification of the config models
* Add new features:
    * file data discovery
    * file ruleset generation
    * locality
    * seed file deletion
    * list connections and delete connections
    * user APIs
* Use v2 ruleset generation API

0.1.2 (2024-01-22)
------------------

* Export RunID, remove RunFailureReason
* Run tests using Tox against Python 3.9 and above

0.1.1 (2024-01-19)
------------------

* First release
