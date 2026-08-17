=======
History
=======

1.2.5 (2026-08-17)
------------------

* Added ``iam_role_arn`` to ``DatabaseConnectionConfig``: the IAM role DataMasque assumes to tag
  the connection's AWS resource, for resources in another AWS account. Sent only for the engines
  that can be an Amazon RDS instance, Aurora cluster or Redshift cluster.

Requires server version 3.26.16

1.2.4 (2026-08-12)
------------------

* Added ``rg_config`` to ``ValidationErrorType``, covering ruleset generation config validation errors.

Requires server version 3.26.15

1.2.3 (2026-08-12)
------------------

* Added file-discovery value counts:

  * ``value_count`` on ``FileDiscoveryLocatorResult``, the total over the files in the group.
  * ``value_counts`` and ``value_count_status`` on ``FileDiscoveryFile``, typed by ``ValueCountStatus``.

Requires server version 3.26.15

1.2.2 (2026-07-31)
------------------

* Added ``validation_error_details`` to ``DiscoveryConfig``.
* Added ``usage_count`` to ``DiscoveryConfigLibrary``.
* ``create_discovery_config``, ``update_discovery_config``, ``create_discovery_config_library``, and
  ``update_discovery_config_library`` now raise ``ValueError`` when the passed entity has no ``yaml``
  content, instead of sending a request the server rejects.

Requires server version 3.26.14

1.2.1 (2026-07-30)
------------------

* Added ruleset generation configuration

1.2.0 (2026-07-27)
------------------

* Made discovery config libraries untyped, matching the DataMasque 3.26.14 server. A single
  library is now identified by ``(namespace, name)`` and may be imported by both database and
  file discovery configs. **Breaking:** removed the ``config_type`` field from
  ``DiscoveryConfigLibrary`` and the ``config_type`` argument from
  ``get_discovery_config_library_by_name``, ``create_or_update_discovery_config_library``, and
  ``delete_discovery_config_library_by_name_if_exists``. Requires a 3.26.14 or later server.

1.1.8 (2026-07-27)
------------------

* Added typed Safe Data Preview support:

  * ``safe_data_preview`` on ``InDataDiscoveryConfig`` (via ``SafeDataPreviewOptions``) to configure or disable it.
  * ``safe_data_preview`` on schema-discovery result columns and file-discovery locators, typed by ``kind``.

* Added ``row_count`` to ``TableConstraints`` in schema-discovery ``table_metadata``.
* Added table-reference management APIs (``list_table_references``, ``create_table_reference``, and friends),
  along with the ``TableReference`` and ``TableReferenceOptions`` models.

1.1.7 (2026-07-14)
------------------

* Added ``retry_writes`` to ``MongoConnectionConfig`` (default ``True``), serialized only when
  disabled. Set it to ``False`` for AWS DocumentDB, which rejects retryable writes.
* Updated the validation error API on ``Ruleset`` and ``RulesetLibrary``.

1.1.6 (2026-07-13)
------------------

* Added discovery-config-library management APIs (``list_discovery_config_libraries``, ``create_discovery_config_library``, and friends).

1.1.5 (2026-06-29)
------------------

* Added support for DataMasque deployments on Snowpark Container Services (SPCS):

  * Added ``spcs_pat`` to ``DataMasqueInstanceConfig`` for authenticating through the SPCS app gateway.
  * Added ``SpcsGatewayAuthError``, raised when the gateway rejects the PAT.
  * Added the ``spcs`` option to ``SnowflakeStageLocation``.
  * Made several ``SnowflakeConnectionConfig`` fields optional, since SPCS-staged connections leave them unset.

1.1.4 (2026-06-29)
------------------

* Added ``DatabaseType.saphana`` for SAP HANA support.

1.1.3 (2026-06-26)
------------------

* Added ``tls_ca_file`` and ``tls_allow_invalid_certificates`` to ``MongoConnectionConfig``,
  serialized only when TLS is enabled (``tls_ca_file`` only when set,
  ``tls_allow_invalid_certificates`` only when ``True``).
* Added ``DatabaseType.documentdb`` and ``DocumentDbConnectionConfig`` for AWS DocumentDB,
  which is MongoDB wire-compatible and reuses ``MongoConnectionConfig`` (differing only by ``db_type``).

1.1.2 (2026-06-26)
------------------

* Completed wiring of the ``finished_with_warnings`` status for ``AsyncRulesetGenerationTaskStatus``.
* Added the ``cancelled`` status to ``AsyncRulesetGenerationTaskStatus``.

1.1.1 (2026-06-25)
------------------

* Made ``DiscoveryMatch.label`` optional (it is absent for non-sensitive/ignore matches).
* Added the ``finished_with_warnings`` status to ``AsyncRulesetGenerationTaskStatus``.
* ``get_db_discovery_result_report`` may now return ``bytes`` (a zip)
  when the server splits a large DB-discovery report,
  and ruleset generation from CSV now detects and forwards zip uploads.

1.1.0 (2026-06-24)
------------------

* Added discovery configuration models and management APIs.
* Added schema-discovery and file-data-discovery APIs that take a saved discovery configuration
  (``start_schema_discovery_run_from_config`` / ``start_file_data_discovery_run_from_config``).
  Adoption is recommended; the older APIs that take individual options will be deprecated in a future release.
* Corrected the file-data-discovery ``include``/``skip`` filter syntax and added ``ignore_rules`` support.
* Added ``InvalidDiscoveryConfigError`` and ``DiscoveryConfigNotFoundError``,
  raised when a discovery run can't start due to an unusable or missing discovery config.
* Added ``get_discovery_run_config_snapshot_yaml`` to retrieve the discovery-config YAML
  that was effective at the start of a given discovery run.
* Added ``is_user_subscribed`` to ``MaskingRunRequest`` to subscribe the requesting user to a run's email notifications.
* Added ``auto_pull`` / ``auto_pull_branch`` to ``MaskingRunOptions``
  to refresh the run's ruleset from git before starting.
* Added ``validation_error`` (and ``validation_error_type`` for rulesets) to ``Ruleset`` and ``RulesetLibrary``.
* Exposed git provenance on ``Ruleset`` and ``RulesetLibrary`` as a nested ``git`` field (``GitSnapshot``).
* Read-only fields (``id``, ``is_valid``, ``validation_error``, etc.)
  are no longer echoed back in ``Ruleset`` / ``RulesetLibrary`` create/update request bodies.
* Fixed ``SslZipFile`` uploads to send the required ``database_type=mysql`` form field.
* **Breaking:** ``delete_ruleset_by_name_if_exists`` now requires a ``ruleset_type`` argument,
  since ruleset names are unique only per type.

1.0.5 (2026-06-18)
------------------

* Renamed the ``DatabaseType.sql_server`` member to ``DatabaseType.mssql`` to match the DataMasque server's wire value and the sibling ``mssql_linked`` member. The value is unchanged (``"mssql"``).

1.0.4 (2026-06-09)
------------------

* Added ``informix`` to ``DatabaseType`` enum.
* Pool HTTP connections via a per-client ``requests.Session`` so TCP/TLS connections are reused across calls. Note: a client is not thread-safe; construct one per worker.
* Send a descriptive ``User-Agent`` identifying the SDK name, version, Python interpreter, and OS.
* Only re-authenticate and replay on a ``401`` for requests that actually sent a token (gate the retry on ``requires_authorization``).

1.0.3 (2026-05-27)
------------------

* Added ``databricks`` to ``DatabaseType`` enum.
* Removed ``DatabricksDeltaS3ConnectionConfig``.

1.0.2 (2026-05-14)
------------------

* Added ``DatabricksDeltaS3ConnectionConfig`` for Databricks Delta tables stored in S3.

1.0.1 (2026-05-11)
------------------

* Added ``databricks_lakebase`` to ``DatabaseType`` enum.

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
