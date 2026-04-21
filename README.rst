=================
datamasque-python
=================

Official Python client for the `DataMasque <https://datamasque.com/>`_ platform.

DataMasque is a data masking platform that replaces sensitive data with realistic but non-production values,
so teams can use production-shaped data in non-production environments without exposing PII.
This package is a thin Python wrapper around the DataMasque server's HTTP API,
covering connection management, ruleset and ruleset-library CRUD,
masking run lifecycle, discovery results, user administration, and license management.

Installation
============

.. code-block:: console

    pip install datamasque-python

Python 3.9 or newer is required.

Quickstart
==========

.. code-block:: python

    from datamasque.client import DataMasqueClient
    from datamasque.client.models.dm_instance import DataMasqueInstanceConfig

    config = DataMasqueInstanceConfig(
        base_url="https://datamasque.example.com",
        username="api_user",
        password="api_password",
    )
    client = DataMasqueClient(config)
    client.authenticate()

    for connection in client.list_connections():
        print(connection.name)

Authentication is performed on the first request if ``authenticate()`` is not called explicitly,
and is automatically retried once on a 401 response.
``client.healthcheck()`` is available as a lightweight readiness probe that does not consume credentials.

Error handling
==============

All methods raise subclasses of ``DataMasqueException`` on failure:

- ``DataMasqueApiError`` —
  the server responded with a non-2xx status (excluding 502).
  The triggering ``Response`` is available on the ``.response`` attribute.
- ``DataMasqueNotReadyError`` —
  the server responded with 502,
  typically because it is still starting up.
- ``DataMasqueTransportError`` —
  the request failed before any response was received
  (connection refused, timeout, DNS failure, SSL handshake failure, etc.).
- ``FailedToStartError`` / ``InvalidRulesetError`` / ``InvalidLibraryError`` —
  raised by ``start_masking_run`` when the server rejects the run.
- ``DataMasqueUserError`` —
  raised by user-management methods when the input is invalid.

Documentation
=============

- All classes and functions have docstrings and type hints.
- Compiled docs are hosted at `Read the Docs: datamasque-python <https://datamasque-python.readthedocs.io/>`_.
- Documentation for the DataMasque product, including a full API reference,
  can be found on the `DataMasque portal <https://portal.datamasque.com/portal/documentation/>`_.

Contributing
============

See `CONTRIBUTING.rst <CONTRIBUTING.rst>`_ for development setup, testing, and the pull request flow.

License
=======

Apache License 2.0.
See `LICENSE <LICENSE>`_.
