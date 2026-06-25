=====
Usage
=====

To use DataMasque Python in a project:

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

Connecting to an SPCS-hosted instance
=====================================

When DataMasque is hosted behind Snowflake SPCS (Snowpark Container Services)
app ingress, its ``base_url`` ends in ``.snowflakecomputing.app`` and every
request must first clear the Snowflake gateway. Pass a Snowflake Programmatic
Access Token (PAT) as ``spcs_pat`` and the client sends it on the
``X-SF-SPCS-Authorization`` header automatically; the gateway strips that header
before forwarding, so your DataMasque ``username``/``password`` (or
``token_source``) auth is unaffected.

.. code-block:: python

    config = DataMasqueInstanceConfig(
        base_url="https://my-app.snowflakecomputing.app",
        username="api_user",
        password="api_password",
        spcs_pat="<snowflake-programmatic-access-token>",
    )
    client = DataMasqueClient(config)
    client.authenticate()

Mint the PAT in Snowsight (User profile → Programmatic access tokens) for an
account that can reach the SPCS app. If the gateway rejects the PAT (for example
it has expired, or a network policy excludes your IP), the client raises
``SpcsGatewayAuthError`` with the Snowflake-provided detail and a hint at the
likely cause.
