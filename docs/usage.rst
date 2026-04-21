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
