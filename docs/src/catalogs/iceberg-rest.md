---
icon: lucide/database
---

# Iceberg REST

Use `[[catalog.iceberg-rest]]` to access existing Iceberg tables through a REST
catalog. Lakelet reuses `iceberg-rust` for REST requests and reads.

| Option | Type | Required | Description |
| --- | --- | --- | --- |
| `name` | String | Yes | Unique Lakelet catalog name. |
| `uri` | String | Yes | HTTP(S) REST catalog base URL. |
| `warehouse` | String | No | Warehouse identifier. |
| `token` | String | No | Bearer token for authenticating requests to the REST service. |



For a service requiring bearer authentication:

```toml
[[catalog.iceberg-rest]]
name = "iceberg_prod"
uri = "https://catalog.example.com"
warehouse = "warehouse_name"
token = "<REST catalog access token>"
```

To override credentials supplied by the REST catalog for S3, set `s3-storage`
in the catalog configuration.

```toml
[[catalog.iceberg-rest]]
name = "iceberg_prod"
uri = "https://catalog.example.com"
warehouse = "warehouse_name"
s3-storage = { region = "us-east-1", access-key = "<access key>", secret-key = "<secret key>" }
```

## Storage Credentials

Lakelet resolves credentials per location scheme, in this order:

1. This catalog's own `s3-storage` / `oss-storage` block. It suppresses the vended
   credentials for that scheme.
2. Credentials vended by the catalog, when the block above is absent.
3. The backend's own credential chain (environment variables, shared profile,
   instance metadata), when neither of the above supplies the scheme.