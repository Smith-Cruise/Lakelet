---
icon: lucide/hard-drive
---

# Storage

Storage configuration is attached to catalogs and supplies the
credentials needed to access table data.

| Location scheme | Configuration |
| --- | --- |
| `s3://`, `s3a://` | `s3-storage` |
| `oss://` | `oss-storage` |
| `hdfs://` | None |

Storage configuration is optional. Without storage credentials in config,
Lakelet will authenticate with `OpenDAL`'s default credential chain:
environment variables, a shared profile, or instance metadata.

## AWS S3 / S3-Compatible Storage <span id="aws-s3-s3-compatible-storge"></span>

Configure S3-compatible storage with the `s3-storage` inline table.

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `region` | String | No | `AWS_REGION` env | AWS region used for requests. Falls back to the `AWS_REGION` / `AWS_DEFAULT_REGION` environment variables; errors if none is available. |
| `endpoint` | String | No | `AWS_ENDPOINT_URL` env | Custom endpoint for S3-compatible services such as MinIO. Falls back to the `AWS_ENDPOINT_URL` / `AWS_ENDPOINT` / `AWS_S3_ENDPOINT` environment variables. Must not include the bucket name. |
| `access-key` | String | No | Credential chain | Access key. When unset, the env/profile/IMDS credential chain is used. |
| `secret-key` | String | No | Credential chain | Secret key. |
| `session-token` | String | No | None | Session token for temporary (STS) credentials. |
| `path-style-access` | Boolean | No | `false` | Uses path-style requests when `true`; otherwise uses virtual-hosted-style requests. |

```toml
# Minio/Rust-FS demo
[[catalog.hms]]
name = "hms"
metastore-uri = "127.0.0.1:9083"
s3-storage = { region = "us-east-1", endpoint = "http://127.0.0.1:9000", access-key = "admin", secret-key = "password", path-style-access = true }
```

Self-hosted S3-compatible services(like MinIO/Rust-FS) usually set `path-style-access = true`.

## Aliyun OSS

Configure Aliyun OSS with the `oss-storage` inline table. Unlike S3, OSS has no
environment fallback for the endpoint, so a block carrying at least `endpoint` is
always required; the keys themselves may still come from
`ALIBABA_CLOUD_ACCESS_KEY_ID` / `ALIBABA_CLOUD_ACCESS_KEY_SECRET` or an ECS RAM
role.

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `endpoint` | String | Yes | None | OSS endpoint, for example `https://oss-cn-hangzhou.aliyuncs.com`. Must not include the bucket name. It has no environment fallback. |
| `access-key` | String | No | None | Static access key ID. |
| `secret-key` | String | No | None | Static access key secret. |
| `path-style-access` | Boolean | No | `false` | Uses path-style requests when `true`; otherwise uses virtual-hosted-style requests. |

```toml
# demo
[[catalog.hms]]
name = "hms"
metastore-uri = "127.0.0.1:9083"
oss-storage = { endpoint = "https://oss-cn-hangzhou.aliyuncs.com", access-key = "access-key", secret-key = "secret-key" }
```

## HDFS

Don't need to configure anything.

Kerberos is not supported.