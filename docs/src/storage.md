# Storage

Storage configuration is attached to an HMS or Glue catalog and supplies the
properties needed to access table data.

| Location scheme | Configuration |
| --- | --- |
| `s3://`, `s3a://` | `s3-storage` |
| `oss://` | `oss-storage` |
| `hdfs://` | No storage block; the NameNode authority comes from the location. |

A location whose scheme has no matching storage block fails with a clear
error. In particular, Delta tables on S3 no longer fall back to environment
credentials: configure `s3-storage` explicitly.

## AWS S3

Configure S3-compatible storage with the `s3-storage` inline table.

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `region` | String | No | `us-east-1` | AWS region used for requests. |
| `endpoint` | String | No | AWS default | Custom endpoint for S3-compatible services such as MinIO. Must not include the bucket name. |
| `access-key` | String | No | Credential chain | Access key. When both keys are set, the env/profile/IMDS credential chain is skipped. |
| `secret-key` | String | No | Credential chain | Secret key. |
| `path-style-access` | Boolean | No | `false` | Uses path-style requests when `true`; otherwise uses virtual-hosted-style requests. |

```toml
[[catalog.hms]]
name = "hms"
metastore-uri = "127.0.0.1:9083"
s3-storage = { region = "us-east-1", endpoint = "http://127.0.0.1:9000", access-key = "admin", secret-key = "password", path-style-access = true }
```

## Aliyun OSS

Configure Aliyun OSS with the `oss-storage` inline table.

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `endpoint` | String | Yes | None | Bare regional OSS endpoint, for example `https://oss-cn-hangzhou.aliyuncs.com`. Must not include the bucket name; the bucket prefix is added automatically for virtual-hosted-style requests. |
| `access-key` | String | No | None | Static access key ID. |
| `secret-key` | String | No | None | Static access key secret. |
| `path-style-access` | Boolean | No | `false` | Uses path-style requests when `true`; otherwise uses virtual-hosted-style requests. |

> Breaking change: earlier releases required the bucket name inside the OSS
> `endpoint` (`https://<bucket>.oss-<region>.aliyuncs.com`). Remove the bucket
> prefix when upgrading.

```toml
[[catalog.hms]]
name = "hms"
metastore-uri = "127.0.0.1:9083"
oss-storage = { endpoint = "https://oss-cn-hangzhou.aliyuncs.com", access-key = "access-key", secret-key = "secret-key", path-style-access = false }
```

## HDFS

HDFS locations need no storage block: the NameNode authority (`host:port` or
an HA nameservice resolvable through `HADOOP_CONF_DIR`) is taken from the
table location itself, for example `hdfs://namenode:8020/warehouse/db/table`.
Kerberos is not supported yet.