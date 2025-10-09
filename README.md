# meltano-state-backend-fsspec

A state backend for the Meltano platform that uses FSSpec to store state in a filesystem.

## Features

- [X] Supports all filesystems that FSSpec supports
- [X] Supports all storage options that FSSpec supports

## Installation

```shell
uv tool install --with 'meltano-state-backend-fsspec[s3] @ https://github.com/reservoir-data/meltano-state-backend-fsspec' meltano
```

## Configuration

The `state_backend.fsspec.protocol` setting is required, and it can be any FSSpec-supported protocol.

The S3, GCS, and Azure storage options have first-class settings support:

- S3:
  - `state_backend.fsspec.storage_options.s3.key`
  - `state_backend.fsspec.storage_options.s3.secret`
  - `state_backend.fsspec.storage_options.s3.endpoint_url`
  - `state_backend.fsspec.storage_options.s3.region`
- GCS:
  - `state_backend.fsspec.storage_options.gcs.project`
  - `state_backend.fsspec.storage_options.gcs.token`
  - `state_backend.fsspec.storage_options.gcs.endpoint_url`
- Azure:
  - `state_backend.fsspec.storage_options.azure.connection_string`
  - `state_backend.fsspec.storage_options.azure.account_name`
  - `state_backend.fsspec.storage_options.azure.account_key`

### Arbitrary storage options

If you need to support a filesystem that FSSpec does not support out of the box, you can use the `state_backend.fsspec.storage_options` setting to configure the storage options:

```shell
meltano config meltano set state_backend.fsspec.storage_options '{"sftp.foo": "bar", "sftp.baz": "qux"}'
```
