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

The `state_backend.fs.protocol` setting is required, and it can be any FSSpec-supported protocol.

The S3, GCS, Azure, and SFTP storage options have first-class settings support:

- S3:
  - `state_backend.fs.storage_options.s3.key`
  - `state_backend.fs.storage_options.s3.secret`
  - `state_backend.fs.storage_options.s3.endpoint_url`
  - `state_backend.fs.storage_options.s3.region`
- GCS:
  - `state_backend.fs.storage_options.gcs.project`
  - `state_backend.fs.storage_options.gcs.token`
  - `state_backend.fs.storage_options.gcs.endpoint_url`
- Azure:
  - `state_backend.fs.storage_options.azure.connection_string`
  - `state_backend.fs.storage_options.azure.account_name`
  - `state_backend.fs.storage_options.azure.account_key`
- SFTP:
  - `state_backend.fs.storage_options.sftp.host`
  - `state_backend.fs.storage_options.sftp.port`
  - `state_backend.fs.storage_options.sftp.username`
  - `state_backend.fs.storage_options.sftp.password`
  - `state_backend.fs.storage_options.sftp.pkey` (key content)
  - `state_backend.fs.storage_options.sftp.key_filename` (path to key file)
  - `state_backend.fs.storage_options.sftp.passphrase` (passphrase for the private key if encrypted)

### Arbitrary storage options

If you need to use a filesystem that FSSpec does not support out of the box, you can use the `state_backend.fs.storage_options` setting to configure the storage options:

```shell
meltano config meltano set state_backend.fs.storage_options '{"sftp.foo": "bar", "sftp.baz": "qux"}'
```

### Examples

#### S3

```yaml
state_backend:
  uri: fs://path/to/state
  protocol: s3
  storage_options:
    s3:
      key: my_key
      secret: my_secret
      endpoint_url: https://my-endpoint.com
```

#### SFTP with Password Authentication

```yaml
state_backend:
  uri: fs://path/to/state
  protocol: sftp
  storage_options:
    sftp:
      host: sftp.example.com
      port: 22
      username: my_user
      password: my_password
```

#### SFTP with SSH Key Authentication (using key file)

```yaml
state_backend:
  uri: fs://path/to/state
  protocol: sftp
  storage_options:
    sftp:
      host: sftp.example.com
      port: 22
      username: my_user
      key_filename: /path/to/private/key
      passphrase: optional_passphrase
```

#### SFTP with SSH Key Authentication (using key content)

```yaml
state_backend:
  uri: fs://path/to/state
  protocol: sftp
  storage_options:
    sftp:
      host: sftp.example.com
      port: 22
      username: my_user
      pkey: |
        -----BEGIN RSA PRIVATE KEY-----
        ...key content here...
        -----END RSA PRIVATE KEY-----
      passphrase: optional_passphrase
```
