from meltano.core.setting_definition import SettingDefinition, SettingKind


PROTOCOL = SettingDefinition(
    name="state_backend.fs.protocol",
    description="The protocol of the filesystem to use.",
    kind=SettingKind.STRING,
)

STORAGE_OPTIONS = SettingDefinition(
    name="state_backend.fs.storage_options",
    description="The storage options to use.",
    kind=SettingKind.OBJECT,
)

S3_KEY = SettingDefinition(
    name="state_backend.fs.storage_options.s3.key",
    description="The AWS Access Key ID for S3.",
    kind=SettingKind.STRING,
    sensitive=True,
)
S3_SECRET = SettingDefinition(
    name="state_backend.fs.storage_options.s3.secret",
    description="The AWS Secret Access Key for S3.",
    kind=SettingKind.STRING,
    sensitive=True,
)
S3_ENDPOINT_URL = SettingDefinition(
    name="state_backend.fs.storage_options.s3.endpoint_url",
    description="The URL of the S3 endpoint to use.",
    kind=SettingKind.STRING,
)

GCS_PROJECT = SettingDefinition(
    name="state_backend.fs.storage_options.gcs.project",
    description="The project for the GCS endpoint to use.",
    kind=SettingKind.STRING,
)
GCS_TOKEN = SettingDefinition(
    name="state_backend.fs.storage_options.gcs.token",
    description="The token for the GCS endpoint to use.",
    kind=SettingKind.STRING,
    sensitive=True,
)
GCS_ENDPOINT_URL = SettingDefinition(
    name="state_backend.fs.storage_options.gcs.endpoint_url",
    description="The URL of the GCS endpoint to use.",
    kind=SettingKind.STRING,
)

AZURE_CONNECTION_STRING = SettingDefinition(
    name="state_backend.fs.storage_options.azure.connection_string",
    description="The connection string for the Azure endpoint to use.",
    kind=SettingKind.STRING,
    sensitive=True,
)
AZURE_ACCOUNT_NAME = SettingDefinition(
    name="state_backend.fs.storage_options.azure.account_name",
    description="The name of the Azure account to use.",
    kind=SettingKind.STRING,
)
AZURE_ACCOUNT_KEY = SettingDefinition(
    name="state_backend.fs.storage_options.azure.account_key",
    description="The key of the Azure account to use.",
    kind=SettingKind.STRING,
    sensitive=True,
)

SFTP_HOST = SettingDefinition(
    name="state_backend.fs.storage_options.sftp.host",
    description="The hostname of the SFTP server.",
    kind=SettingKind.STRING,
)
SFTP_PORT = SettingDefinition(
    name="state_backend.fs.storage_options.sftp.port",
    description="The port of the SFTP server. Defaults to 22.",
    kind=SettingKind.INTEGER,
    value=22,
)
SFTP_USERNAME = SettingDefinition(
    name="state_backend.fs.storage_options.sftp.username",
    description="The username for SFTP authentication.",
    kind=SettingKind.STRING,
)
SFTP_PASSWORD = SettingDefinition(
    name="state_backend.fs.storage_options.sftp.password",
    description="The password for SFTP authentication.",
    kind=SettingKind.STRING,
    sensitive=True,
)
SFTP_PRIVATE_KEY = SettingDefinition(
    name="state_backend.fs.storage_options.sftp.pkey",
    description="The content of the private key for SFTP authentication. Must be in PEM format. Supported key types are RSA, ECDSA, and Ed25519.",
    kind=SettingKind.STRING,
    sensitive=True,
)
SFTP_PRIVATE_KEY_FILE = SettingDefinition(
    name="state_backend.fs.storage_options.sftp.key_filename",
    description="The path to the private key file for SFTP authentication.",
    kind=SettingKind.STRING,
)
SFTP_PRIVATE_KEY_PASS = SettingDefinition(
    name="state_backend.fs.storage_options.sftp.passphrase",
    description="The passphrase for the private key if encrypted.",
    kind=SettingKind.STRING,
    sensitive=True,
)
