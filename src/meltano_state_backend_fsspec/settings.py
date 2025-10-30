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
