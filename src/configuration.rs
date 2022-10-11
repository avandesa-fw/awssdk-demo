use aws_sdk_dynamodb::{Config as DynamoConfig, Endpoint};
use aws_types::region::Region;
use aws_types::SdkConfig;
use color_eyre::eyre::{eyre, Result, WrapErr};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct BridgeServiceConfig {
    /// Override for the AWS endpoint provider, used to point the service at LocalStack
    pub override_aws_endpoint: Option<String>,
    pub dynamo_table_name: String,
}

impl BridgeServiceConfig {
    /// Load settings for the service. It will take values from these sources in the following order:
    /// - configuration/base.toml
    /// - configuration/local.toml or production.toml, depending on the APP_ENVIRONMENT variable
    /// - Environment variables
    pub fn load() -> Result<Self> {
        tracing::info!("Loading app config");

        let base_path =
            std::env::current_dir().wrap_err("Failed to determine the current directory")?;
        let configuration_directory = base_path.join("configuration");

        // Detect the running environment.
        // Default to `local` if unspecified.
        let environment: Environment = std::env::var("APP_ENVIRONMENT")
            .unwrap_or_else(|_| "local".into())
            .try_into()
            .wrap_err("Failed to parse APP_ENVIRONMENT")?;
        let environment_file =
            configuration_directory.join(format!("{}.toml", environment.as_str()));
        tracing::info!("Using config file: {}", environment_file.to_string_lossy());

        let settings = config::Config::builder()
            .add_source(config::File::from(
                configuration_directory.join("base.toml"),
            ))
            .add_source(config::File::from(environment_file))
            // Add in settings from environment variables (with a prefix of APP and '__' as separator)
            // E.g. `APP_APPLICATION__PORT=5001 would set `Settings.application.port`
            .add_source(
                config::Environment::with_prefix("APP")
                    .prefix_separator("__")
                    .separator("__"),
            )
            .build()?;

        settings.try_deserialize().map_err(Into::into)
    }
}

pub struct AwsConfig {
    pub aws: SdkConfig,
    pub dynamo: DynamoConfig,
    // TODO: kinesis config
}

impl AwsConfig {
    /// Load the AWS configuration from the environment, optionally overriding the endpoint for all
    /// services
    #[tracing::instrument(skip_all)]
    pub async fn load(override_aws_endpoint: Option<String>) -> Result<AwsConfig> {
        tracing::info!("Loading AWS config from environment");
        let mut aws_config = aws_config::from_env();

        if let Some(override_aws_endpoint) = override_aws_endpoint {
            tracing::info!("Overriding AWS endpoint to {}", override_aws_endpoint);
            let uri = override_aws_endpoint
                .parse()
                .wrap_err("Invalid URI for override_aws_endpoint")?;
            aws_config = aws_config
                .region(Region::new("us-east-1"))
                .endpoint_resolver(Endpoint::immutable(uri));
        }

        let aws_config = aws_config.load().await;
        let dynamodb_config = aws_sdk_dynamodb::config::Builder::from(&aws_config).build();

        Ok(AwsConfig {
            aws: aws_config,
            dynamo: dynamodb_config,
        })
    }
}

/// The possible runtime environment for our application.
pub enum Environment {
    Local,
    Production,
}

impl Environment {
    pub fn as_str(&self) -> &'static str {
        match self {
            Environment::Local => "local",
            Environment::Production => "production",
        }
    }
}

impl TryFrom<String> for Environment {
    type Error = color_eyre::Report;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "local" => Ok(Self::Local),
            "production" => Ok(Self::Production),
            other => Err(eyre!(
                "`{}` is not a supported environment. Use either `local` or `production`",
                other
            )),
        }
    }
}
