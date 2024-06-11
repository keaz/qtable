use clap::Parser;
use config::{Config, ConfigError};

static DEFAULT_CONFIG_PATH: &str = "./config/qtable/config.toml";

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cmd {
    #[arg(short, long, default_value_t=String::from(DEFAULT_CONFIG_PATH))]
    pub config_path: String,
}

#[derive(Debug, serde::Deserialize)]
pub struct ServerConfig {
    pub data_path: String,
}

impl ServerConfig {
    pub fn new() -> Result<Self, ConfigError> {
        let args = Cmd::parse();
        let x = Config::builder()
            .add_source(config::File::with_name(args.config_path.as_str()))
            .add_source(config::Environment::with_prefix("QTABLE_"))
            .build()?;

        x.try_deserialize()
    }
}
