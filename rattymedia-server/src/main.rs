pub mod media;

use color_eyre::Result;
use env_logger::Env;
use log::{debug, info};
use media::windows::{ManagerEvent::*, SessionUpdateEvent::*};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let mut rx = media::windows::SessionManager::create().await?;

    while let Some(evt) = rx.recv().await {
        match evt {
            SessionCreated {
                session_id,
                mut rx,
                source,
            } => {
                info!("Created session: {{id={session_id}, source={source}}}");
                tokio::spawn(async move {
                    while let Some(evt) = rx.recv().await {
                        match evt {
                            Model(model) => {
                                info!(session_id, source, model:?; "Model update")
                            }
                            Media(model, image) => info!(
                                session_id, source, model:?, has_image=image.is_some(); "Media update"
                            ),
                        }
                    }
                    info!(session_id, source;"exited event-loop");
                });
            }
            SessionRemoved { session_id } => info!(session_id;"Session removed"),
            CurrentSessionChanged {
                session_id: Some(session_id),
            } => info!(session_id;"New current session"),
            CurrentSessionChanged { session_id: None } => info!("No more current session"),
        }
    }
    info!("Exited global event-loop");

    Ok(())
}
