pub mod media;
use crate::media::{WindowsMedia, abc::*};
use color_eyre::Result;
use windows::Foundation::TypedEventHandler;
use windows::Media::Control::{
    GlobalSystemMediaTransportControlsSession, GlobalSystemMediaTransportControlsSessionManager,
    MediaPropertiesChangedEventArgs, SessionsChangedEventArgs,
};
use windows::core::Ref;

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;

    let manager = GlobalSystemMediaTransportControlsSessionManager::RequestAsync()?.await?;
    let v = manager.GetSessions()?;
    let session = manager.GetCurrentSession()?;
    let id = session.SourceAppUserModelId()?;

    let token = session.MediaPropertiesChanged(&TypedEventHandler::new(event))?;
    // let token = manager.SessionsChanged(&TypedEventHandler::new(event));
    println!("Session Token: {:?}", token);

    tokio::signal::ctrl_c().await?;

    Ok(())
}

fn session_changed_event(
    _sender: Ref<GlobalSystemMediaTransportControlsSessionManager>,
    args: Ref<SessionsChangedEventArgs>,
) -> windows::core::Result<()> {
    let v = args.unwrap();
    println!("{:?}", v);
    Ok(())
}

fn event(
    sender: Ref<GlobalSystemMediaTransportControlsSession>,
    args: Ref<MediaPropertiesChangedEventArgs>,
) -> windows::core::Result<()> {
    let v = args.unwrap();
    println!("{:?}", v);
    Ok(())
}
