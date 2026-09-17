use crate::media::abc::MediaSession;
use crate::media::{SessionControl, abc};
use color_eyre::Result;
use std::ops::Deref;
use windows::Foundation::TypedEventHandler;
use windows::Media::Control::{
    CurrentSessionChangedEventArgs, GlobalSystemMediaTransportControlsSession,
    GlobalSystemMediaTransportControlsSessionManager,
    GlobalSystemMediaTransportControlsSessionMediaProperties, MediaPropertiesChangedEventArgs,
};
use windows::core::Ref;

/*
TODO: work it out
1.
    Class owns everything.
    Watches all sessions and maintains a clone of them
    On creation register a handler to GlobalSystemMediaTransportControlsSessionManager::SessionsChanged, MediaPropertiesChanged, CurrentSessionChanged

2. short lived.
    No events, listening by default


 */

pub struct WindowsMedia {
    manager: GlobalSystemMediaTransportControlsSessionManager,
}

impl WindowsMedia {
    pub async fn new() -> Result<WindowsMedia> {
        let manager = GlobalSystemMediaTransportControlsSessionManager::RequestAsync()?.await?;

        Ok(Self { manager })
    }
}


#[async_trait::async_trait]
impl abc::SessionControl for WindowsMedia {
    async fn get_session(&self) -> Result<Option<impl MediaSession>> {
        let session = self.manager.GetCurrentSession()?;
        Ok(Some(WindowsMediaSession::new(session)?))
    }
}

pub struct WindowsMediaSession {
    inner: GlobalSystemMediaTransportControlsSession,
}

impl WindowsMediaSession {
    fn new(session: GlobalSystemMediaTransportControlsSession) -> Result<Self> {
        // let token = session.MediaPropertiesChanged(&TypedEventHandler::new(func))?;
        Ok(Self { inner: session })
    }
    fn event(
        sender: Ref<GlobalSystemMediaTransportControlsSession>,
        args: Ref<MediaPropertiesChangedEventArgs>,
    ) -> windows::core::Result<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl abc::MediaSession for WindowsMediaSession {
    async fn title(&self) -> Result<String> {
        let props = self.inner.TryGetMediaPropertiesAsync()?.await?;
        Ok(props.Title()?.to_string())
    }

    async fn subtitle(&self) -> Result<String> {
        todo!()
    }

    async fn artist(&self) -> Result<String> {
        todo!()
    }

    async fn album_title(&self) -> Result<String> {
        todo!()
    }

    async fn thumbnail(&self) -> Result<Option<Vec<u8>>> {
        todo!()
    }

    async fn pause(&self) -> Result<()> {
        self.inner.TryPauseAsync()?.await?;
        Ok(())
    }

    async fn play(&self) -> Result<()> {
        todo!()
    }

    async fn pauseplay(&self) -> Result<()> {
        self.inner.TryTogglePlayPauseAsync()?.await?;
        Ok(())
    }
}
