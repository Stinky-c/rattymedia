use color_eyre::Result;

#[async_trait::async_trait]
pub trait SessionControl {
    async fn get_session(&self) -> Result<Option<impl MediaSession>>;
    // async fn find_session<F>(&self, func: F) where F: FnOnce() ->
}

/// A currently available controllable session
///
/// - Windows: [GlobalSystemMediaTransportControlsSession](https://learn.microsoft.com/en-us/uwp/api/windows.media.control.globalsystemmediatransportcontrolssession?view=winrt-28000)
/// - Mpris: [Player](https://docs.rs/mpris/latest/mpris/struct.Player.html)
#[async_trait::async_trait]
pub trait MediaSession {
    async fn title(&self) -> Result<String>;
    async fn subtitle(&self) -> Result<String>;
    async fn artist(&self) -> Result<String>;
    async fn album_title(&self) -> Result<String>;
    async fn thumbnail(&self) -> Result<Option<Vec<u8>>>; // TODO: image crate

    async fn pause(&self) -> Result<()>;
    async fn play(&self) -> Result<()>;
    async fn pauseplay(&self) -> Result<()>;


}
