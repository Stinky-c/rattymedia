// #![no_std]
use mousefood::embedded_graphics::mock_display::MockDisplay;
use mousefood::prelude::Rgb565;
use mousefood::{EmbeddedBackend, EmbeddedBackendConfig};
use ratatui::Terminal;
use rattymedia_app::App;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut display = MockDisplay::<Rgb565>::new();
    let config = EmbeddedBackendConfig {
        ..Default::default()
    };
    let backend = EmbeddedBackend::new(&mut display, config);

    let terminal = Terminal::new(backend)?;

    let mut app = App::new(terminal);

    loop {
        app.try_run()?;
    }
}
