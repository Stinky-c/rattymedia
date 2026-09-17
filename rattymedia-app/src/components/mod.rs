use crate::app::AppState;
use ratatui::widgets::StatefulWidget;
// pub(crate) mod fps;

trait Component: StatefulWidget {
    fn tick(&mut self, state: &mut AppState);
}
