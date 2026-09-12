//! User input loop implementation

pub enum InputEvent {
    Noop,
    Click(u16, u16),
    Resize(u16, u16),
    Quit,
}
