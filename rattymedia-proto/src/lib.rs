mod media {
    super::proto!("rattymedia.media");
}

#[macro_export]
macro_rules! proto {
    ($package:literal) => {
        include!(concat!(env!("OUT_DIR"), "/", $package, ".rs"));
    };
}
