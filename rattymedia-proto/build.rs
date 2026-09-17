fn main() {
    let mut config = prost_build::Config::new();
    config
        .btree_map(&["."])
        .compile_protos(&["proto/media.proto"], &["proto/"])
        .unwrap();
}
