use base64::Engine;

fn main() {
    let value = fastrand::u32(..);
    let mut buffer = itoa::Buffer::new();
    let text = buffer.format(value);
    println!(
        "{} {} {}",
        text,
        hex::encode(value.to_be_bytes()),
        base64::engine::general_purpose::STANDARD.encode(value.to_be_bytes())
    );
}
