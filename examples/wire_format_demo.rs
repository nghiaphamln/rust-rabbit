//! Simple test to show wire message format

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TestMessage {
    id: u32,
    content: String,
}

fn main() {
    let test_msg = TestMessage {
        id: 123,
        content: "Hello World!".to_string(),
    };

    // Show direct serialization (old way)
    let direct_json = serde_json::to_string_pretty(&test_msg).unwrap();
    println!("Direct message (old format):");
    println!("{}", direct_json);
    println!();

    // Show WireMessage format (new way)
    let wire_msg = rust_rabbit::WireMessage {
        data: test_msg,
        retry_attempt: 0,
    };

    let wire_json = serde_json::to_string_pretty(&wire_msg).unwrap();
    println!("WireMessage format (new format):");
    println!("{}", wire_json);
    println!();

    println!("Publisher now wraps all messages in WireMessage format");
    println!("Consumer expects and deserializes WireMessage format");
    println!("Message includes retry_attempt tracking");
}
