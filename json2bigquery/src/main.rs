use std::io;

fn main() -> Result<(), serde_bigquery::Error> {
    let mut deserializer = serde_json::Deserializer::from_reader(io::stdin());
    let mut serializer = serde_bigquery::Serializer::new(io::stdout());
    serde_transcode::transcode(&mut deserializer, &mut serializer)
}
