use std::io;

fn main() -> Result<(), serde_bigquery::Error> {
    transcode(io::stdin(), io::stdout())
}

fn transcode<R: io::Read, W: io::Write>(reader: R, writer: W) -> Result<(), serde_bigquery::Error> {
    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    let mut serializer = serde_bigquery::Serializer::new(writer);
    serde_transcode::transcode(&mut deserializer, &mut serializer)?;
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    fn to_bigquery(json: &str) -> String {
        let mut buf = Vec::new();
        transcode(json.as_bytes(), io::Cursor::new(&mut buf)).unwrap();
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn test_simple_vals() {
        assert_eq!(to_bigquery("false"), "FALSE");
        assert_eq!(to_bigquery("true"), "TRUE");
        assert_eq!(to_bigquery("42"), "42");
        assert_eq!(to_bigquery("1.25"), "1.25");
    }

    #[test]
    fn test_objects() {
        assert_eq!(to_bigquery("{\"a\": false}"), "STRUCT(FALSE AS `a`)");
        assert_eq!(
            to_bigquery("[{\"a\": false, \"b\": 1}, {\"a\": true, \"b\": 2}]"),
            "[STRUCT(FALSE AS `a`,1 AS `b`),STRUCT(TRUE AS `a`,2 AS `b`)]"
        );
    }

    #[test]
    fn test_fields_out_of_order() {
        assert_eq!(
            to_bigquery("[{\"a\": false, \"b\": 1}, {\"b\": 2, \"a\": true}]"),
            "[STRUCT(FALSE AS `a`,1 AS `b`),STRUCT(TRUE AS `a`,2 AS `b`)]"
        );
    }

    #[test]
    fn test_missing_fields() {
        assert_eq!(
            to_bigquery("[{\"a\": false, \"b\": 1}, {\"a\": true}]"),
            "[STRUCT(FALSE AS `a`,1 AS `b`),STRUCT(TRUE AS `a`,NULL AS `b`)]"
        );
    }
}
