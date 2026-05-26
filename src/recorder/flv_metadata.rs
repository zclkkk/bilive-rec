const KEYFRAME_INDEX_CAPACITY: usize = 4096;
const MAX_SOURCE_METADATA_BYTES: usize = 1024 * 1024;
const MAX_AMF_DEPTH: usize = 16;
const MAX_AMF_OBJECT_ENTRIES: usize = 1024;
const MAX_AMF_ARRAY_ITEMS: usize = KEYFRAME_INDEX_CAPACITY * 2;

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct KeyframeIndex {
    pub time_ms: u32,
    pub file_position: u64,
}

pub(super) fn build_metadata_body(
    source: &[u8],
    duration_ms: u32,
    keyframes: &[KeyframeIndex],
) -> Vec<u8> {
    let mut properties = parse_metadata_properties_with_fallback(source);
    properties.retain(|(key, _)| key != "duration" && key != "keyframes");

    let mut out = Vec::new();
    write_amf_string_value(&mut out, "onMetaData");
    out.push(0x08);
    write_u32(&mut out, (properties.len() + 2) as u32);

    for (key, value) in properties {
        write_key(&mut out, &key);
        write_value(&mut out, &value);
    }

    write_key(&mut out, "duration");
    write_number_value(&mut out, duration_ms as f64 / 1000.0);

    write_key(&mut out, "keyframes");
    write_keyframes_value(&mut out, keyframes);

    write_object_end(&mut out);
    out
}

fn parse_metadata_properties_with_fallback(source: &[u8]) -> Vec<(String, AmfValue)> {
    if source.len() > MAX_SOURCE_METADATA_BYTES {
        tracing::warn!(
            metadata_bytes = source.len(),
            max_metadata_bytes = MAX_SOURCE_METADATA_BYTES,
            "FLV metadata is too large to parse; generating recorder-owned metadata"
        );
        return Vec::new();
    }

    match parse_metadata_properties(source) {
        Some(properties) => properties,
        None => {
            tracing::warn!(
                metadata_bytes = source.len(),
                "could not parse FLV metadata; generating recorder-owned metadata"
            );
            Vec::new()
        }
    }
}

fn parse_metadata_properties(data: &[u8]) -> Option<Vec<(String, AmfValue)>> {
    let mut reader = AmfReader::new(data);
    match reader.read_value()? {
        AmfValue::String(name) if name == "onMetaData" => {}
        _ => return None,
    }

    match reader.read_value()? {
        AmfValue::Object(properties) | AmfValue::EcmaArray(properties) => Some(properties),
        _ => None,
    }
}

#[cfg(test)]
pub(super) fn debug_number_property(data: &[u8], name: &str) -> Option<f64> {
    parse_metadata_properties(data)?
        .into_iter()
        .find_map(|(key, value)| match (key == name, value) {
            (true, AmfValue::Number(value)) => Some(value),
            _ => None,
        })
}

#[cfg(test)]
pub(super) fn debug_keyframe_times(data: &[u8]) -> Option<Vec<f64>> {
    let properties = parse_metadata_properties(data)?;
    let (_, keyframes) = properties.into_iter().find(|(key, _)| key == "keyframes")?;
    let AmfValue::Object(entries) = keyframes else {
        return None;
    };
    let (_, times) = entries.into_iter().find(|(key, _)| key == "times")?;
    let AmfValue::StrictArray(values) = times else {
        return None;
    };
    values
        .into_iter()
        .map(|value| match value {
            AmfValue::Number(value) => Some(value),
            _ => None,
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq)]
enum AmfValue {
    Number(f64),
    Boolean(bool),
    String(String),
    Object(Vec<(String, AmfValue)>),
    Null,
    Undefined,
    EcmaArray(Vec<(String, AmfValue)>),
    StrictArray(Vec<AmfValue>),
}

struct AmfReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> AmfReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn read_value(&mut self) -> Option<AmfValue> {
        self.read_value_at_depth(0)
    }

    fn read_value_at_depth(&mut self, depth: usize) -> Option<AmfValue> {
        if depth > MAX_AMF_DEPTH {
            return None;
        }

        let marker = self.read_u8()?;
        match marker {
            0x00 => Some(AmfValue::Number(self.read_f64()?)),
            0x01 => Some(AmfValue::Boolean(self.read_u8()? != 0)),
            0x02 => Some(AmfValue::String(self.read_utf8_u16()?)),
            0x03 => Some(AmfValue::Object(self.read_object_entries(None, depth + 1)?)),
            0x05 => Some(AmfValue::Null),
            0x06 => Some(AmfValue::Undefined),
            0x08 => {
                let count = self.read_u32()? as usize;
                if count > MAX_AMF_OBJECT_ENTRIES {
                    return None;
                }
                Some(AmfValue::EcmaArray(
                    self.read_object_entries(Some(count), depth + 1)?,
                ))
            }
            0x0A => {
                let count = self.read_u32()? as usize;
                if count > MAX_AMF_ARRAY_ITEMS {
                    return None;
                }
                let mut values = Vec::with_capacity(count);
                for _ in 0..count {
                    values.push(self.read_value_at_depth(depth + 1)?);
                }
                Some(AmfValue::StrictArray(values))
            }
            _ => None,
        }
    }

    fn read_object_entries(
        &mut self,
        count: Option<usize>,
        depth: usize,
    ) -> Option<Vec<(String, AmfValue)>> {
        let mut entries = Vec::new();

        if let Some(count) = count {
            for _ in 0..count {
                if self.consume_object_end() {
                    return Some(entries);
                }
                let key = self.read_utf8_key()?;
                let value = self.read_value_at_depth(depth)?;
                entries.push((key, value));
            }
            let _ = self.consume_object_end();
            return Some(entries);
        }

        loop {
            if self.consume_object_end() {
                return Some(entries);
            }
            if entries.len() >= MAX_AMF_OBJECT_ENTRIES {
                return None;
            }
            let key = self.read_utf8_key()?;
            let value = self.read_value_at_depth(depth)?;
            entries.push((key, value));
        }
    }

    fn consume_object_end(&mut self) -> bool {
        if self.data.get(self.pos..self.pos + 3) == Some(&[0, 0, 9]) {
            self.pos += 3;
            return true;
        }
        false
    }

    fn read_utf8_key(&mut self) -> Option<String> {
        let len = self.read_u16()? as usize;
        self.read_utf8(len)
    }

    fn read_utf8_u16(&mut self) -> Option<String> {
        let len = self.read_u16()? as usize;
        self.read_utf8(len)
    }

    fn read_utf8(&mut self, len: usize) -> Option<String> {
        let bytes = self.take(len)?;
        String::from_utf8(bytes.to_vec()).ok()
    }

    fn read_f64(&mut self) -> Option<f64> {
        let bytes: [u8; 8] = self.take(8)?.try_into().ok()?;
        Some(f64::from_bits(u64::from_be_bytes(bytes)))
    }

    fn read_u32(&mut self) -> Option<u32> {
        let bytes: [u8; 4] = self.take(4)?.try_into().ok()?;
        Some(u32::from_be_bytes(bytes))
    }

    fn read_u16(&mut self) -> Option<u16> {
        let bytes: [u8; 2] = self.take(2)?.try_into().ok()?;
        Some(u16::from_be_bytes(bytes))
    }

    fn read_u8(&mut self) -> Option<u8> {
        let value = *self.data.get(self.pos)?;
        self.pos += 1;
        Some(value)
    }

    fn take(&mut self, len: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(len)?;
        let bytes = self.data.get(self.pos..end)?;
        self.pos = end;
        Some(bytes)
    }
}

fn write_value(out: &mut Vec<u8>, value: &AmfValue) {
    match value {
        AmfValue::Number(value) => write_number_value(out, *value),
        AmfValue::Boolean(value) => {
            out.push(0x01);
            out.push(u8::from(*value));
        }
        AmfValue::String(value) => write_amf_string_value(out, value),
        AmfValue::Object(entries) => {
            out.push(0x03);
            write_entries(out, entries);
            write_object_end(out);
        }
        AmfValue::Null => out.push(0x05),
        AmfValue::Undefined => out.push(0x06),
        AmfValue::EcmaArray(entries) => {
            out.push(0x08);
            write_u32(out, entries.len() as u32);
            write_entries(out, entries);
            write_object_end(out);
        }
        AmfValue::StrictArray(values) => {
            out.push(0x0A);
            write_u32(out, values.len() as u32);
            for value in values {
                write_value(out, value);
            }
        }
    }
}

fn write_entries(out: &mut Vec<u8>, entries: &[(String, AmfValue)]) {
    for (key, value) in entries {
        write_key(out, key);
        write_value(out, value);
    }
}

fn write_keyframes_value(out: &mut Vec<u8>, keyframes: &[KeyframeIndex]) {
    let keyframes = &keyframes[..keyframes.len().min(KEYFRAME_INDEX_CAPACITY)];

    out.push(0x03);
    write_key(out, "times");
    write_strict_number_array(
        out,
        keyframes.iter().map(|item| item.time_ms as f64 / 1000.0),
    );

    write_key(out, "filepositions");
    write_strict_number_array(out, keyframes.iter().map(|item| item.file_position as f64));

    write_key(out, "spacer");
    write_strict_number_array(
        out,
        std::iter::repeat_n(f64::NAN, 2 * (KEYFRAME_INDEX_CAPACITY - keyframes.len())),
    );

    write_object_end(out);
}

fn write_strict_number_array<I>(out: &mut Vec<u8>, values: I)
where
    I: IntoIterator<Item = f64>,
{
    let values: Vec<_> = values.into_iter().collect();
    out.push(0x0A);
    write_u32(out, values.len() as u32);
    for value in values {
        write_number_value(out, value);
    }
}

fn write_amf_string_value(out: &mut Vec<u8>, value: &str) {
    out.push(0x02);
    write_key(out, value);
}

fn write_number_value(out: &mut Vec<u8>, value: f64) {
    out.push(0x00);
    out.extend_from_slice(&value.to_bits().to_be_bytes());
}

fn write_key(out: &mut Vec<u8>, key: &str) {
    let bytes = key.as_bytes();
    assert!(bytes.len() <= u16::MAX as usize, "AMF key is too long");
    out.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    out.extend_from_slice(bytes);
}

fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_object_end(out: &mut Vec<u8>) {
    out.extend_from_slice(&[0, 0, 9]);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_metadata() -> Vec<u8> {
        let mut out = Vec::new();
        write_amf_string_value(&mut out, "onMetaData");
        out.push(0x08);
        write_u32(&mut out, 2);
        write_key(&mut out, "width");
        write_number_value(&mut out, 1920.0);
        write_key(&mut out, "height");
        write_number_value(&mut out, 1080.0);
        write_object_end(&mut out);
        out
    }

    #[test]
    fn metadata_body_len_is_stable_when_duration_and_keyframes_change() {
        let source = sample_metadata();
        let initial = build_metadata_body(&source, 0, &[]);
        let final_body = build_metadata_body(
            &source,
            12_345,
            &[
                KeyframeIndex {
                    time_ms: 0,
                    file_position: 128,
                },
                KeyframeIndex {
                    time_ms: 2_000,
                    file_position: 4096,
                },
            ],
        );

        assert_eq!(initial.len(), final_body.len());
    }

    #[test]
    fn metadata_preserves_existing_properties() {
        let source = sample_metadata();
        let body = build_metadata_body(&source, 1_000, &[]);
        let properties = parse_metadata_properties(&body).unwrap();

        assert!(properties.iter().any(|(key, _)| key == "width"));
        assert!(properties.iter().any(|(key, _)| key == "height"));
        assert!(properties.iter().any(|(key, _)| key == "duration"));
        assert!(properties.iter().any(|(key, _)| key == "keyframes"));
    }

    #[test]
    fn invalid_source_uses_generated_metadata() {
        let body = build_metadata_body(&[0, 1, 2, 3], 1_000, &[]);
        let properties = parse_metadata_properties(&body).unwrap();

        assert_eq!(properties.len(), 2);
        assert!(properties.iter().any(|(key, _)| key == "duration"));
        assert!(properties.iter().any(|(key, _)| key == "keyframes"));
    }

    #[test]
    fn oversized_strict_array_falls_back_to_generated_metadata() {
        let mut source = Vec::new();
        write_amf_string_value(&mut source, "onMetaData");
        source.push(0x0A);
        write_u32(&mut source, (MAX_AMF_ARRAY_ITEMS + 1) as u32);

        let body = build_metadata_body(&source, 1_000, &[]);
        let properties = parse_metadata_properties(&body).unwrap();

        assert_eq!(properties.len(), 2);
        assert!(properties.iter().any(|(key, _)| key == "duration"));
        assert!(properties.iter().any(|(key, _)| key == "keyframes"));
    }

    #[test]
    fn oversized_ecma_array_falls_back_to_generated_metadata() {
        let mut source = Vec::new();
        write_amf_string_value(&mut source, "onMetaData");
        source.push(0x08);
        write_u32(&mut source, (MAX_AMF_OBJECT_ENTRIES + 1) as u32);

        let body = build_metadata_body(&source, 1_000, &[]);
        let properties = parse_metadata_properties(&body).unwrap();

        assert_eq!(properties.len(), 2);
        assert!(properties.iter().any(|(key, _)| key == "duration"));
        assert!(properties.iter().any(|(key, _)| key == "keyframes"));
    }
}
