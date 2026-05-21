use crate::error::{AppError, AppResult};

const KEY_MAP: &[usize] = &[
    46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35, 27, 43, 5, 49, 33, 9, 42, 19, 29,
    28, 14, 39, 12, 38, 41, 13, 37, 48, 7, 16, 24, 55, 40, 61, 26, 17, 0, 1, 60, 51, 30, 4, 22, 25,
    54, 21, 56, 59, 6, 63, 57, 62, 11, 36, 20, 34, 44, 52,
];

pub fn extract_key(url: &str) -> Option<String> {
    let last_slash = url.rfind('/')?;
    let dot = url[last_slash..].find('.')?;
    Some(url[last_slash + 1..last_slash + dot].to_string())
}

pub fn mix_wbi_keys(img_key: &str, sub_key: &str) -> AppResult<String> {
    let full = format!("{}{}", img_key, sub_key);
    if full.chars().count() < 64 {
        return Err(AppError::Bilibili(format!(
            "invalid WBI key length: img_key={} chars, sub_key={} chars",
            img_key.chars().count(),
            sub_key.chars().count()
        )));
    }

    let mut mixed = String::with_capacity(32);
    let chars: Vec<char> = full.chars().collect();
    for &idx in KEY_MAP.iter().take(32) {
        mixed.push(chars[idx]);
    }
    Ok(mixed)
}

pub fn percent_encode(s: &str) -> String {
    let mut encoded = String::new();
    for b in s.bytes() {
        match b {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(b as char);
            }
            _ => {
                encoded.push_str(&format!("%{:02X}", b));
            }
        }
    }
    encoded
}

pub fn sign_wbi_query(
    query: &std::collections::HashMap<String, String>,
    mixed_key: &str,
    timestamp: i64,
) -> std::collections::HashMap<String, String> {
    let mut sanitized = std::collections::HashMap::new();
    for (k, v) in query {
        let clean_v: String = v
            .chars()
            .filter(|&c| c != '!' && c != '\'' && c != '(' && c != ')' && c != '*')
            .collect();
        sanitized.insert(k.clone(), clean_v);
    }
    sanitized.insert("wts".to_string(), timestamp.to_string());

    let mut keys: Vec<&String> = sanitized.keys().collect();
    keys.sort();

    let mut encoded_parts = Vec::new();
    for k in keys {
        let v = &sanitized[k];
        encoded_parts.push(format!("{}={}", percent_encode(k), percent_encode(v)));
    }
    let query_string = encoded_parts.join("&");
    let sign_target = format!("{}{}", query_string, mixed_key);

    let hash = md5::compute(sign_target.as_bytes());
    let w_rid = format!("{:x}", hash);

    let mut result = query.clone();
    result.insert("wts".to_string(), timestamp.to_string());
    result.insert("w_rid".to_string(), w_rid);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_extract_key() {
        let img_url = "https://i0.hdslb.com/bfs/wbi/7250cfc818b84d69a693f7333a281d30.png";
        assert_eq!(
            extract_key(img_url).unwrap(),
            "7250cfc818b84d69a693f7333a281d30"
        );
    }

    #[test]
    fn test_mix_wbi_keys() {
        // 1. Test Bilibili default/fallback keys
        let img_key_default = "7250cfc818b84d69a693f7333a281d30";
        let sub_key_default = "c6b4c407cc5d4b52a792f3957245a4a5";
        assert_eq!(
            mix_wbi_keys(img_key_default, sub_key_default).unwrap(),
            "5295313c99b040b48df76853d16740cd"
        );

        // 2. Test official documentation example keys
        let img_key_doc = "1125313c99b040b48df76853d16740cd";
        let sub_key_doc = "2235313c99b040b48df76853d16740cd";
        assert_eq!(
            mix_wbi_keys(img_key_doc, sub_key_doc).unwrap(),
            "b4f289324fbd6505701d29b704bc4390"
        );
    }

    #[test]
    fn test_mix_wbi_keys_rejects_short_keys() {
        let err = mix_wbi_keys("short", "key").unwrap_err();
        assert!(err.to_string().contains("invalid WBI key length"));
    }

    #[test]
    fn test_percent_encode() {
        assert_eq!(percent_encode("foo bar"), "foo%20bar");
        assert_eq!(percent_encode("a-z_~."), "a-z_~.");
        assert_eq!(percent_encode("!@#"), "%21%40%23");
    }

    #[test]
    fn test_sign_wbi_query() {
        let mut query = HashMap::new();
        query.insert("foo".to_string(), "bar".to_string());
        query.insert("special".to_string(), "a!b'c(d)e*f".to_string());

        let mixed_key = "ea1db124c0beaec8d8d73b06385d38a0";
        let timestamp = 114514;

        let signed = sign_wbi_query(&query, mixed_key, timestamp);
        assert_eq!(signed.get("wts").unwrap(), "114514");

        // The query parameters after sanitization:
        // foo = bar
        // special = abcdef
        // wts = 114514
        // Sorted: foo=bar&special=abcdef&wts=114514
        // Target: foo=bar&special=abcdef&wts=114514ea1db124c0beaec8d8d73b06385d38a0
        let target = "foo=bar&special=abcdef&wts=114514ea1db124c0beaec8d8d73b06385d38a0";
        let expected_hash = format!("{:x}", md5::compute(target.as_bytes()));

        assert_eq!(signed.get("w_rid").unwrap(), &expected_hash);
        assert_eq!(signed.get("foo").unwrap(), "bar");
        // Check that the original params in signed map are NOT sanitized, only the sign target was sanitized
        assert_eq!(signed.get("special").unwrap(), "a!b'c(d)e*f");
    }
}
