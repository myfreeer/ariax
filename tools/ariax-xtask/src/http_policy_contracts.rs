use crate::inventory::{GenerationMode, apply_outputs, json_string};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;
use std::net::IpAddr;
use std::path::{Path, PathBuf};

const PIN_PATH: &str = "compat/iana-special-purpose.pin";
const IPV4_PATH: &str = "compat/iana-ipv4-special-registry.csv";
const IPV6_PATH: &str = "compat/iana-ipv6-special-registry.csv";
const RUST_OUTPUT: &str = "crates/ariax-engine/src/http_special_purpose_generated.rs";
const JSON_OUTPUT: &str = "generated/http_destination_policy.json";
const TRANSPORT_JSON_OUTPUT: &str = "generated/http_transport_policy.json";
const MAX_DESTINATION_ADDRESSES: usize = 32;
const MAX_DESTINATION_HOST_BYTES: usize = 253;
const DEFAULT_RESOLUTION_TIMEOUT_SECONDS: u64 = 5;

#[derive(Clone, Debug, Eq, PartialEq)]
struct RegistryPin {
    schema: u32,
    ipv4_url: String,
    ipv4_last_modified: String,
    ipv4_sha256: String,
    ipv6_url: String,
    ipv6_last_modified: String,
    ipv6_sha256: String,
    line_endings: String,
    license: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum Prefix {
    V4 { network: u32, length: u8 },
    V6 { network: u128, length: u8 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Entry {
    prefix: Prefix,
    class: Class,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Class {
    GlobalUnicast,
    Loopback,
    Private,
    UniqueLocal,
    CarrierGradeNat,
    LinkLocal,
    Metadata,
    Documentation,
    Benchmark,
    Unspecified,
    Multicast,
    Reserved,
}

impl Class {
    const fn rust_name(self) -> &'static str {
        match self {
            Self::GlobalUnicast => "GlobalUnicast",
            Self::Loopback => "Loopback",
            Self::Private => "Private",
            Self::UniqueLocal => "UniqueLocal",
            Self::CarrierGradeNat => "CarrierGradeNat",
            Self::LinkLocal => "LinkLocal",
            Self::Metadata => "Metadata",
            Self::Documentation => "Documentation",
            Self::Benchmark => "Benchmark",
            Self::Unspecified => "Unspecified",
            Self::Multicast => "Multicast",
            Self::Reserved => "Reserved",
        }
    }
}

pub(crate) fn generate_http_policy_contracts(
    workspace_root: &Path,
    mode: GenerationMode,
) -> Result<String, String> {
    let pin = parse_pin(
        &fs::read_to_string(workspace_root.join(PIN_PATH))
            .map_err(|error| format!("failed to read {PIN_PATH}: {error}"))?,
    )?;
    let ipv4 = read_pinned_registry(workspace_root, IPV4_PATH, &pin.ipv4_sha256)?;
    let ipv6 = read_pinned_registry(workspace_root, IPV6_PATH, &pin.ipv6_sha256)?;
    let mut entries = parse_registry(&ipv4, false)?;
    entries.extend(parse_registry(&ipv6, true)?);
    entries.extend(security_overrides());
    let entries = normalize_entries(entries)?;
    let outputs = [
        (PathBuf::from(RUST_OUTPUT), render_rust(&entries)),
        (
            PathBuf::from(JSON_OUTPUT),
            render_json(&pin, &entries, ipv4.len(), ipv6.len()),
        ),
        (
            PathBuf::from(TRANSPORT_JSON_OUTPUT),
            render_transport_json(),
        ),
    ];
    apply_outputs(workspace_root, &outputs, mode)?;
    Ok(format!(
        "{} pinned IANA HTTP destination policy and direct HTTP(S) transport contract: {} IPv4/IPv6 prefixes",
        match mode {
            GenerationMode::Write => "generated",
            GenerationMode::Check => "verified",
        },
        entries.len()
    ))
}

fn render_transport_json() -> String {
    concat!(
        "{\n",
        "  \"schema\": 1,\n",
        "  \"status\": \"implemented\",\n",
        "  \"backend\": {\"http\": \"hyper-1-http1\", \"tls\": \"rustls-0.23\", \"connector\": \"hyper-rustls-0.27\", \"crypto_provider\": \"ring\", \"native_roots\": \"rustls-native-certs\"},\n",
        "  \"schemes\": {\"http\": {\"default_port\": 80}, \"https\": {\"default_port\": 443}},\n",
        "  \"tls\": {\"versions\": [\"TLS1.2\", \"TLS1.3\"], \"default_minimum\": \"TLS1.2\", \"trust_sources\": [\"system\", \"custom_pem\", \"system_and_custom_pem\"], \"certificate_verification\": \"required\", \"alpn\": [], \"http1_without_alpn\": true, \"max_custom_pem_bytes\": 4194304, \"max_custom_certificates\": 4096},\n",
        "  \"pool\": {\"scope\": \"origin\", \"keep_alive_default\": true, \"default_max_connections_per_origin\": 1, \"default_max_idle_connections_per_origin\": 1, \"max_connections_per_origin\": 8, \"max_idle_connections_per_origin\": 8, \"default_idle_timeout_seconds\": 30, \"connection_reservation_bytes\": 262144, \"fresh_destination_admission_per_physical_connection\": true, \"incomplete_or_failed_response_reusable\": false},\n",
        "  \"diagnostics\": [\"connections_opened\", \"connections_reused\", \"connections_expired\", \"connections_poisoned\", \"pool_exhausted\", \"tls_handshakes\", \"tls_failures\"],\n",
        "  \"errors\": [{\"code\": \"http_transport_invalid_policy\", \"retriable\": false}, {\"code\": \"http_transport_invalid_origin\", \"retriable\": false}, {\"code\": \"http_transport_origin_mismatch\", \"retriable\": false}, {\"code\": \"http_transport_pool_exhausted\", \"retriable\": true}, {\"code\": \"connect_timeout\", \"retriable\": true}, {\"code\": \"connect\", \"retriable\": true}, {\"code\": \"tls_configuration\", \"retriable\": false}, {\"code\": \"tls_server_name\", \"retriable\": false}, {\"code\": \"tls_handshake_timeout\", \"retriable\": true}, {\"code\": \"tls_handshake\", \"retriable\": false}, {\"code\": \"handshake_timeout\", \"retriable\": true}, {\"code\": \"http_protocol\", \"retriable\": true}, {\"code\": \"request_build\", \"retriable\": false}],\n",
        "  \"deferred\": [\"redirects\", \"proxies\", \"dns_cache\", \"dns_singleflight\", \"happy_eyeballs\", \"http2\", \"client_certificates\", \"insecure_verification\", \"native_tls\", \"ordinary_cli_rpc_uri_exposure\"]\n",
        "}\n",
    )
    .to_owned()
}

fn parse_pin(input: &str) -> Result<RegistryPin, String> {
    let mut values = BTreeMap::new();
    for (line_number, line) in input.lines().enumerate() {
        if line.is_empty() {
            continue;
        }
        let (key, value) = line
            .split_once('=')
            .ok_or_else(|| format!("{PIN_PATH} line {} is not key=value", line_number + 1))?;
        if key.is_empty() || value.is_empty() || values.insert(key, value).is_some() {
            return Err(format!("{PIN_PATH} line {} is malformed", line_number + 1));
        }
    }
    let take = |key: &str| {
        values
            .get(key)
            .copied()
            .ok_or_else(|| format!("{PIN_PATH} is missing {key}"))
    };
    let schema = take("schema")?
        .parse::<u32>()
        .map_err(|_| format!("{PIN_PATH} has an invalid schema"))?;
    if schema != 1 {
        return Err(format!("{PIN_PATH} has unsupported schema {schema}"));
    }
    let expected = [
        "schema",
        "ipv4_url",
        "ipv4_last_modified",
        "ipv4_sha256",
        "ipv6_url",
        "ipv6_last_modified",
        "ipv6_sha256",
        "line_endings",
        "license",
    ];
    if values.len() != expected.len() || values.keys().any(|key| !expected.contains(key)) {
        return Err(format!("{PIN_PATH} contains unknown keys"));
    }
    let ipv4_sha256 = take("ipv4_sha256")?.to_owned();
    let ipv6_sha256 = take("ipv6_sha256")?.to_owned();
    for (name, hash) in [("ipv4_sha256", &ipv4_sha256), ("ipv6_sha256", &ipv6_sha256)] {
        if hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(format!("{PIN_PATH} has an invalid {name}"));
        }
    }
    if take("line_endings")? != "lf" {
        return Err(format!("{PIN_PATH} must pin LF line endings"));
    }
    Ok(RegistryPin {
        schema,
        ipv4_url: take("ipv4_url")?.to_owned(),
        ipv4_last_modified: take("ipv4_last_modified")?.to_owned(),
        ipv4_sha256,
        ipv6_url: take("ipv6_url")?.to_owned(),
        ipv6_last_modified: take("ipv6_last_modified")?.to_owned(),
        ipv6_sha256,
        line_endings: take("line_endings")?.to_owned(),
        license: take("license")?.to_owned(),
    })
}

fn read_pinned_registry(
    workspace_root: &Path,
    relative: &str,
    expected_hash: &str,
) -> Result<String, String> {
    let bytes = fs::read(workspace_root.join(relative))
        .map_err(|error| format!("failed to read {relative}: {error}"))?;
    if bytes.contains(&b'\r') {
        return Err(format!("{relative} contains non-LF line endings"));
    }
    let actual = hex(&Sha256::digest(&bytes));
    if actual != expected_hash {
        return Err(format!(
            "{relative} hash {actual} does not match pinned hash"
        ));
    }
    String::from_utf8(bytes).map_err(|error| format!("{relative} is not UTF-8: {error}"))
}

fn parse_registry(input: &str, ipv6: bool) -> Result<Vec<Entry>, String> {
    let records = parse_csv(input)?;
    let header = records
        .first()
        .ok_or_else(|| "IANA registry is empty".to_owned())?;
    if header.len() != 10 || header[0] != "Address Block" || header[8] != "Globally Reachable" {
        return Err("unexpected IANA special-purpose registry header".to_owned());
    }
    let mut entries = Vec::new();
    for (index, fields) in records.iter().enumerate().skip(1) {
        if fields.len() != 10 {
            return Err(format!(
                "IANA registry record {} has {} fields",
                index + 1,
                fields.len()
            ));
        }
        let class = class_for_name(&fields[1], fields[8] == "True");
        for block in fields[0].split(',') {
            let block = block.trim();
            if block.is_empty() {
                continue;
            }
            let prefix = parse_prefix(block, ipv6)
                .map_err(|error| format!("IANA registry record {}: {error}", index + 1))?;
            entries.push(Entry { prefix, class });
        }
    }
    Ok(entries)
}

fn class_for_name(name: &str, globally_reachable: bool) -> Class {
    let name = name.to_ascii_lowercase();
    if name.contains("private-use") {
        Class::Private
    } else if name.contains("unique-local") {
        Class::UniqueLocal
    } else if name.contains("loopback") {
        Class::Loopback
    } else if name.contains("shared address") {
        Class::CarrierGradeNat
    } else if name.contains("link local") || name.contains("link-local") {
        Class::LinkLocal
    } else if name.contains("documentation") {
        Class::Documentation
    } else if name.contains("benchmark") {
        Class::Benchmark
    } else if name.contains("unspecified")
        || name.contains("this network")
        || name.contains("this host")
    {
        Class::Unspecified
    } else if globally_reachable {
        Class::GlobalUnicast
    } else {
        Class::Reserved
    }
}

fn parse_prefix(input: &str, ipv6: bool) -> Result<Prefix, String> {
    let input = input
        .split_once(" [")
        .map_or(input, |(prefix, _)| prefix)
        .trim();
    let (address, length) = input
        .split_once('/')
        .ok_or_else(|| format!("invalid prefix {input:?}"))?;
    let length = length
        .parse::<u8>()
        .map_err(|_| format!("invalid prefix length in {input:?}"))?;
    let address = address
        .parse::<IpAddr>()
        .map_err(|_| format!("invalid address in {input:?}"))?;
    match (address, ipv6) {
        (IpAddr::V4(address), false) if length <= 32 => Ok(Prefix::V4 {
            network: u32::from(address) & mask32(length),
            length,
        }),
        (IpAddr::V6(address), true) if length <= 128 => Ok(Prefix::V6 {
            network: u128::from(address) & mask128(length),
            length,
        }),
        _ => Err(format!("address-family mismatch in {input:?}")),
    }
}

fn security_overrides() -> [Entry; 4] {
    [
        Entry {
            prefix: Prefix::V4 {
                network: u32::from_be_bytes([169, 254, 169, 254]),
                length: 32,
            },
            class: Class::Metadata,
        },
        Entry {
            prefix: Prefix::V4 {
                network: u32::from_be_bytes([224, 0, 0, 0]),
                length: 4,
            },
            class: Class::Multicast,
        },
        Entry {
            prefix: Prefix::V6 {
                network: 0xff_u128 << 120,
                length: 8,
            },
            class: Class::Multicast,
        },
        Entry {
            prefix: Prefix::V6 {
                network: 0,
                length: 96,
            },
            class: Class::Reserved,
        },
    ]
}

fn normalize_entries(entries: Vec<Entry>) -> Result<Vec<Entry>, String> {
    let mut by_prefix = BTreeMap::new();
    for entry in entries {
        if let Some(previous) = by_prefix.insert(entry.prefix, entry.class)
            && previous != entry.class
        {
            return Err(format!(
                "conflicting classes for prefix {:?}: {:?} and {:?}",
                entry.prefix, previous, entry.class
            ));
        }
    }
    let mut entries: Vec<_> = by_prefix
        .into_iter()
        .map(|(prefix, class)| Entry { prefix, class })
        .collect();
    entries.sort_by_key(|entry| match entry.prefix {
        Prefix::V4 { network, length } => (0_u8, u8::MAX - length, u128::from(network)),
        Prefix::V6 { network, length } => (1_u8, u8::MAX - length, network),
    });
    Ok(entries)
}

fn render_rust(entries: &[Entry]) -> String {
    let mut output = String::new();
    output.push_str("// @generated by cargo xtask; do not edit.\n");
    writeln!(
        output,
        "const GENERATED_HTTP_MAX_DESTINATION_ADDRESSES: usize = {MAX_DESTINATION_ADDRESSES};"
    )
    .expect("write generated source");
    writeln!(
        output,
        "const GENERATED_HTTP_MAX_DESTINATION_HOST_BYTES: usize = {MAX_DESTINATION_HOST_BYTES};"
    )
    .expect("write generated source");
    writeln!(
        output,
        "const GENERATED_HTTP_DEFAULT_RESOLUTION_TIMEOUT_SECONDS: u64 = {DEFAULT_RESOLUTION_TIMEOUT_SECONDS};\n"
    )
    .expect("write generated source");
    output.push_str("const GENERATED_HTTP_IPV4_PREFIXES: &[(u32, u8, HttpAddressClass)] = &[\n");
    for entry in entries
        .iter()
        .filter(|entry| matches!(entry.prefix, Prefix::V4 { .. }))
    {
        if let Prefix::V4 { network, length } = entry.prefix {
            writeln!(
                output,
                "    (0x{network:08x}, {length}, HttpAddressClass::{}),",
                entry.class.rust_name()
            )
            .expect("write generated source");
        }
    }
    output.push_str(
        "];\n\nconst GENERATED_HTTP_IPV6_PREFIXES: &[(u128, u8, HttpAddressClass)] = &[\n",
    );
    for entry in entries
        .iter()
        .filter(|entry| matches!(entry.prefix, Prefix::V6 { .. }))
    {
        if let Prefix::V6 { network, length } = entry.prefix {
            writeln!(
                output,
                "    (0x{network:032x}, {length}, HttpAddressClass::{}),",
                entry.class.rust_name()
            )
            .expect("write generated source");
        }
    }
    output.push_str("];\n");
    output
}

fn render_json(
    pin: &RegistryPin,
    entries: &[Entry],
    ipv4_bytes: usize,
    ipv6_bytes: usize,
) -> String {
    let mut output = String::new();
    write!(
        output,
        "{{\n  \"schema\": {},\n  \"source\": {{",
        pin.schema
    )
    .expect("write generated JSON");
    write!(
        output,
        "\"ipv4_url\": {}, \"ipv4_last_modified\": {}, \"ipv4_sha256\": {}, \"ipv6_url\": {}, \"ipv6_last_modified\": {}, \"ipv6_sha256\": {}, \"license\": {}",
        json_string(&pin.ipv4_url),
        json_string(&pin.ipv4_last_modified),
        json_string(&pin.ipv4_sha256),
        json_string(&pin.ipv6_url),
        json_string(&pin.ipv6_last_modified),
        json_string(&pin.ipv6_sha256),
        json_string(&pin.license)
    )
    .expect("write generated JSON");
    write!(
        output,
        "}},\n  \"inputs\": {{\"ipv4_bytes\": {ipv4_bytes}, \"ipv6_bytes\": {ipv6_bytes}, \"line_endings\": {} }},\n  \"caps\": {{\"max_addresses\": {MAX_DESTINATION_ADDRESSES}, \"max_host_bytes\": {MAX_DESTINATION_HOST_BYTES}, \"default_resolution_timeout_seconds\": {DEFAULT_RESOLUTION_TIMEOUT_SECONDS}, \"default_allows\": [\"global_unicast\"], \"allow_private_classes\": [\"private\", \"unique_local\"], \"always_denied_classes\": [\"carrier_grade_nat\", \"link_local\", \"metadata\", \"documentation\", \"benchmark\", \"unspecified\", \"multicast\", \"reserved\"]}},\n  \"prefix_count\": {},\n  \"status\": \"implemented\"\n}}\n",
        json_string(&pin.line_endings),
        entries.len()
    )
    .expect("write generated JSON");
    output
}

fn parse_csv(input: &str) -> Result<Vec<Vec<String>>, String> {
    if !input.is_ascii() {
        return Err("IANA CSV snapshot contains unsupported non-ASCII text".to_owned());
    }
    let mut records = Vec::new();
    let mut fields = Vec::new();
    let mut field = String::new();
    let mut quoted = false;
    let mut index = 0;
    let bytes = input.as_bytes();
    while index < bytes.len() {
        let byte = bytes[index];
        if quoted {
            if byte == b'"' {
                if bytes.get(index + 1) == Some(&b'"') {
                    field.push('"');
                    index += 2;
                    continue;
                }
                quoted = false;
            } else {
                field.push(byte as char);
            }
            index += 1;
            continue;
        }
        match byte {
            b'"' if field.is_empty() => {
                quoted = true;
                index += 1;
            }
            b',' => {
                fields.push(std::mem::take(&mut field));
                index += 1;
            }
            b'\n' => {
                fields.push(std::mem::take(&mut field));
                records.push(std::mem::take(&mut fields));
                index += 1;
            }
            b'\r' => {
                index += 1;
            }
            _ => {
                field.push(byte as char);
                index += 1;
            }
        }
    }
    if quoted {
        return Err("unterminated quoted CSV field".to_owned());
    }
    if !field.is_empty() || !fields.is_empty() {
        fields.push(field);
        records.push(fields);
    }
    Ok(records)
}

const fn mask32(length: u8) -> u32 {
    if length == 0 {
        0
    } else {
        u32::MAX << (32 - length)
    }
}

const fn mask128(length: u8) -> u128 {
    if length == 0 {
        0
    } else {
        u128::MAX << (128 - length)
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(output, "{byte:02x}").expect("write hash");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::{
        Class, Prefix, class_for_name, normalize_entries, parse_csv, parse_prefix,
        render_transport_json,
    };

    #[test]
    fn csv_parser_preserves_quoted_commas_and_newlines() {
        let records = parse_csv("a,b\n\"c,d\",\"e\nf\"\n").expect("CSV");
        assert_eq!(records[1], ["c,d", "e\nf"]);
    }

    #[test]
    fn registry_names_map_to_conservative_classes() {
        assert_eq!(class_for_name("Private-Use", false), Class::Private);
        assert_eq!(class_for_name("Unique-Local", false), Class::UniqueLocal);
        assert_eq!(class_for_name("Documentation", false), Class::Documentation);
        assert_eq!(class_for_name("ordinary", true), Class::GlobalUnicast);
        assert_eq!(class_for_name("ordinary", false), Class::Reserved);
    }

    #[test]
    fn prefix_parser_strips_registry_footnotes() {
        assert_eq!(
            parse_prefix("192.0.0.0/24 [2]", false),
            Ok(Prefix::V4 {
                network: 0xc0000000,
                length: 24
            })
        );
        assert!(parse_prefix("192.0.2.0/128", false).is_err());
    }

    #[test]
    fn conflicting_prefix_classes_are_rejected() {
        let error = normalize_entries(vec![
            super::Entry {
                prefix: Prefix::V4 {
                    network: 1,
                    length: 32,
                },
                class: Class::Reserved,
            },
            super::Entry {
                prefix: Prefix::V4 {
                    network: 1,
                    length: 32,
                },
                class: Class::GlobalUnicast,
            },
        ])
        .expect_err("conflict");
        assert!(error.contains("conflicting classes"));
    }

    #[test]
    fn direct_transport_contract_freezes_security_and_pool_bounds() {
        let contract = render_transport_json();
        assert!(contract.contains("\"crypto_provider\": \"ring\""));
        assert!(contract.contains("\"certificate_verification\": \"required\""));
        assert!(contract.contains("\"alpn\": []"));
        assert!(contract.contains("\"http1_without_alpn\": true"));
        assert!(contract.contains("\"max_connections_per_origin\": 8"));
        assert!(contract.contains("\"fresh_destination_admission_per_physical_connection\": true"));
        assert!(contract.contains("\"incomplete_or_failed_response_reusable\": false"));
        assert!(contract.contains("\"ordinary_cli_rpc_uri_exposure\""));
    }
}
