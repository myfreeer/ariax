use super::*;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum BitTorrentEncryption {
    Required,
    #[default]
    Preferred,
    Disabled,
}

/// Startup authority. Task options cannot relax destination or resource policy.
#[derive(Clone, Debug)]
pub struct BitTorrentConfig {
    pub listen: SocketAddr,
    pub allow_private_destinations: bool,
    pub dht: bool,
    pub peer_exchange: bool,
    pub encryption: BitTorrentEncryption,
    pub max_tasks: u32,
    pub max_peers: u32,
    pub max_open_files: u32,
    pub disk_threads: u32,
}

impl Default for BitTorrentConfig {
    fn default() -> Self {
        Self {
            listen: ([127, 0, 0, 1], 0).into(),
            allow_private_destinations: false,
            dht: true,
            peer_exchange: true,
            encryption: BitTorrentEncryption::Preferred,
            max_tasks: 32,
            max_peers: 256,
            max_open_files: 64,
            disk_threads: 1,
        }
    }
}

impl BitTorrentConfig {
    pub fn from_pairs(
        values: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, NativeApiError> {
        if !cfg!(feature = "bt") {
            return Err(NativeApiError::Control(HttpControlError::Unsupported(
                "BitTorrent feature unavailable",
            )));
        }
        let mut result = Self::default();
        let mut seen = std::collections::BTreeSet::new();
        for (name, value) in values {
            if !seen.insert(name.clone()) || value.len() > 128 {
                return Err(NativeApiError::InvalidConfiguration(
                    "duplicate or oversized BitTorrent startup option",
                ));
            }
            let definition = ariax_config::builtin_registry().find(&name).ok_or(
                NativeApiError::InvalidConfiguration("unsupported BitTorrent startup option"),
            )?;
            let parsed =
                ariax_config::parse_option_value(definition, &value, None).map_err(|_| {
                    NativeApiError::InvalidConfiguration("invalid BitTorrent startup value")
                })?;
            match (name.as_str(), parsed) {
                ("bt-listen-address", ariax_config::OptionValue::String(value)) => {
                    result.listen = value.parse().map_err(|_| {
                        NativeApiError::InvalidConfiguration(
                            "BitTorrent listener must be a numeric socket address",
                        )
                    })?
                }
                ("bt-allow-private-destinations", ariax_config::OptionValue::Bool(value)) => {
                    result.allow_private_destinations = value
                }
                ("enable-dht", ariax_config::OptionValue::Bool(value)) => result.dht = value,
                ("enable-peer-exchange", ariax_config::OptionValue::Bool(value)) => {
                    result.peer_exchange = value
                }
                ("bt-encryption", ariax_config::OptionValue::Enum(value)) => {
                    result.encryption = match value.as_str() {
                        "required" => BitTorrentEncryption::Required,
                        "preferred" => BitTorrentEncryption::Preferred,
                        "disabled" => BitTorrentEncryption::Disabled,
                        _ => {
                            return Err(NativeApiError::InvalidConfiguration(
                                "invalid BitTorrent encryption policy",
                            ));
                        }
                    }
                }
                _ => {
                    return Err(NativeApiError::InvalidConfiguration(
                        "unsupported BitTorrent startup option",
                    ));
                }
            }
        }
        Ok(result)
    }
    pub fn apply(self, plane: &mut HttpControlPlane) -> Result<(), NativeApiError> {
        #[cfg(feature = "bt")]
        {
            if !(1..=4096).contains(&self.max_tasks)
                || !(2..=16384).contains(&self.max_peers)
                || !(2..=16384).contains(&self.max_open_files)
                || !(1..=16).contains(&self.disk_threads)
            {
                return Err(NativeApiError::InvalidConfiguration(
                    "BitTorrent resource limit is out of range",
                ));
            }
            plane
                .configure_bittorrent(ariax_bt::BtAdapterConfig {
                    listen: self.listen.to_string(),
                    max_torrents: self.max_tasks,
                    peers: self.max_peers,
                    files: self.max_open_files,
                    disk_threads: self.disk_threads,
                    allow_private: self.allow_private_destinations,
                    dht: self.dht,
                    pex: self.peer_exchange,
                    encryption: match self.encryption {
                        BitTorrentEncryption::Required => 0,
                        BitTorrentEncryption::Preferred => 1,
                        BitTorrentEncryption::Disabled => 2,
                    },
                    ..ariax_bt::BtAdapterConfig::default()
                })
                .map_err(NativeApiError::Control)
        }
        #[cfg(not(feature = "bt"))]
        {
            let _ = (self, plane);
            Err(NativeApiError::Control(HttpControlError::Unsupported(
                "BitTorrent feature unavailable",
            )))
        }
    }
}

/// Validated task settings with typed builders and an explicit option-pair parser.
#[derive(Clone, Default)]
pub struct BitTorrentOptions {
    values: BTreeMap<String, String>,
}

impl fmt::Debug for BitTorrentOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BitTorrentOptions")
            .field("names", &self.values.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl BitTorrentOptions {
    pub fn from_pairs(
        values: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, NativeApiError> {
        let mut options = BTreeMap::new();
        let mut bytes = 0usize;
        for (name, value) in values {
            bytes = bytes
                .saturating_add(name.len())
                .saturating_add(value.len())
                .saturating_add(128);
            if bytes > 256 * 1024
                || options.len() >= 64
                || name == "dir"
                || options.insert(name, value).is_some()
            {
                return Err(NativeApiError::InvalidConfiguration(
                    "duplicate, oversized or unsupported BitTorrent option",
                ));
            }
        }
        let result = Self { values: options };
        crate::http_control::validate_bittorrent_options(&result.value())
            .map_err(NativeApiError::Control)?;
        Ok(result)
    }

    fn with(
        mut self,
        pairs: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, NativeApiError> {
        self.values.extend(pairs);
        Self::from_pairs(self.values)
    }

    pub fn paused(self, paused: bool) -> Result<Self, NativeApiError> {
        self.with([("pause".into(), paused.to_string())])
    }
    pub fn output(self, name: impl Into<String>) -> Result<Self, NativeApiError> {
        self.with([("out".into(), name.into())])
    }
    pub fn rates(
        self,
        download_bytes_per_second: u64,
        upload_bytes_per_second: u64,
    ) -> Result<Self, NativeApiError> {
        self.with([
            (
                "max-download-limit".into(),
                download_bytes_per_second.to_string(),
            ),
            (
                "max-upload-limit".into(),
                upload_bytes_per_second.to_string(),
            ),
        ])
    }
    pub fn peer_limit(self, peers: u32) -> Result<Self, NativeApiError> {
        self.with([("bt-max-peers".into(), peers.to_string())])
    }
    pub fn discovery(self, dht: bool, peer_exchange: bool) -> Result<Self, NativeApiError> {
        self.with([
            ("enable-dht".into(), dht.to_string()),
            ("enable-peer-exchange".into(), peer_exchange.to_string()),
        ])
    }
    pub fn metadata(self, only: bool, save: bool) -> Result<Self, NativeApiError> {
        self.with([
            ("bt-metadata-only".into(), only.to_string()),
            ("bt-save-metadata".into(), save.to_string()),
        ])
    }
    /// A ratio of 1,000 means one uploaded byte per downloaded byte.
    pub fn seeding(
        self,
        ratio_milli: u32,
        duration: Option<Duration>,
    ) -> Result<Self, NativeApiError> {
        let mut values = vec![(
            "seed-ratio".into(),
            format!("{}.{:03}", ratio_milli / 1000, ratio_milli % 1000),
        )];
        if let Some(duration) = duration {
            values.push((
                "seed-time".into(),
                (duration.as_secs_f64() / 60.0).to_string(),
            ));
        }
        self.with(values)
    }
    pub fn selected_files(self, indices: &[u32]) -> Result<Self, NativeApiError> {
        if indices.len() > 10_000 {
            return Err(NativeApiError::InvalidConfiguration(
                "too many selected files",
            ));
        }
        self.with([(
            "select-file".into(),
            indices
                .iter()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join(","),
        )])
    }
    pub fn file_names(self, names: &BTreeMap<u32, String>) -> Result<Self, NativeApiError> {
        let bytes = names.values().fold(0usize, |bytes, name| {
            bytes.saturating_add(name.len()).saturating_add(16)
        });
        if bytes > 65536 || names.len() > 10_000 {
            return Err(NativeApiError::InvalidConfiguration(
                "file mapping is too large",
            ));
        }
        self.with([(
            "index-out".into(),
            names
                .iter()
                .map(|(index, path)| format!("{index}={path}"))
                .collect::<Vec<_>>()
                .join("\n"),
        )])
    }
    pub fn trackers(self, add: &[String], exclude: &[String]) -> Result<Self, NativeApiError> {
        if add.len() > 64
            || exclude.len() > 64
            || add.iter().chain(exclude).any(|value| value.len() > 8192)
        {
            return Err(NativeApiError::InvalidConfiguration(
                "tracker list is too large",
            ));
        }
        let mut values = Vec::new();
        if !add.is_empty() {
            values.push(("bt-tracker".into(), add.join(",")));
        }
        if !exclude.is_empty() {
            values.push(("bt-exclude-tracker".into(), exclude.join(",")));
        }
        self.with(values)
    }
    pub fn checkpoint(self, bytes: usize, timeout: Duration) -> Result<Self, NativeApiError> {
        if timeout.subsec_nanos() != 0 {
            return Err(NativeApiError::InvalidConfiguration(
                "checkpoint timeout must be whole seconds",
            ));
        }
        self.with([
            ("bt-resume-data-limit".into(), bytes.to_string()),
            ("bt-resume-timeout".into(), timeout.as_secs().to_string()),
        ])
    }
    pub(super) fn input_bytes(&self) -> usize {
        self.values.iter().fold(8192usize, |bytes, (name, value)| {
            bytes
                .saturating_add(name.capacity())
                .saturating_add(value.capacity())
                .saturating_add(512)
        })
    }
    pub(super) fn value(&self) -> Value {
        Value::Object(
            self.values
                .iter()
                .map(|(name, value)| (name.clone(), Value::String(value.clone())))
                .collect(),
        )
    }
}

pub struct AddTorrent {
    pub bytes: Vec<u8>,
    pub web_seeds: Vec<String>,
    pub options: BitTorrentOptions,
    pub position: Option<i64>,
}
impl fmt::Debug for AddTorrent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AddTorrent")
            .field("bytes", &self.bytes.len())
            .field("web_seeds", &self.web_seeds.len())
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}
pub struct AddMagnet {
    pub uri: String,
    pub options: BitTorrentOptions,
    pub position: Option<i64>,
}
impl fmt::Debug for AddMagnet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AddMagnet")
            .field("uri_bytes", &self.uri.len())
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitTorrentStatus {
    pub info_hash: String,
    pub info_hash_v2: Option<String>,
    pub seeder: bool,
    pub peers: u64,
    pub seeders: u64,
    pub downloaded: u64,
    pub uploaded: u64,
    pub download_speed: u64,
    pub upload_speed: u64,
    pub seed_seconds: u64,
    pub checkpoint_dirty: bool,
}

impl BitTorrentStatus {
    pub(super) fn from_status(value: &Value) -> Result<Option<Self>, NativeApiError> {
        let Some(hash) = value.get("infoHash") else {
            return Ok(None);
        };
        let hash = hash
            .as_str()
            .filter(|hash| {
                matches!(hash.len(), 40 | 64) && hash.bytes().all(|b| b.is_ascii_hexdigit())
            })
            .ok_or(NativeApiError::InvalidResponse("invalid torrent identity"))?;
        let v2 = value
            .get("infoHashV2")
            .map(|value| {
                value
                    .as_str()
                    .filter(|hash| hash.len() == 64 && hash.bytes().all(|b| b.is_ascii_hexdigit()))
                    .map(str::to_owned)
                    .ok_or(NativeApiError::InvalidResponse(
                        "invalid v2 torrent identity",
                    ))
            })
            .transpose()?;
        Ok(Some(Self {
            info_hash: hash.into(),
            info_hash_v2: v2,
            seeder: match value.get("seeder").and_then(Value::as_str) {
                Some("true") => true,
                Some("false") => false,
                _ => return Err(NativeApiError::InvalidResponse("invalid seeder flag")),
            },
            peers: decimal_field(value, "connections")?,
            seeders: decimal_field(value, "numSeeders")?,
            downloaded: decimal_field(value, "btDownloadedLength")?,
            uploaded: decimal_field(value, "uploadLength")?,
            download_speed: decimal_field(value, "downloadSpeed")?,
            upload_speed: decimal_field(value, "uploadSpeed")?,
            seed_seconds: value
                .get("btSeedTime")
                .and_then(Value::as_u64)
                .ok_or(NativeApiError::InvalidResponse("invalid seed time"))?,
            checkpoint_dirty: value
                .get("btCheckpointDirty")
                .and_then(Value::as_bool)
                .ok_or(NativeApiError::InvalidResponse("invalid checkpoint state"))?,
        }))
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitTorrentPeer {
    pub peer_id: String,
    pub ip: IpAddr,
    #[serde(deserialize_with = "decimal_u64")]
    pub port: u64,
    pub bitfield: String,
    #[serde(deserialize_with = "text_bool")]
    pub am_choking: bool,
    #[serde(deserialize_with = "text_bool")]
    pub peer_choking: bool,
    #[serde(deserialize_with = "text_bool")]
    pub seeder: bool,
    #[serde(deserialize_with = "decimal_u64")]
    pub download_speed: u64,
    #[serde(deserialize_with = "decimal_u64")]
    pub upload_speed: u64,
}

impl Engine {
    pub async fn add_torrent(&self, request: AddTorrent) -> Result<Gid, NativeApiError> {
        use base64ct::Encoding as _;
        if request.bytes.len() > 16 * 1024 * 1024
            || request.web_seeds.len() > 64
            || request.web_seeds.iter().any(|value| value.len() > 8192)
        {
            return Err(NativeApiError::InvalidConfiguration(
                "torrent admission exceeds its input limit",
            ));
        }
        let lease = self.client.try_request(0).map_err(native_budget_error)?;
        lease
            .reserve(
                request
                    .bytes
                    .capacity()
                    .saturating_add(request.bytes.len().saturating_mul(2))
                    .saturating_add(request.options.input_bytes())
                    .saturating_add(
                        request
                            .web_seeds
                            .iter()
                            .map(|uri| uri.capacity().saturating_mul(2))
                            .sum::<usize>(),
                    ),
            )
            .map_err(native_budget_error)?;
        let value = self
            .call_control_admitted(
                "aria2.addTorrent",
                json!([
                    base64ct::Base64::encode_string(&request.bytes),
                    request.web_seeds,
                    request.options.value(),
                    request.position.unwrap_or(-1)
                ]),
                lease,
            )
            .await?;
        admitted_gid(&value)
    }
    pub async fn add_magnet(&self, request: AddMagnet) -> Result<Gid, NativeApiError> {
        if request.uri.len() > 65536 || !request.uri.starts_with("magnet:?") {
            return Err(NativeApiError::InvalidConfiguration(
                "invalid or oversized magnet URI",
            ));
        }
        let lease = self.client.try_request(0).map_err(native_budget_error)?;
        lease
            .reserve(
                request
                    .uri
                    .capacity()
                    .saturating_mul(2)
                    .saturating_add(request.options.input_bytes()),
            )
            .map_err(native_budget_error)?;
        let value = self
            .call_control_admitted(
                "aria2.addUri",
                json!([
                    [request.uri],
                    request.options.value(),
                    request.position.unwrap_or(-1)
                ]),
                lease,
            )
            .await?;
        admitted_gid(&value)
    }
    pub async fn peers(&self, gid: Gid) -> Result<Vec<BitTorrentPeer>, NativeApiError> {
        self.decode_control("aria2.getPeers", json!([gid.to_string()]))
            .await
    }
    pub async fn change_bittorrent_options(
        &self,
        gid: Gid,
        options: BitTorrentOptions,
    ) -> Result<(), NativeApiError> {
        let lease = self.client.try_request(0).map_err(native_budget_error)?;
        lease
            .reserve(options.input_bytes())
            .map_err(native_budget_error)?;
        self.call_control_admitted(
            "aria2.changeOption",
            json!([gid.to_string(), options.value()]),
            lease,
        )
        .await?;
        Ok(())
    }
}

fn admitted_gid(value: &Value) -> Result<Gid, NativeApiError> {
    value
        .as_str()
        .and_then(|value| value.parse().ok())
        .ok_or(NativeApiError::InvalidResponse(
            "invalid admitted torrent GID",
        ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bt_native_options_share_validation_and_explicit_feature_errors() {
        for values in [
            [("bt-max-peers", "0")],
            [("seed-ratio", "NaN")],
            [("index-out", "1=../outside")],
            [("bt-encryption", "required")],
        ] {
            assert!(
                BitTorrentOptions::from_pairs(values.map(|(k, v)| (k.into(), v.into()))).is_err()
            );
        }
        let parsed = BitTorrentOptions::default().peer_limit(32);
        #[cfg(feature = "bt")]
        assert!(parsed.is_ok());
        #[cfg(not(feature = "bt"))]
        assert!(matches!(
            parsed,
            Err(NativeApiError::Control(HttpControlError::Unsupported(_)))
        ));
    }

    #[cfg(feature = "bt")]
    #[tokio::test]
    async fn typed_torrent_admission_status_peers_options_and_json_use_the_same_owner() {
        let root = std::env::temp_dir().join(format!("ariax-native-bt-{}", std::process::id()));
        let engine = Engine::builder()
            .output_root(root.join("output"))
            .bittorrent(BitTorrentConfig {
                dht: false,
                peer_exchange: false,
                ..BitTorrentConfig::default()
            })
            .build()
            .await
            .unwrap();
        let gid = engine
            .add_torrent(AddTorrent {
                bytes: include_bytes!("../../../ariax-bt-libtorrent-sys/tests/fixtures/v1.torrent")
                    .to_vec(),
                web_seeds: Vec::new(),
                options: BitTorrentOptions::default().paused(true).unwrap(),
                position: None,
            })
            .await
            .unwrap();
        assert_eq!(
            engine.status(gid).await.unwrap().status,
            Aria2Status::Paused
        );
        assert_eq!(
            engine
                .status(gid)
                .await
                .unwrap()
                .bittorrent
                .unwrap()
                .info_hash
                .len(),
            40
        );
        assert!(engine.peers(gid).await.unwrap().is_empty());
        assert_eq!(engine.files(gid).await.unwrap()[0].length, 5000);
        engine
            .change_bittorrent_options(gid, BitTorrentOptions::default().peer_limit(32).unwrap())
            .await
            .unwrap();
        assert_eq!(engine.options(gid).await.unwrap()["bt-max-peers"], "32");
        assert!(
            engine
                .change_bittorrent_options(
                    gid,
                    BitTorrentOptions::default().metadata(true, false).unwrap()
                )
                .await
                .is_err()
        );
        let response = engine
            .rpc_json(
                format!(
                    r#"{{"jsonrpc":"2.0","id":1,"method":"aria2.tellStatus","params":["{gid}"]}}"#
                )
                .as_bytes(),
                crate::RpcCompatibility::Aria2,
            )
            .await
            .unwrap();
        let response: Value = serde_json::from_slice(&response).unwrap();
        assert_eq!(
            response["result"]["infoHash"],
            engine
                .status(gid)
                .await
                .unwrap()
                .bittorrent
                .unwrap()
                .info_hash
        );
        engine.shutdown().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}
