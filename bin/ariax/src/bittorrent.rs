use ariax_engine::{AddMagnet, AddTorrent, BitTorrentOptions, Engine};
use std::ffi::OsString;
use std::path::Path;

pub(super) enum Admission {
    Torrent(AddTorrent),
    Magnet(AddMagnet),
}

struct Arguments {
    options: BitTorrentOptions,
    position: Option<i64>,
    web_seeds: Vec<String>,
    positional: Vec<String>,
}

fn arguments(values: &[OsString]) -> Result<Arguments, String> {
    let mut pairs = Vec::new();
    let mut position = None;
    let mut web_seeds = Vec::new();
    let mut positional = Vec::new();
    let mut literal = false;
    for value in values {
        let text = value.to_str().ok_or("BitTorrent arguments must be UTF-8")?;
        if !literal && text == "--" {
            literal = true;
            continue;
        }
        if !literal && let Some(flag) = text.strip_prefix("--") {
            let (name, value) = flag
                .split_once('=')
                .ok_or("BitTorrent options require --NAME=VALUE")?;
            match name {
                "position" => {
                    let value = value
                        .parse::<i64>()
                        .ok()
                        .filter(|value| *value >= -1)
                        .ok_or("invalid queue position")?;
                    if position.replace(value).is_some() {
                        return Err("duplicate queue position".into());
                    }
                }
                "web-seed" => {
                    if web_seeds.len() == 64 || value.len() > 8192 {
                        return Err("web-seed limit exceeded".into());
                    }
                    web_seeds.push(value.to_owned());
                }
                _ => pairs.push((name.to_owned(), value.to_owned())),
            }
        } else {
            if positional.len() == 1 || text.len() > 65536 {
                return Err("expected one bounded BitTorrent input".into());
            }
            positional.push(text.to_owned());
        }
    }
    Ok(Arguments {
        options: BitTorrentOptions::from_pairs(pairs).map_err(|error| error.to_string())?,
        position,
        web_seeds,
        positional,
    })
}

pub(super) fn torrent(path: &Path, flags: &[OsString]) -> Result<Admission, String> {
    use std::io::Read as _;
    let args = arguments(flags)?;
    if !args.positional.is_empty() {
        return Err("unexpected torrent argument".into());
    }
    let file = std::fs::File::open(path).map_err(|_| "cannot open torrent file")?;
    if !file
        .metadata()
        .map_err(|_| "cannot inspect torrent file")?
        .is_file()
    {
        return Err("torrent input must be a regular file".into());
    }
    let mut bytes = Vec::new();
    file.take(16 * 1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read torrent file")?;
    if bytes.len() > 16 * 1024 * 1024 {
        return Err("torrent exceeds 16 MiB".into());
    }
    Ok(Admission::Torrent(AddTorrent {
        bytes,
        web_seeds: args.web_seeds,
        options: args.options,
        position: args.position,
    }))
}

pub(super) fn magnet(values: &[OsString]) -> Result<Admission, String> {
    let mut args = arguments(values)?;
    if args.positional.len() != 1 || !args.web_seeds.is_empty() {
        return Err("magnet requires exactly one URI and no separate web seeds".into());
    }
    let uri = args.positional.remove(0);
    if !uri.starts_with("magnet:?") {
        return Err("invalid magnet URI".into());
    }
    Ok(Admission::Magnet(AddMagnet {
        uri,
        options: args.options,
        position: args.position,
    }))
}

pub(super) async fn run(engine: &Engine, request: Admission) -> Result<(), String> {
    let gid = match request {
        Admission::Torrent(request) => engine.add_torrent(request).await,
        Admission::Magnet(request) => engine.add_magnet(request).await,
    }
    .map_err(|error| error.to_string())?;
    println!("added {gid}");
    super::direct_wait(engine, gid).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_torrent_options_share_bounds_and_feature_rejection() {
        for values in [
            vec!["--position=-2"],
            vec!["--seed-ratio=NaN"],
            vec!["--select-file=0"],
            vec!["--pause=true", "--pause=false"],
        ] {
            assert!(
                arguments(&values.into_iter().map(OsString::from).collect::<Vec<_>>()).is_err()
            );
        }
        let values = [
            OsString::from("--pause=true"),
            OsString::from("--bt-max-peers=32"),
        ];
        #[cfg(feature = "full")]
        assert!(arguments(&values).is_ok());
        #[cfg(not(feature = "full"))]
        assert!(
            arguments(&values)
                .err()
                .unwrap()
                .contains("BitTorrent feature unavailable")
        );
    }
}
