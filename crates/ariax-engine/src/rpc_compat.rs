//! Wire projection and validation shared by every JSON-RPC transport.

use crate::HttpRpcBackendError;
use serde_json::Value;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RpcCompatibility {
    #[default]
    Aria2,
    Extended,
    Strict,
}

impl RpcCompatibility {
    pub fn parse(value: &str) -> Result<Self, HttpRpcBackendError> {
        match value {
            "aria2" => Ok(Self::Aria2),
            "extended" => Ok(Self::Extended),
            "strict" => Ok(Self::Strict),
            _ => Err(HttpRpcBackendError::new(
                -32602,
                "invalid RPC compatibility mode",
            )),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Aria2 => "aria2",
            Self::Extended => "extended",
            Self::Strict => "strict",
        }
    }
}

const ARIA2_STATUS_KEYS: &[&str] = &[
    "gid",
    "status",
    "totalLength",
    "completedLength",
    "downloadSpeed",
    "uploadSpeed",
    "connections",
    "errorCode",
    "errorMessage",
    "uploadLength",
    "infoHash",
    "seeder",
    "numSeeders",
    "bittorrent",
    "pieceLength",
    "numPieces",
    "dir",
    "files",
    "followedBy",
];
const EXTENDED_STATUS_KEYS: &[&str] = &[
    "slotState",
    "slotReason",
    "slowSince",
    "demotionCount",
    "readmitAfter",
    "retryWaitConsumesSlot",
    "verifiedLength",
    "verifiedSpeed",
    "retryCount",
    "discardedLength",
    "discardBudgetConsumed",
    "discardBudgetRemaining",
    "receivedPayloadLength",
    "acceptedLength",
    "wireSpeed",
    "usefulSpeed",
    "smoothedSpeed",
    "sampleAge",
    "connectionCondition",
    "conditionReason",
    "rateDebt",
    "retryDiagnostic",
    "infoHashV2",
    "btCheckpointDirty",
    "btCheckpointError",
    "btDownloadedLength",
    "btSeedTime",
];

fn status_keys<'a>(method: &str, params: &'a Value) -> Option<&'a Vec<Value>> {
    let index = match method {
        "aria2.tellStatus" | "tellStatus" => 1,
        "aria2.tellActive" | "tellActive" => 0,
        "aria2.tellWaiting" | "tellWaiting" | "aria2.tellStopped" | "tellStopped" => 2,
        _ => return None,
    };
    params.get(index).and_then(Value::as_array)
}

pub(crate) fn validate(
    mode: RpcCompatibility,
    method: &str,
    params: &Value,
) -> Result<(), HttpRpcBackendError> {
    if mode != RpcCompatibility::Strict {
        return Ok(());
    }
    if !crate::RPC_METHODS.contains(&method) {
        return Err(HttpRpcBackendError::new(
            -32601,
            "method is not in the implemented compatibility matrix",
        ));
    }
    if let Some(keys) = status_keys(method, params)
        && keys.iter().any(|key| {
            key.as_str().is_none_or(|key| {
                !ARIA2_STATUS_KEYS.contains(&key) && !EXTENDED_STATUS_KEYS.contains(&key)
            })
        })
    {
        return Err(HttpRpcBackendError::new(
            -32602,
            "status key is not in the implemented compatibility matrix",
        ));
    }
    Ok(())
}

pub(crate) fn project(
    mode: RpcCompatibility,
    method: &str,
    explicit_keys: bool,
    value: &mut Value,
) {
    if mode == RpcCompatibility::Extended {
        return;
    }
    let project_status = |value: &mut Value| {
        if !explicit_keys && let Some(object) = value.as_object_mut() {
            object.retain(|key, _| ARIA2_STATUS_KEYS.contains(&key.as_str()));
        }
    };
    match method {
        "aria2.tellStatus" | "tellStatus" => project_status(value),
        "aria2.tellActive" | "tellActive" | "aria2.tellWaiting" | "tellWaiting"
        | "aria2.tellStopped" | "tellStopped" => {
            if let Some(values) = value.as_array_mut() {
                for value in values {
                    project_status(value);
                }
            }
        }
        "aria2.getOption" | "getOption" | "aria2.getGlobalOption" | "getGlobalOption" => {
            if let Some(options) = value.as_object_mut() {
                options.retain(|name, _| {
                    ariax_config::builtin_registry()
                        .find(name)
                        .is_some_and(|definition| definition.aria2_available)
                });
            }
        }
        _ => {}
    }
}

pub(crate) fn has_explicit_keys(method: &str, params: &Value) -> bool {
    status_keys(method, params).is_some()
}
