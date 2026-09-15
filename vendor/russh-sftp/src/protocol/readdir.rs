use super::{impl_packet_for, impl_request_id, Packet, RequestId};

/// Implementation for `SSH_FXP_READDIR`
#[derive(Debug, Serialize, Deserialize)]
pub struct ReadDir {
    pub id: u32,
    #[serde(with = "serde_bytes")]
    pub handle: Vec<u8>,
}

impl_request_id!(ReadDir);
impl_packet_for!(ReadDir);
