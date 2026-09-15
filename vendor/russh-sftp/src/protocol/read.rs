use super::{impl_packet_for, impl_request_id, Packet, RequestId};

/// Implementation for `SSH_FXP_READ`
#[derive(Debug, Serialize, Deserialize)]
pub struct Read {
    pub id: u32,
    #[serde(with = "serde_bytes")]
    pub handle: Vec<u8>,
    pub offset: u64,
    pub len: u32,
}

impl_request_id!(Read);
impl_packet_for!(Read);
