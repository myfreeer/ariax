use super::{impl_packet_for, impl_request_id, Packet, RequestId};

/// Implementation for `SSH_FXP_FSTAT`
#[derive(Debug, Serialize, Deserialize)]
pub struct Fstat {
    pub id: u32,
    #[serde(with = "serde_bytes")]
    pub handle: Vec<u8>,
}

impl_request_id!(Fstat);
impl_packet_for!(Fstat);
