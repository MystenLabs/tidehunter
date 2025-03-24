use crate::math::starting_u32;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::cmp::Ordering;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CellId {
    Integer(usize),
    Bytes(CellIdBytesContainer),
}

pub type CellIdBytesContainer = SmallVec<[u8; 16]>;

impl CellId {
    pub fn mutex_seed(&self) -> usize {
        match self {
            CellId::Integer(p) => *p,
            CellId::Bytes(bytes) => starting_u32(&bytes) as usize,
        }
    }
}

impl PartialOrd for CellId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (CellId::Integer(this), CellId::Integer(other)) => this.partial_cmp(other),
            (CellId::Bytes(this), CellId::Bytes(other)) => this.partial_cmp(other),
            (CellId::Integer(_), CellId::Bytes(_)) => {
                panic!("Not comparable cell ids: left integer, right bytes")
            }
            (CellId::Bytes(_), CellId::Integer(_)) => {
                panic!("Not comparable cell ids: left bytes, right integer")
            }
        }
    }
}

impl Ord for CellId {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (CellId::Integer(this), CellId::Integer(other)) => this.cmp(other),
            (CellId::Bytes(this), CellId::Bytes(other)) => this.cmp(other),
            (CellId::Integer(_), CellId::Bytes(_)) => {
                panic!("Not comparable cell ids: left integer, right bytes")
            }
            (CellId::Bytes(_), CellId::Integer(_)) => {
                panic!("Not comparable cell ids: left bytes, right integer")
            }
        }
    }
}
