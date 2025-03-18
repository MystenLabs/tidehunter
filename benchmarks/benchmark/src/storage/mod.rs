#[cfg(feature = "rocks")]
pub mod rocks;
#[cfg(not(feature = "rocks"))]
pub mod tidehunter;

use minibytes::Bytes;

pub trait Storage: Clone + Sync + Send + 'static {
    fn insert(&self, k: Bytes, v: Bytes);
    fn get(&self, k: &[u8]) -> Option<Bytes>;
}
