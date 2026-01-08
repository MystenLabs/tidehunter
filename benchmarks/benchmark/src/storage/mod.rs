// FASTER backend - only available on Linux with the feature enabled
#[cfg(all(target_os = "linux", feature = "enable-faster"))]
pub mod faster;

pub mod rocks;
pub mod tidehunter;

use minibytes::Bytes;

pub trait Storage: Sync + Send + 'static {
    fn insert(&self, k: Bytes, v: Bytes);

    fn get(&self, k: &[u8]) -> Option<Bytes>;

    fn get_lt(&self, k: &[u8], iterations: usize) -> Vec<Bytes>;

    fn exists(&self, k: &[u8]) -> bool;

    fn name(&self) -> &'static str;
}
