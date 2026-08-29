//! Build provenance, captured by `build.rs` at compile time.

/// What `lakelet --version` prints: the commit this binary was built from and
/// when that commit was made. Reads "an unknown commit" when the build had no
/// git metadata available.
pub const BUILD_INFO: &str = concat!("Lakelet: compiled with ", env!("LAKELET_BUILD_PROVENANCE"));
