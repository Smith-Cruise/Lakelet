use datafusion::common::error::Result;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

pub fn main() -> Result<()> {
    lakelet_app::server::run()
}
