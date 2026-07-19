use datafusion::common::error::Result;

pub fn main() -> Result<()> {
    lakelet_app::server::run()
}
