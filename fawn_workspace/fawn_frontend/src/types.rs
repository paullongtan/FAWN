use fawn_common::types::NodeInfo;

pub struct MigrateInfo {
    pub src_info: NodeInfo,
    pub start_id: u32,
    pub end_id: u32,
}

impl From<MigrateInfo> for fawn_common::fawn_frontend_api::MigrateInfo {
    fn from(migrate_info: MigrateInfo) -> Self {
        fawn_common::fawn_frontend_api::MigrateInfo {
            src_info: Some(migrate_info.src_info.into()),
            start_id: migrate_info.start_id,
            end_id: migrate_info.end_id,
        }
    }
}