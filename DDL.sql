create table tareLoad_edu
(
    tare_id     Int64,
    dt          DateTime,
    office_id   Int32,
    office_name String,
    is_pvz      Int8
) engine = MergeTree order by tare_id settings index_granularity = 8192;
