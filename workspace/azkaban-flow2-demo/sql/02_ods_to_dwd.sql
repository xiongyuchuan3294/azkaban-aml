use aml_demo;

set hive.cbo.enable=false;
set hive.compute.query.using.stats=false;
set hive.stats.fetch.column.stats=false;
set hive.stats.autogather=false;
set hive.exec.parallel=false;
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;
set hive.exec.reducers.max=1;
set mapreduce.job.reduces=1;

create table if not exists dwd_txn_detail_di (
  txn_id string,
  acct_id string,
  counterparty_acct string,
  channel string,
  amount decimal(18,2),
  currency string,
  txn_time timestamp,
  province string,
  night_flag int,
  large_flag int
)
partitioned by (dt string)
stored as orc;

insert overwrite table dwd_txn_detail_di partition (dt='${hivevar:run_date_std}')
select
  txn_id,
  acct_id,
  counterparty_acct,
  channel,
  amount,
  currency,
  cast(txn_time as timestamp) as txn_time,
  province,
  case when substr(txn_time, 12, 2) between '00' and '05' then 1 else 0 end as night_flag,
  case when amount >= 50000 then 1 else 0 end as large_flag
from ods_txn_raw
where dt = '${hivevar:run_date_std}'
  and amount > 0
  and acct_id is not null;
