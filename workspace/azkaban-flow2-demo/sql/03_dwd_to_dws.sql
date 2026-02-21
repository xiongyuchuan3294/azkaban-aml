use aml_demo;

create table if not exists dws_acct_risk_stat_di (
  acct_id string,
  txn_cnt bigint,
  total_amt decimal(18,2),
  night_txn_cnt bigint,
  large_txn_cnt bigint,
  cp_acct_cnt bigint,
  max_single_amt decimal(18,2)
)
partitioned by (dt string)
stored as orc;

insert overwrite table dws_acct_risk_stat_di partition (dt='${hivevar:run_date_std}')
select
  acct_id,
  count(1) as txn_cnt,
  cast(sum(amount) as decimal(18,2)) as total_amt,
  sum(night_flag) as night_txn_cnt,
  sum(large_flag) as large_txn_cnt,
  count(distinct counterparty_acct) as cp_acct_cnt,
  cast(max(amount) as decimal(18,2)) as max_single_amt
from dwd_txn_detail_di
where dt = '${hivevar:run_date_std}'
group by acct_id;
