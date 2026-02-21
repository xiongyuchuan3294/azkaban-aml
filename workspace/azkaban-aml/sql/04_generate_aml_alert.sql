use aml_demo;

insert overwrite table ads_aml_alert_di partition (dt='${hivevar:run_date_std}')
select
  acct_id,
  rule_code,
  rule_desc,
  risk_level,
  evidence,
  current_timestamp() as etl_time
from (
  select
    acct_id,
    'R001' as rule_code,
    'Single-day high amount or very large single transaction' as rule_desc,
    'HIGH' as risk_level,
    concat('total_amt=', cast(total_amt as string), ', max_single_amt=', cast(max_single_amt as string)) as evidence
  from dws_acct_risk_stat_di
  where dt = '${hivevar:run_date_std}'
    and (total_amt >= 200000 or max_single_amt >= 100000)

  union all

  select
    acct_id,
    'R002' as rule_code,
    'Frequent night transactions' as rule_desc,
    'MEDIUM' as risk_level,
    concat('night_txn_cnt=', cast(night_txn_cnt as string), ', total_amt=', cast(total_amt as string)) as evidence
  from dws_acct_risk_stat_di
  where dt = '${hivevar:run_date_std}'
    and night_txn_cnt >= 3
    and total_amt >= 50000

  union all

  select
    acct_id,
    'R003' as rule_code,
    'Many counterparties with moderate total amount' as rule_desc,
    'MEDIUM' as risk_level,
    concat('cp_acct_cnt=', cast(cp_acct_cnt as string), ', txn_cnt=', cast(txn_cnt as string)) as evidence
  from dws_acct_risk_stat_di
  where dt = '${hivevar:run_date_std}'
    and cp_acct_cnt >= 5
    and txn_cnt >= 6
    and total_amt >= 80000
) alert_union;
