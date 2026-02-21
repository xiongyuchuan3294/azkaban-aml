use aml_demo;

select
  acct_id,
  rule_code,
  rule_desc,
  risk_level,
  evidence,
  dt
from ads_aml_alert_di
where dt = '${hivevar:run_date_std}'
order by acct_id, rule_code;
