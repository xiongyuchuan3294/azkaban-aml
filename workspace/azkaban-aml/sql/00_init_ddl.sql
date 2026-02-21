create database if not exists aml_demo;
alter database aml_demo set location 'file:${hivevar:warehouse_external_dir}/aml_demo.db';
alter database aml_demo set managedlocation 'file:${hivevar:warehouse_managed_dir}/aml_demo.db';
use aml_demo;

create table if not exists ods_txn_raw (
  txn_id string,
  acct_id string,
  counterparty_acct string,
  channel string,
  amount decimal(18,2),
  currency string,
  txn_time string,
  province string
)
partitioned by (dt string)
stored as orc;

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

create table if not exists ads_aml_alert_di (
  acct_id string,
  rule_code string,
  rule_desc string,
  risk_level string,
  evidence string,
  etl_time timestamp
)
partitioned by (dt string)
stored as orc;
