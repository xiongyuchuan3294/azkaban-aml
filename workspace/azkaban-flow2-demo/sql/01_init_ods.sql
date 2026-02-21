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

insert overwrite table ods_txn_raw partition (dt='${hivevar:run_date_std}')
select * from (
  select 'tx001','A001','C1001','ONLINE', 80000.00,'CNY','2026-02-18 00:23:10','SH' union all
  select 'tx002','A001','C1002','ONLINE', 90000.00,'CNY','2026-02-18 01:05:42','SH' union all
  select 'tx003','A001','C1003','ATM',    70000.00,'CNY','2026-02-18 02:11:01','SH' union all
  select 'tx004','A001','C1004','BRANCH', 30000.00,'CNY','2026-02-18 13:40:59','SH' union all
  select 'tx005','A002','C2001','ONLINE',  1200.00,'CNY','2026-02-18 11:08:00','BJ' union all
  select 'tx006','A002','C2002','ATM',      900.00,'CNY','2026-02-18 15:30:00','BJ' union all
  select 'tx007','A003','C3001','ONLINE', 15000.00,'CNY','2026-02-18 09:00:00','GD' union all
  select 'tx008','A003','C3002','ONLINE', 16000.00,'CNY','2026-02-18 09:30:00','GD' union all
  select 'tx009','A003','C3003','ONLINE', 17000.00,'CNY','2026-02-18 10:00:00','GD' union all
  select 'tx010','A003','C3004','ONLINE', 14000.00,'CNY','2026-02-18 10:30:00','GD' union all
  select 'tx011','A003','C3005','ONLINE', 15000.00,'CNY','2026-02-18 11:00:00','GD' union all
  select 'tx012','A003','C3006','ONLINE', 16000.00,'CNY','2026-02-18 11:30:00','GD'
) t;
