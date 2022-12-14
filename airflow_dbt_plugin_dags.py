from airflow import DAG
from airflow.models import Variable
from airflow_dbt.operators.dbt_operator import (
    DbtSeedOperator,
    DbtSnapshotOperator,
    DbtRunOperator,
    DbtTestOperator,
)
from airflow.utils.dates import days_ago

synq_token = Variable.get("SYNQ_TOKEN", default_var=None)

default_args = {
    "dir": "/opt/airflow/dags/repo/dbt_example",
    "start_date": days_ago(0),
    "profiles_dir": "/opt/airflow/dags/repo/dbt_example",
}

default_args_synq = default_args.copy()
default_args_synq.update({"env": {"SYNQ_TOKEN": synq_token}, "dbt_bin": "synq-dbt"})

###
# DAGs
###

###
# Vanilla dbt run
###
with DAG(dag_id="dbt", default_args=default_args, schedule_interval="@daily") as dag:

    dbt_seed = DbtSeedOperator(task_id="dbt_seed")

    dbt_snapshot = DbtSnapshotOperator(task_id="dbt_snapshot")

    dbt_run = DbtRunOperator(task_id="dbt_run")

    dbt_test = DbtTestOperator(
        task_id="dbt_test",
        retries=0,  # Failing tests would fail the task, and we don't want Airflow to try again
    )

    dbt_seed >> dbt_snapshot >> dbt_run >> dbt_test


##
# Dbt reporting to synq with synq-dby
# IMPORTANT: Because of a missing feature the SYNQ_TOKEN is not passed via the DbtOperator
#            you need to start the Airflow worker process with the SYNQ_TOKEN environment variable set
##

with DAG(
    dag_id="dbt_with_synq", default_args=default_args_synq, schedule_interval="@daily"
) as dag_synq:

    dbt_seed = DbtSeedOperator(task_id="dbt_seed_synq")

    dbt_snapshot = DbtSnapshotOperator(task_id="dbt_snapshot_synq")

    dbt_run = DbtRunOperator(task_id="dbt_run_synq")

    dbt_test = DbtTestOperator(
        task_id="dbt_test_synq",
        retries=0,  # Failing tests would fail the task, and we don't want Airflow to try again
    )

    (dbt_seed >> dbt_snapshot >> dbt_run >> dbt_test)
