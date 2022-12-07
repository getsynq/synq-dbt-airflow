import os
import stat
import urllib.request

from airflow import DAG
from airflow.models import Variable
from airflow_dbt.operators.dbt_operator import (
    DbtSeedOperator,
    DbtSnapshotOperator,
    DbtRunOperator,
    DbtTestOperator,
)
from airflow.operators.python import (
    ShortCircuitOperator,
    PythonOperator,
)
from airflow.utils.dates import days_ago

synq_token = Variable.get("SYNQ_TOKEN", default_var=None)

default_args = {
    "dir": "/opt/airflow/dags/repo/dbt_example",
    "start_date": days_ago(0),
    "profiles_dir": "/opt/airflow/dags/repo/dbt_example",
}

###
# DAGs
###

# Vanilla dbt
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
# Install latest Synq dbt
# For production you can add SYNQ dbt to your airflow Docker image at image build time
##
with DAG(
    dag_id="dag_install_synq_dbt", default_args=default_args
) as dag_install_synq_dbt:

    def install_synq_dbt_f():

        SYNQ_VERSION = Variable.get("SYNQ_VERSION", "v1.2.3")
        URL = f"https://github.com/getsynq/synq-dbt/releases/download/{SYNQ_VERSION}/synq-dbt-amd64-linux"
        urllib.request.urlretrieve(URL, "/opt/airflow/.local/bin/synq-dbt")
        os.chmod("/opt/airflow/.local/bin/synq-dbt", stat.S_IXUSR)

    install_synq_dbt = PythonOperator(
        task_id="install_synq_dbt", python_callable=install_synq_dbt_f
    )


##
# Dbt reporting to synq
##
default_args_synq = default_args.copy()
default_args_synq.update(
    {"env": {"SYNQ_TOKEN": synq_token}, "dbt_bin": "/opt/airflow/bin/synq-dbt"}
)

with DAG(
    dag_id="dbt_with_synq", default_args=default_args_synq, schedule_interval="@daily"
) as dag_synq:
    # We need the synq tooken for synq integrated dags
    synq_token_defined = ShortCircuitOperator(
        task_id="synq_token_defined", python_callable=lambda: synq_token
    )

    dbt_seed = DbtSeedOperator(task_id="dbt_seed_synq")

    dbt_snapshot = DbtSnapshotOperator(task_id="dbt_snapshot_synq")

    dbt_run = DbtRunOperator(task_id="dbt_run_synq")

    dbt_test = DbtTestOperator(
        task_id="dbt_test_synq",
        retries=0,  # Failing tests would fail the task, and we don't want Airflow to try again
    )

    synq_token_defined >> dbt_seed >> dbt_snapshot >> dbt_run >> dbt_test
