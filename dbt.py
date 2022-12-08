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

from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

synq_token = Variable.get("SYNQ_TOKEN", default_var=None)

default_args = {
    "dir": "/opt/airflow/dags/repo/dbt_example",
    "start_date": days_ago(0),
    "profiles_dir": "/opt/airflow/dags/repo/dbt_example",
}

default_args_synq = default_args.copy()
default_args_synq.update(
    {"env": {"SYNQ_TOKEN": synq_token}, "dbt_bin": "/opt/airflow/bin/synq-dbt"}
)

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
    dag_id="dag_install_synq_dbt", default_args=default_args, schedule_interval=None
) as dag_install_synq_dbt:

    def install_synq_dbt_f():
        dbt_bin = default_args_synq["dbt_bin"]
        dbt_bin_dir = os.path.dirname(dbt_bin)

        SYNQ_VERSION = Variable.get("SYNQ_VERSION", "v1.2.3")
        URL = f"https://github.com/getsynq/synq-dbt/releases/download/{SYNQ_VERSION}/synq-dbt-amd64-linux"

        if not os.path.exists(dbt_bin_dir):
            os.makedirs(dbt_bin_dir)

        if os.path.exists(dbt_bin):
            os.remove(dbt_bin)
        urllib.request.urlretrieve(URL, dbt_bin)
        os.chmod(dbt_bin, stat.S_IXUSR)

    install_synq_dbt = PythonOperator(
        task_id="install_synq_dbt", python_callable=install_synq_dbt_f
    )


##
# Dbt reporting to synq
##

with DAG(
    dag_id="dbt_with_synq", default_args=default_args_synq, schedule_interval="@daily"
) as dag_synq:
    # We need the synq tooken for synq integrated dags
    synq_token_defined = ShortCircuitOperator(
        task_id="synq_token_defined", python_callable=lambda: synq_token
    )

    install_synq_dbt = TriggerDagRunOperator(
        task_id="install_synq_dby", trigger_dag_id="", wait_for_completion=True
    )

    dbt_seed = DbtSeedOperator(task_id="dbt_seed_synq")

    dbt_snapshot = DbtSnapshotOperator(task_id="dbt_snapshot_synq")

    dbt_run = DbtRunOperator(task_id="dbt_run_synq")

    dbt_test = DbtTestOperator(
        task_id="dbt_test_synq",
        retries=0,  # Failing tests would fail the task, and we don't want Airflow to try again
    )

    (
        synq_token_defined
        >> install_synq_dbt
        >> dbt_seed
        >> dbt_snapshot
        >> dbt_run
        >> dbt_test
    )
