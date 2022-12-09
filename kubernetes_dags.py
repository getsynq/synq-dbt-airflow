from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
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

# Vanilla k8s
with DAG(
    dag_id="kubernetes", default_args=default_args, schedule_interval="@daily"
) as dag:

    task = KubernetesPodOperator(
        name="hello-dry-run",
        namespace="airflow-dbt",
        in_cluster=True,
        image="ghcr.io/dbt-labs/dbt-postgres:1.2.3",
        cmds=["bash", "-cx"],
        arguments=["sleep 600"],
        labels={"foo": "bar"},
        task_id="dry_run_demo",
    )
