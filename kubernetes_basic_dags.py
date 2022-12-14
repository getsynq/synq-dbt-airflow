from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.utils.dates import days_ago

# SYNQ_TOKEN is requred to authenticate with SYNQ
SYNQ_TOKEN = Variable.get("SYNQ_TOKEN", default_var=None)

env_dict = {"SYNQ_TOKEN": SYNQ_TOKEN}
# Config JSON object for overrides OPTIONAL
env_dict.update(Variable.get("CONFIG_OBJECT", {}, deserialize_json=True))
default_args = {
    "start_date": days_ago(0),
    "env": env_dict,
}

DOCKER_IMAGE = "ghcr.io/getsynq/dbt-postgres-synq-dbt:1.0.0"

###
# DAGs
###

with DAG(
    dag_id="kubernetes_basic_dag",
    default_args=default_args,
    schedule_interval="@daily",
) as dag_run:

    run_task = KubernetesPodOperator(
        task_id="dbt_airflow_k8s_run",
        name="dbt-task-run",
        namespace="airflow-dbt",
        in_cluster=True,
        image=DOCKER_IMAGE,
        arguments=["run"],
        env_vars=env_dict,
        is_delete_operator_pod=True,
    )

    test_task = KubernetesPodOperator(
        task_id="dbt_airflow_k8s_test",
        name="dbt-task-test",
        namespace="airflow-dbt",
        in_cluster=True,
        image=DOCKER_IMAGE,
        arguments=["test"],
        env_vars=env_dict,
        is_delete_operator_pod=True,
    )

    run_task >> test_task
