import os

from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.utils.dates import days_ago
from kubernetes.client import models as k8s

# SYNQ_TOKEN is requred to authenticate with SYNQ
SYNQ_TOKEN = Variable.get("SYNQ_TOKEN", default_var=None)

DOCKER_IMAGE = Variable.get("DOCKER_IMAGE", "")

env_dict = {"SYNQ_TOKEN": SYNQ_TOKEN}
env_dict.update(Variable.get("SYNQ_OBJECT", {}, deserialize_json=True))
default_args = {
    "start_date": days_ago(0),
    "env": env_dict,
}

DOCKER_IMAGE = "ghcr.io/getsynq/dbt-postgres-dbtsynq:latest"

###
# DAGs
###

with DAG(
    dag_id="kubernetes_dbt_with_synq",
    default_args=default_args,
    schedule_interval="@daily",
) as dag_run:

    run_task = KubernetesPodOperator(
        task_id="dbt_airflow_k8s_run",
        name="dbt-task-run",
        namespace="airflow-dbt",
        in_cluster=True,
        image=DOCKER_IMAGE,
        cmds=["bash", "-cex"],
        arguments=["sleep 300"],
        env_vars=env_dict,
        is_delete_operator_pod=False,
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
