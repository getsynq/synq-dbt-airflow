from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.utils.dates import days_ago
from kubernetes.client import models as k8s


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


synqdbt_volume_mount = k8s.V1VolumeMount(
    mount_path="/usr/app/dbt/synqdbt",
    name="synqdbt-data",
    sub_path=None,
    read_only=False,
)

dbt_project_volume_mount = k8s.V1VolumeMount(
    mount_path="/usr/app/dbt/dbt_project",
    name="dbt-data",
    sub_path=None,
    read_only=False,
)


init_container_gitsync = k8s.V1Container(
    name="init-container-gitsync",
    image="registry.k8s.io/git-sync/git-sync:v3.6.2",
    volume_mounts=[dbt_project_volume_mount],
    args=[
        "--repo=https://github.com/getsynq/synq-dbt-airflow.git",
        "--root=/usr/app/dbt/dbt_project",
        "--period=30s",
    ],
)

init_container_install = k8s.V1Container(
    name="init-container-install-synqdbt",
    image="alpine",
    volume_mounts=[synqdbt_volume_mount],
    command=["bash", "-cx"],
    args=[
        (
            "apk add --no-cache wget; "
            "SYNQ_VERSION=v1.2.3 wget -O ./synq-dbt https://github.com/getsynq/synq-dbt/releases/download/$SYNQ_VERSION/synq-dbt-amd64-linux; "
            "chmod +x ./synq-dbt; "
            "mv ./synq-dbt /usr/app/dbt/synqdbt; "
        )
    ],
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
        arguments=[
            "pushd usr/app/dbt/dbt_project; "
            "/usr/app/dbt/synqdbt run; "
            "/usr/app/dbt/synqdbt test"
        ],
        task_id="dbt_airflow_k8s",
        init_containers=[
            init_container_install,
            init_container_gitsync,
        ],
        volume_mounts=[
            synqdbt_volume_mount,
            dbt_project_volume_mount,
        ],
    )
