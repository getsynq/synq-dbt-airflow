import os

from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.utils.dates import days_ago
from kubernetes.client import models as k8s

##
# Settings
###

INSTALL_SYNQDBT = True
PULL_DBT_PROJECT = True

# Kubernetes namespace
NAMESPACE = Variable.get("NAMESPACE", default_var="airflow-dbt")

# SYNQ_TOKEN is requred to authenticate with SYNQ
SYNQ_TOKEN = Variable.get("SYNQ_TOKEN", default_var=None)

# Path to synq binary
SYNQ_DBT_BIN_PATH = Variable.get(
    "SYNQ_DBT_BIN_PATH", default_var="/usr/app/dbt/bin/synqdbt"
)

DBT_PROJECT_DIR = Variable.get(
    "DBT_PROJECT_PATH", "/usr/app/dbt/gitsync/repo/dbt_example"
)
DBT_PROFILES_DIR = Variable.get(
    "DBT_PROFILES_DIR", "/usr/app/dbt/gitsync/repo/dbt_example"
)


env_dict = {"SYNQ_TOKEN": SYNQ_TOKEN, "DBT_PROFILES_DIR": DBT_PROFILES_DIR}
# Config JSON object for overrides OPTIONAL
env_dict.update(Variable.get("CONFIG_OBJECT", {}, deserialize_json=True))
# Filter key value pairs that have value None
env_dict = {k: v for k, v in env_dict.items() if v is not None}

default_args = {
    "start_date": days_ago(0),
    "env": env_dict,
}

###
# Helper functions
###


# Install synq-dbt binary. OPTIONAL if you have synq-dbt in docker image.
# If your image does not include synq-dbt binary, you can install it before your Kubernetes Operators starts
def create_synq_dbt_init_container():
    # Synq-dbt version to install
    SYNQ_DBT_VERSION = Variable.get("SYNQ_DBT_VERSION", default_var="v1.3.1")

    synqdbt_volume = k8s.V1Volume(
        name="synqdbt-data", empty_dir=k8s.V1EmptyDirVolumeSource()
    )

    synqdbt_volume_mount = k8s.V1VolumeMount(
        mount_path=os.path.dirname(SYNQ_DBT_BIN_PATH),
        name="synqdbt-data",
        sub_path=None,
        read_only=False,
    )

    init_container_synqdbt_install = k8s.V1Container(
        name="init-container-install-synqdbt",
        image="alpine",
        volume_mounts=[synqdbt_volume_mount],
        command=["/bin/sh", "-cx"],
        args=[
            (
                "apk add --no-cache wget; "
                f"wget -O ./synq-dbt https://github.com/getsynq/synq-dbt/releases/download/{SYNQ_DBT_VERSION}/synq-dbt-amd64-linux; "
                "chmod +x ./synq-dbt; "
                f"mv ./synq-dbt {SYNQ_DBT_BIN_PATH}; "
            )
        ],
    )
    return synqdbt_volume, synqdbt_volume_mount, init_container_synqdbt_install


# Git pull your dbt repository before Kubernetes Operator starts.
# OPTIONAL if your dbt models are in your docker image.
def create_gitsync_init_container():
    DBT_PROJECT_REPO = Variable.get(
        "DBT_PROJECT_REPO", "https://github.com/getsynq/synq-dbt-airflow.git"
    )
    DBT_PROJECT_BRANCH = Variable.get("DBT_PROJECT_BRANCH", "main")

    dbt_project_volume = k8s.V1Volume(
        name="dbt-data", empty_dir=k8s.V1EmptyDirVolumeSource()
    )
    dbt_project_volume_mount = k8s.V1VolumeMount(
        mount_path="/usr/app/dbt/gitsync",
        name="dbt-data",
        sub_path=None,
        read_only=False,
    )

    init_container_gitsync = k8s.V1Container(
        name="init-container-gitsync",
        image="registry.k8s.io/git-sync/git-sync:v3.6.2",
        volume_mounts=[dbt_project_volume_mount],
        args=[
            f"--repo={DBT_PROJECT_REPO}",
            f"--branch={DBT_PROJECT_BRANCH}",
            "--root=/usr/app/dbt/gitsync",
            "--dest=repo",
            "--one-time",
        ],
    )
    return dbt_project_volume, dbt_project_volume_mount, init_container_gitsync


###
# DAGs
###

with DAG(
    dag_id="kubernetes_advanced_dag",
    default_args=default_args,
    schedule_interval="@daily",
) as dag:

    volumes = []
    volume_mounts = []
    init_containers = []

    # Install synq-dbt binary. OPTIONAL if you have synq-dbt in docker image.
    if INSTALL_SYNQDBT:
        volume, mount, init_container = create_synq_dbt_init_container()
        volumes.append(volume)
        volume_mounts.append(mount)
        init_containers.append(init_container)
    # Git pull your dbt repository before Kubernetes Operator starts.
    # OPTIONAL if your dbt models are in docker image.
    if PULL_DBT_PROJECT:
        volume, mount, init_container = create_gitsync_init_container()
        volumes.append(volume)
        volume_mounts.append(mount)
        init_containers.append(init_container)

    task = KubernetesPodOperator(
        task_id="dbt_airflow_k8s_advanced",
        name="dbt-task-advanced",
        namespace=NAMESPACE,
        in_cluster=True,
        image="ghcr.io/dbt-labs/dbt-postgres:1.2.3",
        cmds=["bash", "-cex"],
        arguments=[
            f"pushd {DBT_PROJECT_DIR}; "
            f"{SYNQ_DBT_BIN_PATH} run; "
            f"{SYNQ_DBT_BIN_PATH} test; "
        ],
        env_vars=env_dict,
        volumes=volumes,
        volume_mounts=volume_mounts,
        init_containers=init_containers,
        is_delete_operator_pod=True,
    )
