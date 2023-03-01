from airflow import DAG
from airflow.models import Variable
from airflow_dbt.operators.dbt_operator import (
    DbtSeedOperator,
    DbtSnapshotOperator,
    DbtRunOperator,
    DbtTestOperator,
    DbtBaseOperator
)
from airflow.utils.dates import days_ago
from airflow.utils.decorators import apply_defaults

synq_token = Variable.get("SYNQ_TOKEN", default_var=None)

default_args = {
    "dir": "/opt/airflow/dags/repo/dbt_example",
    "start_date": days_ago(0),
    "profiles_dir": "/opt/airflow/dags/repo/dbt_example",
}

default_args_synq = default_args.copy()

env_dict = {"SYNQ_TOKEN": synq_token}
# Config JSON object for overrides OPTIONAL
env_dict.update(Variable.get("CONFIG_OBJECT", {}, deserialize_json=True))

default_args_synq.update({"env": env_dict, "dbt_bin": "synq-dbt"})


##
# Dbt reporting to synq with synq-dby
# IMPORTANT: Because of a missing feature the SYNQ_TOKEN is not passed via the DbtOperator
#            you need to start the Airflow worker process with the SYNQ_TOKEN environment variable set
##

class DbtCompileOperator(DbtBaseOperator):
    @apply_defaults
    def __init__(self, profiles_dir=None, target=None, *args, **kwargs):
        super(DbtRunOperator, self).__init__(profiles_dir=profiles_dir, target=target, *args, **kwargs)

    def execute(self, context):
        self.create_hook().run_cli('compile')

with DAG(
    dag_id="airflow_dbt_plugin_compile_only",
    default_args=default_args_synq,
    schedule_interval="@daily",
) as dag_synq:

    dbt_compile = DbtCompileOperator(task_id="dbt_compile_synq")

    (dbt_compile)
