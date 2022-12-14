# SYNQ dbt Airflow example

## Prerequisites

To run this example you will need

- [docker](https://docs.docker.com/get-docker/)
- [kubectl](https://kubernetes.io/docs/tasks/tools/) (to manage kubernetes)
- [kind](https://kind.sigs.k8s.io/) (Kubernetes in docker)
- [helm](https://helm.sh/docs/intro/quickstart/) (package manager for Kubernetes)

Make sure that docker works:

```bash
docker version
```

You will need the SYNQ_TOKEN to send dbt output to Synq.io

## Installation

### Create local kubernetes cluster

If you don't have a kind cluster locally, please create it.

```bash
kind create cluster
kubectl cluster-info --context kind-kind
```

### Install Airflow with dbt support

In the next step we will install Airflow to our local Kubernetes cluster.

We will do that with Helm and we use airflow-helm chart for that https://github.com/airflow-helm/charts/tree/main/charts/airflow .

As a part of installation 2 additional python packages are installed:

- `airflow-dbt` that provides the Dbt*Operators
- `dbt-postgres` that provides the `dbt` command and support for Postgres

Before we install it you might want to edit [the Helm values.yml file](values.yml). The installations is configured in a way that it will periodically pull this repository with git from https://github.com/getsynq/synq-dbt-airflow.git and add it to the `dags` folder.


```bash
# Add the airflow helm repository
helm repo add airflow-stable https://airflow-helm.github.io/charts
# Install/upgrade Airflow with helm using the values from values.yml
helm upgrade --install \
  "$AIRFLOW_NAME" \
  airflow-stable/airflow \
  --namespace "airflow-dbt" \
  --version "8.6.1" \
  --values ./values.yml \
  --create-namespace \
  --wait
```
This can take a while (5 minutes).

## Synq-dbt and DbtOperatorPlugin DAGs

### Get the Synq dbt wrapper binary

To integrate with synq, you have to install the dbt wrapper program from synq. https://github.com/getsynq/synq-dbt

For production we recommend you add the `synq-dbt` binary to the Airflow image with other dependencies that you need to run your DAGs. In [the basic DbtPlugin example](airflow_dbt_plugin_dags.py) we use [an Airflow image that has synq-dbt preinstalled](Dockerfile.airflow).

In [the advanced DbtPlugin example](airflow_dbt_plugin_advanced_dags.py) we have created a DAG that installs the synq dbt on to the worker, but it will not persist if the worker is restarted. Installation is triggered every DAG start.

### Set the Synq token

The Synq dbt wrapper needs `SYNQ_TOKEN` to be set. The airflow dbt plugin is currently not supporting `env` passing via the `Dbt*` operators. So we have to set the `SYNQ_TOKEN` in 2 places:

Firstly, set the variable `SYNQ_TOKEN` in Airflow. In the top navbar go to **Admin -> Variables** and add a new variable:


![Synq variable token](doc/img/synq_variable.jpg)

Secondly, set the token as environment variable for the pods. This will only be needed until the dbt airflow plugin releases a new version. Currently the DbtOperator is not passing trough the environment variables like `SYNQ_TOKEN`

Edit [the Helm values.yml file](values.yml) and update the token. Then upgrade the airflow release.

```bash
# Upgrade Airflow with helm using the values from values.yml
helm upgrade --install \
  "$AIRFLOW_NAME" \
  airflow-stable/airflow \
  --namespace "airflow-dbt" \
  --version "8.6.1" \
  --values ./values.yml \
  --create-namespace \
  --wait
```

## Synq-dbt and KubernetesOperator DAGs

If you want to use `synq-dbt` with Kubernetes operator you have to add 2 things
to the image/container that will be used in the kubernetes Job/Pod

- `synq-dbt`
- the dbt project

You have to pass the `SYNQ_TOKEN` to the Kubernetes Operator.

In [the basic Kubernetes example](kubernetes_basic_dags.py) we use [a docker image that has the `synq-dbt` and dbt project in the image itself](Dockerfile.dbt).

In [the advanced example](kubernetes_advanced_dags.py) we install `synq-dbt` and we `git clone` our dbt project in seperate Kubernetes init containers.


## How to use

### Connect to Airflow instance

To connect to Airflow in Kubernetes we have to port forward.

```bash
kubectl -n airflow port-forward service/airflow-web 8080
```

Open your browser http://localhost:8080

### Run the DAGs

The dbt project has 2 simple models that will create one table and one view in the `airflow` database/`dbt_example` schema.

Click on the trigger DAG (red `1` circle)

![Airflow home view](doc/img/home.jpg)

After a few seconds the DAG should complete successful.

![Dag run](doc/img/dag_runs.jpg)


### Connect to Postgresql

`kubectl -n airflow-dbt port-forward service/airflow-postgresql 5432`

You can now use your database client to inspect the database.


![Database](doc/img/psql.jpg)

## Deletion

If you want to delete the whole setup, you just need to delete the kind cluster with:
`kind delete cluster`
