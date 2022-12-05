#!/usr/bin/env bash

set -e

##
helm repo add airflow-stable https://airflow-helm.github.io/charts

## set the release-name & namespace
export AIRFLOW_NAME="airflow"
export AIRFLOW_NAMESPACE="airflow"

## create the namespace

## install using helm 3
helm upgrade --install \
  "$AIRFLOW_NAME" \
  airflow-stable/airflow \
  --namespace "$AIRFLOW_NAMESPACE" \
  --version "8.6.1" \
  --values ./values.yml \
  --create-namespace \
  --wait
