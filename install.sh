#!/bin/bash

## set the release-name & namespace
export AIRFLOW_NAME="airflow"
export AIRFLOW_NAMESPACE="airflow"

## create the namespace
kubectl create ns "$AIRFLOW_NAMESPACE"

## install using helm 3
helm upgrade --install \
  "$AIRFLOW_NAME" \
  airflow-stable/airflow \
  --namespace "$AIRFLOW_NAMESPACE" \
  --version "8.6.1" \
  --values ./values.yml