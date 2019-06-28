#!/bin/bash -e
# =====================================================================
# Build script running OpenNMS gRPC Server in Docker environment
# =====================================================================

function join { local IFS="$1"; shift; echo "$*"; }

IFS=$'\n'
OPTIONS=("acks=1")
for VAR in $(env)
do
  env_var=$(echo "$VAR" | cut -d= -f1)
  if [[ $env_var =~ ^KAFKA_ ]]; then
    key=$(echo "$env_var" | cut -d_ -f2- | tr '[:upper:]' '[:lower:]' | tr _ .)
    val=${!env_var}
    if [[ $key == "manager."* ]]; then
      echo "[Skipping] '$key'"
    else
      echo "[Configuring] '$key'='$val'"
      OPTIONS+=("$key=$val")
    fi
  fi
done

exec java ${JAVA_OPTS} -jar grpc2kafka.jar -b ${BOOTSTRAP_SERVERS} -l ${MINION_LOCATION} -m ${MINION_ID} -p ${PORT-50051} -M ${MESSAGE_BUFFER_SIZE-0} -t ${TOPIC-OpenNMS.Sink.Telemetry-NXOS} -e $(join , ${OPTIONS[@]})
