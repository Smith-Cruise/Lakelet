#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR_DIR="${SCRIPT_DIR}/.jars"
PAIMON_SPARK_JAR="paimon-spark-4.0_2.13-1.4.1.jar"
PAIMON_S3_JAR="paimon-s3-1.4.1.jar"
ICEBERG_SPARK_JAR="iceberg-spark-runtime-4.0_2.13-1.11.0.jar"
ICEBERG_AWS_JAR="iceberg-aws-bundle-1.11.0.jar"
MAVEN_BASE="https://repo.maven.apache.org/maven2"

mkdir -p "${JAR_DIR}"

download_if_missing() {
  local name="$1"
  local url="$2"
  local target="${JAR_DIR}/${name}"

  if [[ -f "${target}" ]]; then
    return
  fi

  curl -fsSL --retry 3 --retry-delay 2 -o "${target}" "${url}"
}

download_if_missing \
  "${PAIMON_SPARK_JAR}" \
  "${MAVEN_BASE}/org/apache/paimon/paimon-spark-4.0_2.13/1.4.1/${PAIMON_SPARK_JAR}"

download_if_missing \
  "${PAIMON_S3_JAR}" \
  "${MAVEN_BASE}/org/apache/paimon/paimon-s3/1.4.1/${PAIMON_S3_JAR}"

download_if_missing \
  "${ICEBERG_SPARK_JAR}" \
  "${MAVEN_BASE}/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.11.0/${ICEBERG_SPARK_JAR}"

download_if_missing \
  "${ICEBERG_AWS_JAR}" \
  "${MAVEN_BASE}/org/apache/iceberg/iceberg-aws-bundle/1.11.0/${ICEBERG_AWS_JAR}"
