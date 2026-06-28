#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR_DIR="${SCRIPT_DIR}/.jars"
PAIMON_SPARK_JAR="paimon-spark-4.0_2.13-1.4.1.jar"
PAIMON_S3_JAR="paimon-s3-1.4.1.jar"
ICEBERG_SPARK_JAR="iceberg-spark-runtime-4.0_2.13-1.11.0.jar"
ICEBERG_AWS_JAR="iceberg-aws-bundle-1.11.0.jar"
DELTA_SPARK_JAR="delta-spark_2.13-4.0.0.jar"
DELTA_STORAGE_JAR="delta-storage-4.0.0.jar"
ANTLR_RUNTIME_JAR="antlr4-runtime-4.13.1.jar"
HADOOP_AWS_JAR="hadoop-aws-3.4.1.jar"
AWS_SDK_BUNDLE_JAR="bundle-2.24.6.jar"
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

download_if_missing \
  "${DELTA_SPARK_JAR}" \
  "${MAVEN_BASE}/io/delta/delta-spark_2.13/4.0.0/${DELTA_SPARK_JAR}"

download_if_missing \
  "${DELTA_STORAGE_JAR}" \
  "${MAVEN_BASE}/io/delta/delta-storage/4.0.0/${DELTA_STORAGE_JAR}"

download_if_missing \
  "${ANTLR_RUNTIME_JAR}" \
  "${MAVEN_BASE}/org/antlr/antlr4-runtime/4.13.1/${ANTLR_RUNTIME_JAR}"

download_if_missing \
  "${HADOOP_AWS_JAR}" \
  "${MAVEN_BASE}/org/apache/hadoop/hadoop-aws/3.4.1/${HADOOP_AWS_JAR}"

download_if_missing \
  "${AWS_SDK_BUNDLE_JAR}" \
  "${MAVEN_BASE}/software/amazon/awssdk/bundle/2.24.6/${AWS_SDK_BUNDLE_JAR}"
