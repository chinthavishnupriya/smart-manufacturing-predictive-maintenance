#!/usr/bin/env bash
set -euo pipefail

command -v sqoop >/dev/null 2>&1 || { echo "ERROR: sqoop command not found." >&2; exit 1; }
command -v hdfs >/dev/null 2>&1 || { echo "ERROR: hdfs command not found." >&2; exit 1; }

MYSQL_JDBC_URL="${MYSQL_JDBC_URL:-jdbc:mysql://localhost:3306/smart_manufacturing}"
MYSQL_USER="${MYSQL_USER:-sqoop_user}"
SQOOP_PASSWORD_FILE="${SQOOP_PASSWORD_FILE:-}"
HDFS_BASE_PATH="${HDFS_BASE_PATH:-/user/$(id -un)/smart-manufacturing/sqoop-imports/$(date +%Y%m%d_%H%M%S)}"

[[ -n "$SQOOP_PASSWORD_FILE" ]] || {
  echo "ERROR: Set SQOOP_PASSWORD_FILE to an HDFS path readable by Sqoop." >&2
  exit 1
}
hdfs dfs -test -e "$SQOOP_PASSWORD_FILE" || {
  echo "ERROR: Sqoop password file not found in HDFS: $SQOOP_PASSWORD_FILE" >&2
  exit 1
}

for table in machine_master maintenance_history work_orders; do
  target="$HDFS_BASE_PATH/$table"
  if hdfs dfs -test -e "$target"; then
    echo "ERROR: Target already exists; refusing to overwrite: $target" >&2
    exit 1
  fi
  echo "Importing MySQL table $table -> $target"
  sqoop import \
    --connect "$MYSQL_JDBC_URL" \
    --username "$MYSQL_USER" \
    --password-file "$SQOOP_PASSWORD_FILE" \
    --table "$table" \
    --target-dir "$target" \
    --num-mappers 1 \
    --as-textfile \
    --fields-terminated-by '\\t'
  hdfs dfs -test -e "$target/part-m-00000" || {
    echo "ERROR: Sqoop output file missing for table $table at $target" >&2
    exit 1
  }
done

echo "Sqoop imports completed. HDFS base path: $HDFS_BASE_PATH"
hdfs dfs -ls "$HDFS_BASE_PATH"
