#!/bin/bash

git diff --name-only $COMMIT_ID >> dag_out
cat dag_out
grep -E "dags/config|dags/sql" dag_out | awk -F / '{print $3}' | sort | uniq | while IFS= read -r dir; do
  echo "Dag config directory name: $dir"
  dag_name=$(grep -m1 'name:' config/$dir/*.yaml | awk '{ print $2}')
  echo "Dag Name : $dag_name"
  echo "Execute run_dag puthon script  python3 tests/integration/run_dag.py  --dag_name $dag_name"
  python3 tests/integration/run_dag.py  --dag_name $dag_name
done
