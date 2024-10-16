#!/bin/bash


echo "Run Custom Dags"
IFS=$'\n' read -r -d '' -a chg_files < <( git diff --name-only $COMMIT_ID && printf '\0' )
for i in "${chg_files[@]}"; do
  echo $i | grep -E "dags/config|dags/sql|dags/*.py" | awk -F / '{print $3}' | sort | uniq | while IFS= read -r dir; do
			echo "Dag config directory name: $dir"
			dag_name=$(grep -m1 'name:' dags/config/$dir/*.yaml | awk '{ print $2}')
			echo "Dag Name : $dag_name"
			#echo "Execute run_dag python script  python3 tests/integration/run_dag.py  --dag_name $dag_name"
      #python3 tests/integration/run_dag.py  --dag_name $dag_name
	done
done
