#!/usr/bin/env bash

trap ctrl_c INT

function ctrl_c() {
    echo "Stopping program... "
    for pid in ${pids[*]}; do
      echo "Killing subprocess $pid"
      kill -9 $pid
    done
    echo "Stop program."
}

ARG_DEFS=(
  "--dataset-id=(.*)"
  "--dataset-version=(.*)"
  "--records=(.*)"
  "--datasets-in=(.*)"
  "--datasets-extensions-in=(.*)"
  "[--num-files=(.*)]"
  "[--show=(true|false)]"
  "[--max-threads=(.*)]"
  "[--init-iteration=(.*)]"
)

function init() {
  export SHOW=${SHOW:-false}
  export NUM_FILES=${NUM_FILES:-1}
  export MAX_THREADS=${MAX_THREADS:-1}
  export INIT_ITERATION=${INIT_ITERATION:-0}
}

function run() {
  local out_dir="/datasets-out/${DATASET_ID}/${DATASET_VERSION}"
  rm -rf $out_dir || true
  mkdir -p $out_dir
  local out_schemas_dir="$out_dir/schemas"
  rm -rf ${out_schemas_dir} || true
  mkdir -p ${out_schemas_dir}

  local schema_orig=$(cat ${DATASETS_IN}/${DATASET_ID}/v${DATASET_VERSION}/${DATASET_ID}_${DATASET_VERSION}.avsc)
  local schema_extension=$(cat ${DATASETS_EXTENSIONS_IN}/${DATASET_ID}/v${DATASET_VERSION}/extensions.json || (true ; echo "{}"))
  local merged_schema_path="${out_schemas_dir}/merged_schema.json"
  python3 lib/merge-schemas.py -s "$schema_orig" -e "$schema_extension" --out "${merged_schema_path}"

  local counter=0
  local current_threads=0
  while [  $counter -lt $NUM_FILES ]; do

    if [ "${SHOW}" = "true" ]; then
      ARGS="-j -p"
    else
      out="${out_dir}/${DATASET_ID}.${counter}.avro"
      touch $out
      ARGS="-b -o ${out}"
    fi

    export ITERATION_STEP="$((${RECORDS} * ${counter} + ${INIT_ITERATION}))"
    envsubst < ${merged_schema_path} > ${out_schemas_dir}/schema_${counter}.json
    avro-generator -f ${out_schemas_dir}/schema_${counter}.json -i ${RECORDS} ${ARGS} &
    pids[$counter]=$!
    echo "Generating ${out} in background, current thread [${pids[current_threads]}]"

    let current_threads=current_threads+1

    if [ "${current_threads}" = "${MAX_THREADS}" ]; then
        for index in "${!pids[@]}"; do
          if [ "${pids[index]}" != "" ]; then
            wait ${pids[index]}
            let current_threads=current_threads-1
            pids[$index]=""
            break
          fi
        done
    fi

    let counter=counter+1

  done
}

source $(dirname $0)/base.inc
