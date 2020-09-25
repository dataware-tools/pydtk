#!/bin/bash

CMDNAME=`basename $0`
if [ $# -ne 2 ]; then
    echo "Usage: ./$CMDNAME start_HDD_No. end_HDD_No." 1>&2
    exit 1
fi
start_hdd=$1
end_hdd=$2

RECORDS_DIR=/data_pool_1/DrivingBehaviorDatabase/records

PRJ_PATH=../..
SCRIPT=${PRJ_PATH}/nudt/builder/dbdb/file_info.py


for record_id in `\find ${RECORDS_DIR} -mindepth 1 -maxdepth 1 -type d | sort`; do
    data_dir=${record_id}/data
    hdd_id=`echo ${record_id} | awk  -F'[/_]' '{print $7}'`
    expr "${hdd_id}" + 1 >/dev/null 2>&1
    if [ $? -gt 1 ]; then
        hdd_id=400  # W,BシリーズはHDD番号を400として数値比較できるようにする
    fi
    if [ ${hdd_id} -lt ${start_hdd} ] || [ ${hdd_id} -gt ${end_hdd} ]; then
        continue
    fi

    for file in `\find ${data_dir} -mindepth 1 -maxdepth 1 -not -name "*.json" -type f | sort`; do
        echo Loading: ${file}
        json_file=${file}".json"
        python3 ${SCRIPT} \
            ${file} \
            --json_path ${json_file}
        echo Finished: ${json_file} \(Processing HDD${start_hdd}-${end_hdd}\)
    done

done
