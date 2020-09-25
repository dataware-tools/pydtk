#!/bin/bash

CMDNAME=`basename $0`
if [ $# -ne 1 ]; then
    echo "Usage: ./$CMDNAME <contetnt name>" 1>&2
    echo "e.g.: ./$CMDNAME /vehicle/acceleration"
    exit 1
fi
q_content=$1
span=60.0

DB_DIR=/data_pool_1/DrivingBehaviorDatabase
RECORDS_DIR=${DB_DIR}/records
DB_NAME=nudt.db
DB_FILE=${DB_DIR}/${DB_NAME}


echo "Adding statistic tables to ${DB_FILE} ..."
create_stat_db \
    ${DB_FILE} \
    ${RECORDS_DIR} \
    ${q_content} \
    --span ${span} \
    --stat_db ${DB_FILE} \
    --overwrite

echo "Finished adding statistic table. -> ${DB_FILE}"
