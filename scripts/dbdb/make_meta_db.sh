#!/bin/bash

DB_DIR=/data_pool_1/DrivingBehaviorDatabase
RECORDS_DIR=${DB_DIR}/records
DB_NAME=nudt.db
DB_FILE=${DB_DIR}/${DB_NAME}

PRJ_PATH=../..
meta_db_script=${PRJ_PATH}/nudt/builder/meta_db.py


echo Building meta DB...
python3 ${meta_db_script} \
    --target_dir ${RECORDS_DIR} \
    --output_db_host ${DB_FILE}

echo Finished building index DB. \-\> ${DB_FILE}
