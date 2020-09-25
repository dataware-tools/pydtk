#!/bin/bash

DB_DIR=/work5/share/NEDO/distribution/data/records
DB_NAME=nudt.db
DB_FILE=${DB_DIR}/${DB_NAME}

echo Building meta DB...
create_meta_db ${DB_DIR} --output_db_host ${DB_FILE}

echo Finished building index DB. \-\> ${DB_FILE}
