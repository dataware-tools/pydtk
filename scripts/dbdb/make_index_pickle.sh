#!/bin/bash

DB_DIR=/data_pool_1/DrivingBehaviorDatabase
RECORDS_DIR=${DB_DIR}/records
TMP_PKL_DIR=${DB_DIR}/index
PKL_NAME=nudt.pkl
PKL_FILE=${DB_DIR}/${PKL_NAME}

PRJ_PATH=../..
df_make_script=${PRJ_PATH}/nudt/builder/df_list.py
concat_script=${PRJ_PATH}/nudt/builder/concat_df.py
path_fix_script=${PRJ_PATH}/nudt/builder/df_path.py


echo Step1: Making pickle files...
starts=`echo {10..400..10}`
mkdir -p ${TMP_PKL_DIR}
pkl_list=()
for start in ${starts};do
    end=`echo $((${start} + 9))`
    tmp_pkl_file=${TMP_PKL_DIR}/${PKL_NAME::-4}_$(printf "%03d" ${start})_$(printf "%03d" ${end}).pkl
    python3 ${df_make_script} \
        --db_dir ${RECORDS_DIR} \
        --pkl ${tmp_pkl_file} \
        --start ${start} \
        --end ${end}
    pkl_list+=( ${tmp_pkl_file} )
done

echo Step2: Concatinating pickle files...
pkl_list=`echo ${pkl_list[@]}`
python3 ${concat_script} \
    ${pkl_list} \
    --out_pkl ${PKL_FILE}

echo Step3: Change absolute path to relative path.
python3 ${path_fix_script} \
    --in-pkl ${PKL_FILE} \
    --out-pkl ${PKL_FILE}

echo Step3: Finish building index file. \-\> ${PKL_FILE}