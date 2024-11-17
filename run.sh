#!/bin/bash

# chmod +x run.sh

# Definir o PYTHONPATH
export PYTHONPATH=app

# Executar o script Python
python3 app/src/local_main.py \
    --JOB_NAME 'LocalJob' \
    --JOB_ENVIRONMENT local \
    --PARAM_BASES_TO_PROCESS 'CNPJ9,CNPJ14,CARTEIRA,CONTA' \
    --INPUT_MESH_DB_TABLE 'data//input//cliente.csv'\
    --INPUT_S3_PATH_CACHE_CNPJ9 'data//input//cache_cnpj9.csv' \
    --INPUT_S3_PATH_CACHE_CNPJ14 'data//input//cache_cnpj14.csv' \
    --INPUT_S3_PATH_CACHE_CARTEIRA 'data//input//cache_carteira.csv' \
    --INPUT_S3_PATH_CACHE_CONTA 'data//input//cache_conta.csv' \
    --OUTPUT_S3_PATH_DELTA_CNPJ9 'data//output//delta_cnpj9' \
    --OUTPUT_S3_PATH_DELTA_CNPJ14 'data//output//delta_cnpj14' \
    --OUTPUT_S3_PATH_DELTA_CARTEIRA 'data//output//delta_carteira' \
    --OUTPUT_S3_PATH_DELTA_CONTA 'data//output//delta_conta' 