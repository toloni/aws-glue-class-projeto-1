#!/bin/bash

# Definir o PYTHONPATH
export PYTHONPATH=app

# Verificar se o parâmetro de entrada é 'coverage'
if [ "$1" == "cov" ]; then
    echo "Executando os testes com cobertura..."
    
    # Executar o script de teste com pytest e coverage
    coverage run -m pytest app/tests/
    
    # Gerar o relatório HTML de cobertura
    coverage html
    echo "Relatório HTML gerado na pasta htmlcov"
else
    echo "Executando os testes sem cobertura..."
    
    # Executar os testes sem cobertura
    pytest app/tests/test_transform_sb_carteira.py

fi