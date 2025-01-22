# DS3X

## Visão Geral

Este projeto automatiza o processo de download de dados de URLs específicas, renomeia os arquivos baixados e faz o upload dos dados para o Google BigQuery. Ele usa Selenium para web scraping, Pandas para manipulação de dados e Google BigQuery para armazenamento de dados.

## Requisitos

- Python 3.12.3
- Google Chrome
- ChromeDriver
- Conta de Serviço do Google Cloud com permissões para BigQuery

## Instalação

1. **Clone o repositório:**
   ```sh
   git clone https://github.com/anaflxvia/ds3x_.git
   cd ds3x_
   ```

2. **Crie um ambiente virtual e ative-o:**
   ```sh
   python -m venv .venv
   .venv\Scripts\activate  # No Windows
   ```

3. **Instale os pacotes necessários:**
   ```sh
   pip install -r requirements.txt
   ```

4. **Baixe o ChromeDriver:**
   - Baixe o ChromeDriver [aqui](https://sites.google.com/a/chromium.org/chromedriver/downloads).
   - Extraia e coloque-o no diretório do projeto.

5. **Configure as credenciais do Google Cloud:**
   - Crie uma conta de serviço no Google Cloud e baixe o arquivo de chave JSON.
   - Coloque o arquivo de chave JSON no diretório do projeto e nomeie-o como credentials.json.

## Uso

1. **Atualize o arquivo main.py:**
   - Certifique-se de que os caminhos para chromedriver_path e credentials_path estão corretos.
   - Atualize o project_id e dataset_id com os detalhes do seu projeto no Google Cloud.
   - Atualize o dicionário urls com as URLs das quais você deseja baixar dados.

2. **Execute o script:**
   ```sh
   python src/main.py
   ```

## Como Funciona

1. **Download de Dados:**
   - A classe 

DataDownloader

 usa Selenium para navegar até URLs especificadas e baixar arquivos de dados.

2. **Renomear Arquivos:**
   - A função rename_files renomeia os arquivos baixados com base em padrões predefinidos.

3. **Upload para BigQuery:**
   - A classe BigQueryUploader faz o upload dos dados dos arquivos renomeados para o Google BigQuery.

## Logging

- O script registra seu progresso e quaisquer erros encontrados. Os logs são impressos no console.

## Solução de Problemas

- Certifique-se de que o ChromeDriver é compatível com a versão instalada do Google Chrome.
- Verifique se a conta de serviço do Google Cloud tem as permissões necessárias para o BigQuery.
- Verifique os logs para mensagens de erro detalhadas.



# Processamento de Dados com Google BigQuery

Este repositório contém um Jupyter notebook para consultar e processar dados do Google BigQuery usando Python. O notebook demonstra como autenticar com o Google Cloud, executar consultas SQL e manipular dados usando pandas.

## Conteúdo do Notebook

### 1. Configuração e Autenticação
A primeira célula importa as bibliotecas necessárias e configura a autenticação usando um arquivo JSON de conta de serviço.
```python
from pandas_gbq import read_gbq
from google.oauth2 import service_account
import pandas as pd
from pathlib import Path

download_dir = Path().parent.resolve()
credentials_path = f"{str(download_dir)}/credentials.json"

credentials = service_account.Credentials.from_service_account_file(credentials_path)
```

### 2. Consultar Dados do BigQuery
A segunda célula executa uma consulta SQL para recuperar dados de uma tabela do BigQuery e exibe os resultados.
```python
query = """
SELECT *
FROM `ps-eng-dados-ds3x.anaflavia_199.icf_icc_refined`
LIMIT 12
"""

project_id = 'ps-eng-dados-ds3x'

df = read_gbq(query, project_id=project_id, dialect='standard', credentials=credentials)
df
```

### 3. Criar Tabelas Confiáveis
A terceira e quarta células criam novas tabelas no BigQuery executando consultas SQL para selecionar registros distintos das tabelas brutas.
```python
query = """
CREATE OR REPLACE TABLE `ps-eng-dados-ds3x.anaflavia_199.icc_trusted` AS
SELECT DISTINCT *
FROM `ps-eng-dados-ds3x.anaflavia_199.icc_raw`
"""

# Execute a consulta
from google.cloud import bigquery
client = bigquery.Client(credentials=credentials, project=credentials.project_id)
query_job = client.query(query)
query_job.result()

print("Tabela 'icc_trusted' criada com sucesso no BigQuery!")
```

### 4. Verificar Colunas da Tabela
A quinta e sexta células verificam as colunas das tabelas confiáveis recém-criadas.
```python
# Verifique as colunas da tabela
table_ref = client.dataset('anaflavia_199').table('icc_trusted')
schema = client.get_table(table_ref).schema

# Listar os nomes das colunas
columns = [field.name for field in schema]
print(columns)
```

### 5. Criar Tabela Refinada
A sétima célula cria uma tabela refinada combinando dados das tabelas confiáveis e calculando variações percentuais.
```python
query = """
CREATE OR REPLACE TABLE `ps-eng-dados-ds3x.anaflavia_199.icf_icc_refined` AS
WITH icc_data AS (
    SELECT 
        icc.`ÍNDICES E SEGMENTAÇÕES`,
        ROUND(((icc.`dez-24_nov-24` - icc.`dez-24_dez-23`) / icc.`dez-24_dez-23`) * 100, 1) AS variacao_percentual,
        CURRENT_TIMESTAMP() AS load_timestamp
    FROM 
        `ps-eng-dados-ds3x.anaflavia_199.icc_trusted` icc
    WHERE 
        icc.`ÍNDICES E SEGMENTAÇÕES` IN ('Índice de Confiança do Consumidor', 'Índice das Condições Econômicas Atuais', 'Índice de Expectativas do Consumidor')
),
icf_data AS (
    SELECT 
        icf.`ÍNDICES E SEGMENTAÇÕES`,
        ROUND(((icf.`dez-24_nov-24` - icf.`dez-24_dez-23`) / icf.`dez-24_dez-23`) * 100, 1) AS variacao_percentual,
        CURRENT_TIMESTAMP() AS load_timestamp
    FROM 
        `ps-eng-dados-ds3x.anaflavia_199.icf_trusted` icf
    WHERE 
        icf.`ÍNDICES E SEGMENTAÇÕES` = 'ICF'
)

SELECT * FROM icc_data
UNION ALL
SELECT * FROM icf_data
"""

project_id = 'ps-eng-dados-ds3x'

df = read_gbq(query, project_id=project_id, dialect='standard', credentials=credentials)
df
```

## Requisitos
- Python 3.12.3
- pandas
- pandas_gbq
- google-cloud-bigquery
- google-auth

## Configuração
1. Clone o repositório.
2. Instale os pacotes Python necessários.
3. Coloque seu arquivo JSON de conta de serviço do Google Cloud no diretório pai do notebook.
4. Execute o Jupyter notebook.
