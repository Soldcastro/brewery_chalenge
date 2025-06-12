# brewery_chalenge

## Objetivo

O repositório **brewery_chalenge** tem como objetivo central a implementação de um pipeline de dados para ingestão, processamento e análise de informações sobre cervejarias, utilizando dados públicos da Open Brewery DB. O projeto segue uma arquitetura de data lake em camadas (bronze, silver, gold) para organizar os dados e permitir análises mais avançadas.

## Tecnologias Empregadas

- **Python**: Linguagem principal utilizada para codificação dos scripts de ingestão e transformação dos dados.
- **Apache Spark**: Usado para processamento distribuído dos dados nas etapas silver e gold, facilitando a manipulação de grandes volumes de dados.
- **Apache Airflow**: Utilizado para orquestração dos workflows de dados (conforme evidenciado no Dockerfile da pasta `airflow`).
- **MinIO**: Serviço local de Object Store que utiliza o protocolo do AWS S3.
- **Docker**: Containerização do ambiente de orquestração com Airflow.
- **Requests (Python)**: Biblioteca utilizada para realizar requisições HTTP à API da Open Brewery DB.
- **Parquet**: Formato de arquivo utilizado para armazenamento eficiente dos dados processados, principalmente nas camadas silver e gold do data lake.
- **Pytest**: Framework utilizado para testes dos scripts de processamento de dados.

## Estrutura do Pipeline

1. **Ingestão (Bronze Layer):**
   - Coleta os dados brutos da API pública Open Brewery DB utilizando a biblioteca `requests`.
   - Os dados são salvos em arquivos JSON na camada bronze do data lake.

2. **Processamento Silver:**
   - Utiliza Apache Spark para ler os dados brutos (JSON), realizar limpeza, transformação e seleção das colunas relevantes.
   - Os dados processados são salvos em formato Parquet, organizados por país e tipo de cervejaria.

3. **Agregação Gold:**
   - Novamente com Spark, são realizadas agregações e análises mais complexas sobre os dados transformados.
   - O resultado é armazenado na camada gold do data lake, pronto para consumo analítico.

4. **Orquestração:**
   - O Airflow é utilizado para automatizar e agendar as etapas do pipeline, garantindo a execução ordenada e monitorada dos processos.

## Organização do Repositório

- `notebooks/`: Contém notebooks Jupyter para exploração, transformação e análise dos dados nas diferentes camadas do pipeline.
- `spark/scripts/`: Scripts Python para processamento automatizado dos dados.
- `spark/tests/`: Testes automatizados para validação das funções de processamento utilizando Pytest.
- `spark/requirements.txt`: Dependências Python necessárias para execução dos scripts Spark.
- `airflow/`: Dockerfile e configurações para ambiente de orquestração com Apache Airflow.
- `minio/`: Script bash de inicialização do MinIO e policy com as permissões de acesso ao bucket.

## Resumo

O **brewery_chalenge** demonstra a aplicação de boas práticas de engenharia de dados, integrando orquestração de workflows, processamento distribuído e testes automatizados para criar um pipeline robusto de ETL (Extract, Transform, Load) voltado para o domínio de dados de cervejarias.

## Débitos Técnicos
- Configurar um servidor SMTP ou Webhook no Airflow para alertas de falha na execução da DAG etl_brewery.
- Adicionar o serviço Jupyter a imagem docker para facilitar a exploração dos dados e utilização dos notebooks inclusos.
- Desenvolver um módulo de Qualidade de Dados (Possivelmente com a biblioteca Great Expectations).
- Adicionar um serviço de DataViz (Possivelmente Metabase).

## 🛠 Setup
## Clone o projeto

    $ git clone https://github.com/Soldcastro/brewery_chalenge.git

## Build Docker

    $ cd brewery_chalenge
    $ docker-compose build
    $ docker-compose up -d

## Acesse Airflow UI http://localhost:8090/ com login e senha airflow
## SparkUI disponível em http://localhost:8080/
## Spark History Server disponível em http://localhost:18080/
## MinIO Object Store disponível em http://localhost:9001/ com login e senha minioadmin
## Ative a dag etl_brewery e execute.