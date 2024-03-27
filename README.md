README: Engenharia de Dados - Desafio Geolinkage

# TODOS OS RESULTADOS SÃO REFERENTES A UMA BASE DE DADOS MENOR PARA PODER SUBIR O PROJETO PARA O GITHUB, MAS ASSIM QUE O PROJETO FOR EXECUTADO NA SUA MÁQUINA OS RESULTADO GERADOS SERÃO OS MESMOS PEDIDOS NO DESAFIO.

O objetivo do desafio é criar um pipeline de ETL (Extração, Transformação e Carregamento) para manipular dados de endereços, gerar ruídos controlados e calcular a distância entre registros de duas bases de dados. Todas essas tarefas são realizadas de forma autoática pelo Airflow de acordo a periodicidade indicada.

Componentes do Pipeline:

download: Baixa o banco de dados necessário.
processing: Realiza o processamento dos dados baixados para extrair informações relevantes e criar bases de dados 'base_a' e 'base_b'.
distance_matrix: Calcula a distância entre os registros das bases 'base_a' e 'base_b' e cria uma matriz de distância.

Fluxo de Tarefas (DAG):

O pipeline é orquestrado por uma DAG com as seguintes tarefas:

download_database: Baixa o banco de dados necessário.
database_extracted: Extrai os dados do banco de dados baixado.
create_base_a: Cria o conjunto de dados base 'base_a'.
create_base_b: Cria o conjunto de dados base 'base_b'.
create_distance_matrix_task: Cria a matriz de distância entre os registros das bases A e B.

As dependências entre as tarefas são definidas de forma que cada tarefa depende da conclusão da anterior.

Dependências e Execução:

Antes de executar o pipeline, certifique-se de ter todas as dependências instaladas, conforme especificado no arquivo requirements.txt.
Para executar o pipeline, utilize um ambiente Apache Airflow configurado com o DAG presente neste repositório.
