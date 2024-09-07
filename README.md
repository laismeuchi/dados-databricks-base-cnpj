# Tratamento da base de CNPJ's da Receita Federal
Olá!

Nesse projeto proponho uma solução de engenharia de dados realizando o processamento da base de CNPJ's das empresas do Brasil disponibilizada pela [Receita Federal](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj) para atender a demanda de um usuário final que deseje avaliar alguns aspectos referentes à esses dados.

Nas seções abaixo apresento mais detalhes dos requisitos e as soluções utilizadas.

# Demanda

Para o desenvolvimento segui a seguinte lista de demandas que o usuário final elencou que precisaria para suas análises:

1. Trazer os indicadores listados nos itens 2 e 3, sempre sendo possível avaliar por: cidade, estado, natureza juridica, CNAE, situação cadastral e porte da empresa.

2. Alterações dos status das empresas por período:
    - Quantas empresas estão ativas
    - Quantas empresas estão suspensas
    - Quantas empresas estão inaptas
    - Quantas empresas estão baixadas

3. Quantas empresas são abertas por período?
4. Qual o movimento de migração de empresas entre cidades/estado, ou seja, de qual cidade/estado as empresas saem e para qual cidade/estado se mudam?

A partir desses requisitos, segui para a ciação da arquitetura do projeto.

# Arquitetura

A arquitetura proposta foi baseda nas ferramentas disponíveis na [Azure](https://azure.microsoft.com/).

Em uma visão geral, a fonte de dados é consumida utilizando o Azure Data Factory em um *pipeline* que copia os arquivos para Azure Data Lake Storage e faz a descompactação deles no container *landing*.

O mesmo *pipeline* do Azure Data Factory descompacta esses arquivos os salva em formato *parquet* na camada *bronze*, mantendo os mesmos dados, só alterando o formato.

Na fase seguinte, os dados são tratatos em *notebooks* do Databricks para serem salvos como tabelas na camada *silver* utilizando particionamento para melhor performance.

Com base nas tabelas silver é possível criar as agregações e visões que atendem os requisitos solicitados pelo usuário na camada *gold*, também utilizando *notebooks* no Databricks.

Para realizar a orquestração dos *notebooks* do Databricks, foi criado um pipelines no Azure Data Factory.

Após a finalização, é criado um painel no Power BI que conecta na camada *gold* do Data Lake para apresentar os dados ao usuário.


![image](https://github.com/user-attachments/assets/6e8f9267-5212-40ff-81a1-e862e8ecd734)


# Fontes

As fontes de dados desse projeto está disponível no [Portal de Dados Abertos do Governo Federal](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj).
Os arquivos são disponibilizados mensalmente no [link](https://dadosabertos.rfb.gov.br/CNPJ/dados_abertos_cnpj/) em fromato *csv* compactados em arquivos *zip* .
Nesse projeto utilizei os seguintes arquivos:

| Arquivos          |
| -------------     |
| Municipios        |
| Cnaes             |
| Naturezas         |
| Empresas          |
| Estabelecimentos  |

Os arquivos de Empresas e Estabelecimentos são particionados em vários arquivos do mesmo tamanho, tendo que ser tratados para serem carregados na camada *bronze* na mesma tabela.

# Armazenamento

O armazenamento do projeto usa o Data Lake Storage da Azure com os seguintes containers:
- *configs*: armazena arquivos de configuração para auxiliar nas cargas.
- *landing*: armazena os arquivos *zip* copiados do site do governo. A divisão de pastas mantem a mesma da origem (referência).
- *bronze*: armazena os arquivos *csv* que foram transformados em *parquet* depois da descompactação.A divisão de pastas mantem a mesma da origem (referência).
- *silver*: armazena os dados das tabelas gerenciadas da camada *silver* com as limpezas dos dados necessárias para tender a demanda.
- *gold*: armazena os dados das tabelas gerenciadas da camada *gold* com as agregações necessárias para tender a demanda.


# Ingestão

Para realizar a ingestão foi utilizado o Azure Data Factory e projeto pode ser visualizado nesse [repositório](https://github.com/laismeuchi/dados-adf-base-cnpj).

O primeiro *pipeline* faz a ingestão da fonte para o container *landind*  com as seguintes atividades:
1. *set_variable_p_folder*: atividade do tipo *set variable* que atribui o nome da pasta que deve ser consultada no site do governo.
2. *read_files_metadata*: atividade do tipo *lookup* que faz a leitura de um arquivo *xlsx* que indica quais arquivos devem ser lidos da pasta no site do governo.
3. *filter_actives*: atividade do tipo *filter* que filtra somente os arquivos que estiverem indicados com status *active*. Assim eu podia fazer testes facilmente, inativando os arquivos que não queria processar naquele momento.
4. *ForEach1*: atividade do tipo *for each* que itera sobre os nomes dos arquivos a serem lidos, executando as seguintes atividades para cada arquivo:
    - *copy_http_to_landing*: atividade do tipo *copy data* que copia o arquivo do site do governo para o conateiner *landing* no Azure Data Lake Storage.
    - *unzip_files*: atividade do tipo *copy data* que copia o arquivo *zip*, descompacta ele e salva como *parquet* no container *bronze* no Azure Data Lake Storage.

# Transformação

As transformações foram feitas em *noteboobks* no Databricks e que foram colocados em um segundo *pipeline* no Azure Data Factory para ser realizada a orquestração da execução.

O segundo *pipeline* utilizada as seguintes atividades: 
1. *set_variable_p_folder*: atividade do tipo *set variable* que indica o parâmetro "referencia" que deve ser passado para a execução dos *notebooks*.
2. *Databricks*: atividade do tipo *Databricks* que permite a execução de *notebooks* que estão no *workspace* do Databricks. Foram criadas as seguintes atividades:
    - *bronze_to_silver*: executa o *notebook* [_executar_todos](https://github.com/laismeuchi/dados-databricks-base-cnpj/blob/main/bronze_to_silver/_executar_todos.py) que recebe a referência como parâmtero e executa os demais que copiam os dados da *bronze* para a *silver*.
    - *gold_dimensoes_fixas*: executa o *notebook* [dimensoes_fixas](https://github.com/laismeuchi/dados-databricks-base-cnpj/blob/main/gold/dimensoes_fixas.py) que carrega as informações das dimensões fixas(dim_municipios, dim_natureza_juridica, dim_porte_empresa, dim_situacao_cadastral e dim_tipo_mudanca) da camada *silver* para *gold*.
    - *fato_abertura_empresa*: executa o *notebook* [fato_abertura_empresa](https://github.com/laismeuchi/dados-databricks-base-cnpj/blob/main/gold/fato_abertura_empresa.py) que agrupa os dados das tabelas relacionadas para registrar as empresas abertas salvando os registros na camada *gold*.
    - *fato_abertura_mudanca_endereco*: executa o *notebook* [fato_abertura_mudanca_endereco](https://github.com/laismeuchi/dados-databricks-base-cnpj/blob/main/gold/fato_mudanca_endereco.py) que agrupa os dados das tabelas relacionadas para registrar as mudanças de endereço das empresas salvando os registros na camada *gold*.
    - *fato_situacao_cadastral*: executa o *notebook* [fato_situacao_cadastral](https://github.com/laismeuchi/dados-databricks-base-cnpj/blob/main/gold/fato_situacao_cadastral.py) que agrupa os dados das tabelas relacionadas para registrar as mudanças de situação cadastral das empresas salvando os registros na camada *gold*.
    
Posteriomente, pode ser adicionado uma nova lógica a esse projeto que faz a consulta das pastas disponíveis no site do governo, verifica quais ainda não foram importadas para o Data Lake e faz a importação automaticamente conforme agendamento. Assim não será necessário a passagem de parâmetro manual da referencia para execução.


# Visualização

# Análise dos Dados

# Custo
