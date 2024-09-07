# Tratamento da base de CNPJ's da Receita Federal
Olá!

Nesse projeto proponho uma solução de engenharia de dados realizando o processamento da base de CNPJ's das empresas do Brasil disponibilizada pela [Receita Federal](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj) para atender a demanda de um usuário final que deseje avaliar alguns aspectos referentes à esses dados.

Nas seções abaixo apresento mais detalhes dos requisitos e as soluções utilizadas.

# Demanda

Para o desenvolvimento do projeto, considere a seguinte lista de demandas que o usuário final elencou que precisaria para suas análises:

1. Trazer os indicadores listados nos itens 2 e 3, sempre sendo possível avaliar por: cidade, estado, natureza jurídica, CNAE, situação cadastral e porte da empresa.

2. Alterações dos status das empresas por período:
    - Quantas empresas estão ativas
    - Quantas empresas estão suspensas
    - Quantas empresas estão inaptas
    - Quantas empresas estão baixadas

3. Quantas empresas são abertas por período?
4. Qual o movimento de migração de empresas entre cidades/estado, ou seja, de qual cidade/estado as empresas saem e para qual cidade/estado se mudam?

A partir desses requisitos, segui para a criação da arquitetura do projeto.

# Arquitetura

A arquitetura proposta foi baseada nas ferramentas disponíveis na [Azure](https://azure.microsoft.com/).

Em uma visão geral, a fonte de dados é consumida utilizando o [Azure Data Factory](https://azure.microsoft.com/pt-br/products/data-factory) em um *pipeline* que copia os arquivos para Azure Data Lake Storage e faz a descompactação deles no container *landing*.

O mesmo *pipeline* do Azure Data Factory descompacta esses arquivos os salva em formato *parquet* na camada *bronze*, mantendo os mesmos dados, só alterando o formato.

Na fase seguinte, os dados são tratados em *notebooks* do [Databricks](https://www.databricks.com/) para serem salvos como tabelas na camada *silver* utilizando particionamento para melhor performance.

Com base nas tabelas silver é possível criar as agregações e visões que atendem os requisitos solicitados pelo usuário na camada *gold*, também utilizando *notebooks* no Databricks.

Para realizar a orquestração dos *notebooks* do Databricks, foi criado um pipelines no Azure Data Factory.

Após a finalização, é criado um painel no [Power BI](https://www.microsoft.com/pt-br/power-platform/products/power-bi) que conecta na camada *gold* do Data Lake para apresentar os dados ao usuário.


![image](https://github.com/user-attachments/assets/6e8f9267-5212-40ff-81a1-e862e8ecd734)


# Fontes

As fontes de dados desse projeto está disponível no [Portal de Dados Abertos do Governo Federal](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj).
Os arquivos são disponibilizados mensalmente no [link](https://dadosabertos.rfb.gov.br/CNPJ/dados_abertos_cnpj/) em formato *csv* compactados em arquivos *zip* .
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
- *bronze*: armazena os arquivos *csv* que foram transformados em *parquet* depois da descompactação. A divisão de pastas mantem a mesma da origem (referência).
- *silver*: armazena os dados das tabelas gerenciadas da camada *silver* com as limpezas dos dados necessárias para tender a demanda.
- *gold*: armazena os dados das tabelas gerenciadas da camada *gold* com as agregações necessárias para tender a demanda.


# Ingestão

Para realizar a ingestão foi utilizado o Azure Data Factory e projeto pode ser visualizado nesse [repositório](https://github.com/laismeuchi/dados-adf-base-cnpj).

O primeiro *pipeline* atribui a uma variável o nome da pasta que deve ser lida na fonte, em seguida faz a leitura de um arquivo de metadados onde estão elencados os arquivos disponíveis no site do governo e indica quais devem ser lidos (status = active).

A atividade de filtro remove os arquivos que não devem ser lidos. Essa configuração facilita os testes pois permite deixar só os arquivos desejados para consulta.

![image](https://github.com/user-attachments/assets/af1adb83-9d5c-4a11-9eb1-8bad0bcd8361)

Em seguida, para cada arquivo identificado é feita a ingestão dos dados da fonte para o container *landing* e na atividade seguinte é feita a descompactação dos arquivos *zip*, transforma em *parquet* e salva no container *bronze*.

![pipe_copy_files](https://github.com/user-attachments/assets/f24c2c42-eadc-43da-be2d-e3414368ad72)


# Transformação

As transformações foram feitas em *noteboobks* no Databricks e que foram colocados em um segundo *pipeline* no Azure Data Factory para ser realizada a orquestração da execução.
Os *notebooks* foram separados em 2 etapas:
1. *bronze_to_silver*: nessa etapa os dados são transportados da camada *bronze* para *silver* realizando as limpezas necessárias e adição de informações de controle. Também foram criadas algumas tabelas que tem valores fixos (porte_empresa, situacao_cadastral, tipo_mudanca). O código dessa etapa pode ver consultado [aqui](https://github.com/laismeuchi/dados-databricks-base-cnpj/tree/main/bronze_to_silver).
2. *gold*: nessa etapa os dados são transportados da camada *silver* para *gold* realizando as agregações necessárias para atender as demandas do usuário final. Também foram criadas algumas dimensões que serão utilizadas para compor o modelo *star schema* a ser utilizado no Power BI. O código dessa etapa pode ver consultado [aqui](https://github.com/laismeuchi/dados-databricks-base-cnpj/tree/main/gold)

Em ambas as etapas, para cada entidade foi criada uma tabela gerenciada no formato *Delta* no Databricks.

O segundo *pipeline* do projeto, faz a orquestração dos *notebooks* do Databricks, assim eles executam na ordem correta.

![pipe_orchestrate_notebooks](https://github.com/user-attachments/assets/1bee8df4-4d78-4c1f-8aa7-31d66d04ba8e)

Posteriormente, pode ser adicionado uma nova lógica a esse projeto que faz a consulta das pastas disponíveis no site do governo, verifica quais ainda não foram importadas para o Data Lake e faz a importação automaticamente conforme agendamento. Assim não será necessário a passagem de parâmetro manual da referencia para execução.


# Visualização

# Análise dos Dados

# Custo
