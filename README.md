# Tratamento da base de CNPJ's da Receita Federal
Olá!

Nesse projeto proponho uma solução de engenharia de dados realizando o processamento da base de CNPJ's das empresas do Brasil disponibilizada pela [Receita Federal](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj) para atender a demanda de um usuário final que deseje avaliar alguns aspectos referentes à esses dados.

Nas seções abaixo apresento mais detalhes dos requisitos e as soluções utilizadas.

# Requisitos

Para o desenvolvimento segui a seguinte lista de requisitos que o usuário final elencou que precisaria para suas análises:

1. Trazer os indicadores listados nos itens 2 e 3, sempre sendo possível avaliar por: cidade, estado, natureza juridica, CNAE, status, porte e faixa etária dos sócios.

2. Alterações dos status das empresas por periodo:
    - Quantas empresas foram abertas
    - Quantas empresas foram fechadas
    - Quantas empresas foram supensas

3. Qual o movimento de migração de empresas entre cidades/estado, ou seja, de qual cidade/estado as empresas saem e para qual cidade/estado se mudam?

A partir desses requisitos, segui para a ciação da arquitetura do projeto.

# Arquitetura

A arquitetura proposta foi baseda nas ferramentas disponíveis na [Azure](https://azure.microsoft.com/).

Em uma visão geral, a fonte de dados é consumida utilizando o Azure Data Factory que copia os arquivos para Azure Data Lake Storage e faz a descompactação deles no container *landing*.

O mesmo *pipeline* do Azure Data Factory descompacta esses arquivos os salva em formato *parquet* na camada *bronze*, mantendo os mesmos dados, só alterando o formato.

Na fase seguinte, os dados são tratatos em *notebooks* do Databricks para serem salvos como tabelas na camada *silver* utilizando particionamento para melhor performance.

Com base nas tabelas silver é possível criar as agregações e visões que atendem os requisitos solicitados pelo usuário na camada *gold*, também utilizando *notebooks* no Databricks.

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
| Paises            |
| Sócios            |
| Empresas          |
| Estabelecimentos  |

Os arquivos de Sócios, Empresas e Estabelecimentos são particionados em vários arquivos do mesmo tamanho, tendo que ser tratados para serem carregados na camada *bronze* na mesma tabela.

# Armazenamento

O armazenamento do projeto usa o Data Lake Storage da Azure com os seguintes containers:
- *configs*: armazena arquivos de configuração para auxiliar nas cargas.
- *landing*: armazena os arquivos *zip* copiados do site do governo. A divisão de pastas mantem a mesma da origem (Ano-mes).
- *bronze*: armazena os arquivos *csv* que foram transformados em *parquet* depois da descompactação.A divisão de pastas mantem a mesma da origem (Ano-mes).
- *silver*: armazena os dados das tabelas gerenciadas da camada *silver*.
- *gold*: armazena os dados das tabelas gerenciadas da camada *gold*.


# Ingestão

Para realizar a ingestão dos dados utilizei o Azure Data Factory e projeto pode ser visualizado nesse [repositório](https://github.com/laismeuchi/dados-adf-base-cnpj).
Basicamente criei um pipeline com as seguintes atividades:
1. *set_variable_p_folder*: atividade do tipo *set variable* que atribui o nome da pasta que deve ser consultada no site do governo.
2. *read_files_metadata*: atividade do tipo *lookup* que faz a leitura de um arquivo *xlsx* que indica quais arquivos devem ser lidos da pasta no site do governo.
3. *filter_actives*: atividade do tipo *filter* que filtra somente os arquivos que estiverem indicados com status *active*. Assim eu podia fazer testes facilmente, inativando os arquivos que não queria processar naquele momento.
4. *ForEach1*: atividade do tipo *for each* que itera sobre os nomes dos arquivos a serem lidos, executando as seguintes atividades para cada arquivo:
    - *copy_http_to_landing*: atividade do tipo *copy data* que copia o arquivo do site do governo para o conateiner *landing* no Azure Data Lake Storage.
    - *unzip_files*: atividade do tipo *copy data* que copia o arquivo *zip*, descompacta ele e salva como *parquet* no container *bronze* no Azure Data Lake Storage.

Posteriomente, pode ser adicionado um novo pipeline a esse projeto que faz a consulta das pastas disponíveis no site do governo, verifica quais ainda não foram importadas para o Data Lake e faz a importação automaticamente conforme agendamento.

# Transformação

# Visualização

# Análise dos Dados

# Custo
