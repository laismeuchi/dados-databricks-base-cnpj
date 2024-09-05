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

3. Qual o movimento de migração de empresas entre cidades/estado, ou seja, de qual cidade/estado as empresas saeem e para qual se mudam?

A partir desses requisitos, segui para a ciação da arquitetura do projeto.

# Arquitetura

A arquitetura proposta foi baseda nas ferramentas disponíveis na [Azure](https://azure.microsoft.com/).

Em uma visão geral, a fonte de dados é consumida utilizando o Azure Data Factory que copia os arquivos para Azure Data Lake Storage e faz a descompactação deles no container *landing*.

Esses arquivos são tratados em *notebooks* do Databricks, fazendo a transformação deles para arquivos parquet que são salvos na camada *bronze*.

Na fase seguinte, os dados são tratatos para serem salvos como tabelas na camada *silver* utilizando particionamento para melhor performance.

Com base nas tabelas silver é possível criar as agregações e visões que atendem os requisitos solicitados pelo usuário na camada *gold*.

Após a finalização, é criado um painel no Power BI que conecta na camada *gold* do Data Lake para apresentar os dados ao usuário.


![image](https://github.com/user-attachments/assets/6e8f9267-5212-40ff-81a1-e862e8ecd734)


# Fontes

# Armazenamento

# Ingestão

# Transformação

# Visualização

# Análise dos Dados

# Custo
