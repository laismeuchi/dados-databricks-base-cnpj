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

A entrega dos dados foi feita em um painel do Power BI que conecta no *database gold* do Databricks que pode ser visualizado [aqui](https://app.powerbi.com/view?r=eyJrIjoiZTZhZGZiYTYtZTZhYS00YjFiLThhMWEtZDgwZDQwMzRiY2E0IiwidCI6IjE2NTVhODRhLTgxZTQtNDlkNy1hNTJlLWU0YWMxYmFkZmRmMyJ9) e pode ter seu arquivo baixado [aqui](https://github.com/laismeuchi/dados-powerbi-base-cnpj).
O painel possui 3 abas com os filtros solicitados na demanda inicial:
1. Alterações de Status
2. Alteração de Endereço
3. Abertura de Empresas

Na aba Alterações de Status é possível verificar as quantidades de empresas por status e como os valores evoluíram ao longo das disponibilizações da Receita Federal.

Também é possível verificar a quantidade de empresas por CNAE, Natureza Jurídica, Porte e Situação Cadastral selecionando o tipo de visão desejado.

![image](https://github.com/user-attachments/assets/f1f9b068-fdcf-4cc9-9c38-e9cfbfe0fb3a)

Na aba Alteração de Endereço é possível verificar as quantidades de empresas que mudaram de endereço, tanto as que mudaram de Estado quanto as que mudaram de Cidade, e como os valores evoluíram ao longo das disponibilizações da Receita Federal.

Também é possível verifica a quantidade de empresas por CNAE, Natureza Jurídica, Porte e Situação Cadastral selecionando o tipo de visão desejado.

![image](https://github.com/user-attachments/assets/e01302df-bd44-4a79-b7c1-64dbcce792ff)


Na aba Abertura de Empresas é possível verificar as quantidades de empresas abertas ao longo dos anos.

Também é possível verifica a quantidade de empresas por CNAE, Natureza Jurídica, Porte e Situação Cadastral selecionando o tipo de visão desejado.

![image](https://github.com/user-attachments/assets/9ce5ef24-0cf6-4a06-8963-7fe22f31415d)


# Análise dos Dados

Aqui apresento alguns *insigths* que verifiquei na análise dos dados com base na referência dos dados disponibilizados em agosto de 2024.

🏆 Os 3 principais CNAE's de empresas ativas no Brasil são: 
- 👚 4781400 - Comércio varejista de artigos do vestuário e acessórios que representa as lojas de roupas.
- 💇 9602501 - Cabeleireiros, manicure e pedicure que representa os salões de beleza
- 🤑 7319002 - Promoção de vendas  que representa os vendedores.

![image](https://github.com/user-attachments/assets/75acedf1-fd48-4370-835a-45c5292d6d10)

🗺️ A maior quantidade de mudança de empresas acontece entre os estados de São Paulo e Minas Gerais, onde saíram 319 empresas de São Paulo para Minas Gerais e saíram 287 empresas de Minas Gerais para São Paulo.

📍 E a maior parte das mudanças são da cidade de São Paulo para Belo Horizonte nos CNAEs de 7319002 - Promoção de vendas, 4781400 - Comércio varejista de artigos do vestuário e acessórios e 8219999 - Preparação de documentos e serviços especializados de apoio administrativo não especificados anteriormente.

![image](https://github.com/user-attachments/assets/4d107fe0-a1e2-492d-98ba-f33539c451fd)

🏙️ Já quando verificamos a mudança entre cidades, a maioria ocorre entre Guarulhos pra São Paulo, onde saíram 130 empresas de Guarulhos para São Paulo e 114 saíram de São Paulo para Guarulhos.

![image](https://github.com/user-attachments/assets/6b9185a9-d559-4936-b61d-eb42fdf63131)

🏢 A maior quantidade de empresas abertas no Brasil foi em 2021, em que foram abertas 4.138.020 empresas, onde a maioria foram no estado de São Paulo, Minas Gerais e Rio de Janeiro.

🏆 Os CNAE's campeões foram:
- 🥇 4781400 - Comércio varejista de artigos do vestuário e acessórios
- 🥈 7319002 - Promoção de vendas
- 🥉 9492800 - Atividades de organizações políticas

![image](https://github.com/user-attachments/assets/9309cfc2-547a-49bd-a714-62f65c78bf10)

Caso queira fazer sua própria análise o paniel pode ser acessado [aqui](https://app.powerbi.com/view?r=eyJrIjoiZTZhZGZiYTYtZTZhYS00YjFiLThhMWEtZDgwZDQwMzRiY2E0IiwidCI6IjE2NTVhODRhLTgxZTQtNDlkNy1hNTJlLWU0YWMxYmFkZmRmMyJ9).

# Custo

O projeto todo foi desenvolvido no ambiente Azure em minha conta pessoal que está configurada para *Pay as you go*, ou seja, pago que foi utilizado durante o período do *billing*.
Os recursos utilizados foram os listados a seguir, e todos alocados na região East US:
1. Storage Account: StorageV2 (general purpose v2) do tipo *Standard*, com replicação do tipo local(LRS) e com o Data Lake Storage ativado.
2. Azure Data Factory: Data factory (V2)
3. Key Vault: Key Vault do tipo do tipo *Standard*
4. Databricks: Azure Databricks Service com *Pricing Tier Premium*, com um cluster que gastaria 1-3 DBU/h.

Nessas configurações o custo total foi de R$ 147,99 distribuídos conforme figura abaixo:

![image](https://github.com/user-attachments/assets/359ea836-68a6-414b-98c8-b9a54ede28d8)

O custo de DBUs do Databricks não estão nesse custo pois utilizei os 14 dias de DBUs grátis que é oferecido para teste, porém os custos de VM e Storage dele ainda são cobrados pela *cloud* conforme visto acima. 

Para calcular o que seria cobrado a mais do Databricks fiz uma consulta na tabela de *usage* do banco de *billing* e o valor foi aproximadamente USD 20,20 convertendo para BRL com a cotação de 1 USD = 5.5265 BRL, daria R$ 111,64.

![image](https://github.com/user-attachments/assets/1755302e-12ae-454d-ba1d-1d19ff5fd725)

Ao final o processo todo teria um custo de aproximadamente R$ 259,63.

Porém nesse valor está a execução de uma consulta de 4 disponibilizações da Receita Federal que pode ser considerada uma carga inicial.
Em uma execução da disponibilização mais recente esse custo seria reduzido.

Pensando na experiência que adquiri fazendo esse projeto do zero, acho que o custo benefício foi muito vantajoso pois aprendi várias nuances de um projeto de dados que poderei aplicar na prática.
