Hello!

In this project, I propose a data engineering solution processing the CNPJ's database of companies in Brazil made available by [Receita Federal](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj) to meet the demand of an end user who wishes to evaluate some aspects relating to this data.

In the sections below I present more details of the requirements and the solutions used.

# Demand

For the development of the project, consider the following list of demands that the end user listed that they would need for their analyses:

1. Bring the indicators listed in items 2 and 3, always being possible to evaluate by: city, state, legal nature, CNAE, registration status and company size.

2. Changes in company status by period:
- How many companies are active
- How many companies are suspended
- How many companies are unfit
- How many companies are closed

3. How many companies are opened per period?
4. What is the migration movement of companies between cities/states, that is, which city/state do the companies leave and which city/state do they move to?

Based on these requirements, I moved on to creating the project architecture.

# Architecture

The proposed architecture was based on the tools available on [Azure](https://azure.microsoft.com/).

In general, the data source is consumed using [Azure Data Factory](https://azure.microsoft.com/pt-br/products/data-factory) in a *pipeline* that copies the files to Azure Data Lake Storage and unpacks them in the *landing* container.

The same Azure Data Factory *pipeline* unpacks these files and saves them in *parquet* format in the *bronze* layer, keeping the same data, only changing the format.

In the next phase, the data is processed in [Databricks](https://www.databricks.com/) *notebooks* to be saved as tables in the *silver* layer using partitioning for better performance.

Based on the silver tables, it is possible to create aggregations and views that meet the requirements requested by the user in the *gold* layer, also using *notebooks* in Databricks.

To orchestrate the Databricks *notebooks*, a pipeline was created in Azure Data Factory.

After completion, a panel is created in [Power BI](https://www.microsoft.com/pt-br/power-platform/products/power-bi) that connects to the *gold* layer of the Data Lake to present the data to the user.

![image](https://github.com/user-attachments/assets/6e8f9267-5212-40ff-81a1-e862e8ecd734)


# Sources

The data sources for this project are available on the [Federal Government Open Data Portal](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj).
The files are available monthly at [link](https://dadosabertos.rfb.gov.br/CNPJ/dados_abertos_cnpj/) in *csv* format compressed into *zip* files.
I used the following files in this project:

| Files             |
| -------------     |
| Municipios        |
| Cnaes             |
| Naturezas         |
| Empresas          |
| Estabelecimentos  |

Company and Establishment files are partitioned into several files of the same size, having to be treated to be loaded into the *bronze* layer in the same table.

# Storage

The project storage uses Azure Data Lake Storage with the following containers:
- *configs*: stores configuration files to assist with loads.
- *landing*: stores the *zip* files copied from the government website. The folder division remains the same as the source (reference).
- *bronze*: stores the *csv* files that were transformed into *parquet* after unzipping. The folder division remains the same as the source (reference).
- *silver*: stores the data from the managed tables of the *silver* tier with the data cleansing necessary to meet demand.
- *gold*: stores the data from the managed tables of the *gold* tier with the aggregations necessary to meet demand.


# Ingestion

Azure Data Factory was used to perform the ingestion, and the project can be viewed in this [repository](https://github.com/laismeuchi/dados-adf-base-cnpj).

The first *pipeline* assigns to a variable the name of the folder that should be read in the source, then reads a metadata file where the files available on the government website are listed and indicates which ones should be read (status = active ).

Filter activity removes files that should not be read. This configuration makes testing easier as it allows you to leave only the desired files for consultation.

![image](https://github.com/user-attachments/assets/af1adb83-9d5c-4a11-9eb1-8bad0bcd8361)

Then, for each identified file, data from the source is ingested into the *landing* container and in the following activity the files are decompressed *zip*, transformed into *parquet* and saved in the *bronze* container.

![pipe_copy_files](https://github.com/user-attachments/assets/f24c2c42-eadc-43da-be2d-e3414368ad72)


# Transformation

The transformations were performed in *notebooks* in Databricks and placed in a second *pipeline* in Azure Data Factory to orchestrate the execution.
The *notebooks* were separated into 2 stages:
1. *bronze_to_silver*: in this step, the data is transported from the *bronze* layer to *silver*, performing the necessary cleaning and adding control information. Some tables with fixed values ​​were also created (company_size, registration_situation, change_type). The code for this step can be found [here](https://github.com/laismeuchi/dados-databricks-base-cnpj/tree/main/bronze_to_silver).
2. *gold*: in this stage, data is transported from the *silver* layer to *gold*, performing the necessary aggregations to meet the end user's demands. Some dimensions were also created that will be used to compose the *star schema* model to be used in Power BI. Code for this step can be found [here](https://github.com/laismeuchi/dados-databricks-base-cnpj/tree/main/gold)

In both stages, a managed table in the *Delta* format was created in Databricks for each entity.

The second *pipeline* of the project orchestrates the Databricks *notebooks* so that they run in the correct order.

![pipe_orchestrate_notebooks](https://github.com/user-attachments/assets/1bee8df4-4d78-4c1f-8aa7-31d66d04ba8e)

Later, new logic can be added to this project that queries the folders available on the government website, checks which have not yet been imported into the Data Lake and imports automatically according to the schedule. This way, it will not be necessary to pass manual parameters from the reference for execution.

# Visualization

The data was delivered to a Power BI panel that connects to the Databricks *gold database* which can be viewed [here](https://app.powerbi.com/view?r=eyJrIjoiZTZhZGZiYTYtZTZhYS00YjFiLThhMWEtZDgwZDQwMzRiY2E0IiwidCI6IjE2NTVhODRhLTgxZTQtNDlkNy1hNTJlLWU0YWMxYmFkZmRmMyJ9) and can have your file downloaded [here](https://github.com/laismeuchi/dados-powerbi-base-cnpj). The panel has 3 tabs with the filters requested in the initial demand:
1. Status Changes
2. Change of Address
3. Starting a Business

In the Status Changes tab, it is possible to check the number of companies by status and how the values have evolved throughout the Federal Revenue's availability.

It is also possible to check the number of companies by CNAE, Legal Nature, Size and Registration Status by selecting the desired type of view.

![image](https://github.com/user-attachments/assets/f1f9b068-fdcf-4cc9-9c38-e9cfbfe0fb3a)

In the Change of Address tab, it is possible to check the number of companies that changed their address, both those that changed State and those that changed City, and how the values evolved throughout the Federal Revenue's availability.

It is also possible to check the number of companies by CNAE, Legal Nature, Size and Registration Status by selecting the desired type of view.

![image](https://github.com/user-attachments/assets/e01302df-bd44-4a79-b7c1-64dbcce792ff)


In the Business Opening tab, you can check the number of companies opened over the years.

It is also possible to check the number of companies by CNAE, Legal Nature, Size and Registration Status by selecting the desired type of view.

![image](https://github.com/user-attachments/assets/9ce5ef24-0cf6-4a06-8963-7fe22f31415d)


# Data Analysis

Here I present some *insigths* that I verified in the data analysis based on the reference data made available in August 2024.

🏆 The 3 main CNAE's of companies active in Brazil are:
- 👚 4781400 - Retail trade of clothing and accessories representing clothing stores.
- 💇 9602501 - Hairdressers, manicures and pedicures, which represents beauty salons
- 🤑 7319002 - Sales promotion, which represents salespeople.

![image](https://github.com/user-attachments/assets/75acedf1-fd48-4370-835a-45c5292d6d10)

🗺️ The largest number of companies moving occurs between the states of São Paulo and Minas Gerais, where 319 companies left São Paulo for Minas Gerais and 287 companies left Minas Gerais for São Paulo.

📍 And most of the changes are from the city of São Paulo to Belo Horizonte in the CNAEs of 7319002 - Sales promotion, 4781400 - Retail trade of clothing and accessories and 8219999 - Preparation of documents and specialized administrative support services not previously specified .
![image](https://github.com/user-attachments/assets/4d107fe0-a1e2-492d-98ba-f33539c451fd)

🏙️ When we look at changes between cities, the majority occurs between Guarulhos and São Paulo, where 130 companies left Guarulhos for São Paulo and 114 left São Paulo for Guarulhos.

![image](https://github.com/user-attachments/assets/6b9185a9-d559-4936-b61d-eb42fdf63131)

🏢 The largest number of companies opened in Brazil was in 2021, in which 4,138,020 companies were opened, the majority of which were in the state of São Paulo, Minas Gerais and Rio de Janeiro.

🏆 The champion CNAEs were:
- 🥇 4781400 - Retail trade of clothing and accessories
- 🥈 7319002 - Sales promotion
- 🥉 9492800 - Activities of political organizations

![image](https://github.com/user-attachments/assets/9309cfc2-547a-49bd-a714-62f65c78bf10)

If you want to do your own analysis, the panel can be accessed [here](https://app.powerbi.com/view?r=eyJrIjoiZTZhZGZiYTYtZTZhYS00YjFiLThhMWEtZDgwZDQwMzRiY2E0IiwidCI6IjE2NTVhODRhLTgxZTQtNDlkNy1hNTJlLWU0YWMxYmFkZmRmMyJ9).

# Cost

The entire project was developed in the Azure environment in my personal account, which is configured for *Pay as you go*, that is, the payment that was used during the *billing* period.

The resources used were listed below, and all allocated to the East US region:
1. Storage Account: StorageV2 (general purpose v2) of the *Standard* type, with local replication (LRS) and with Data Lake Storage activated.

2. Azure Data Factory: Data factory (V2)
3. Key Vault: *Standard* type Key Vault
4. Databricks: Azure Databricks Service with *Premium Tier Pricing*, with a cluster that would spend 1-3 DBU/h.

In these configurations, the total cost was R$ 147.99 distributed as shown in the figure below:

![image](https://github.com/user-attachments/assets/359ea836-68a6-414b-98c8-b9a54ede28d8)

The cost of Databricks DBUs is not included in this cost because I used the 14 days of free DBUs offered for testing, but the VM and Storage costs are still charged by *cloud* as seen above.

To calculate what would be charged extra from Databricks, I consulted the *usage* table of the *billing* bank and the value was approximately USD 20.20 converting to BRL with the quote of 1 USD = 5.5265 BRL, it would be R$ 111.64.

![image](https://github.com/user-attachments/assets/1755302e-12ae-454d-ba1d-1d19ff5fd725)

In the end, the whole process would cost approximately R$ 259.63.

However, this value includes the execution of a consultation of 4 Federal Revenue supplies, which can be considered an initial charge.

In a more recent release run this cost would be reduced.

Thinking about the experience I gained doing this project from scratch, I think the cost-benefit was very advantageous as I learned several nuances of a data project that I can apply in practice.
