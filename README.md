# Tratamento da base de CNPJ's da Receita Federal
Olá!

Esse é um projeto que criei utilizandoa base de CNPJ disponibilizada pela da Receita Federal [aqui](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj).

No desenvolvimento segui a premissa de que o usuário final precisaria das seguintes análises:

1. Trazer os indicadores abaixo sempre sendo possível avaliar por: cidade, estado, natureza juridica, CNAE, status, porte e faixa etária dos sócios.

2. Alterações dos status das empresas por periodo:
    - Quantas empresas foram abertas
    - Quantas empresas foram fechadas
    - Quantas empresas foram supensas

3. Qual o movimento de migração de empresas entre cidades/estado, ou seja, de qual cidade/estado as empresas saeem e para qual se mudam?

A partir desses requisitos, segui para a ciação da arquitetura do projeto.

