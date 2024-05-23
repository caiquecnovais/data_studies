# ETL -> Extract, transform, load

# Pipeline de dados: 

#   - Trata-se da fusão de duas grandes empresas.
#   Essas duas empresas se fundiram e possuem bases de dados diferentes.
#   Ou seja, temos dados vindos de origens diferentes e que podem ter estruturas diferentes.
#   Elas precisam gerar um relatório para entender os resultados dessa fusão.
#   As vendas aumentaram, diminuíram ou estão concentradas na empresa A ou na empresa B?
#   Para ter todos esses insights (perspectivas), a equipe de BI, a equipe de Analytics,
#   precisa desses dados. Por essa razão, foi criada uma equipe de pessoas desenvolvedaoras
#   de dados, responsáveis por pegar as duas fontes de dados, fazer as transformações necessárias
#   e disponibilizar esses dados. Mas não só isso, precisamos construir um pipeline que
#   possamos reproduzir nos meses seguintes, quando essa demanda reaparecer. 

from processamento_dados import Dados

path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'


# EXTRACT
dados_empresa_A = Dados.leitura_dados(path_json, 'json')
print(f'\nNomes das colunas da Empresa A: \n{dados_empresa_A.nome_colunas}\n com {dados_empresa_A.qtd_linhas} linhas\n')

dados_empresa_B = Dados.leitura_dados(path_csv, 'csv')
print(f'\nNomes das colunas da Empresa B: \n{dados_empresa_B.nome_colunas}\n com {dados_empresa_B.qtd_linhas} linhas\n')


# TRANSFORM
key_mapping = {'Nome do Item': 'Nome do Produto',
               'Classificação do Produto': 'Categoria do Produto',
               'Valor em Reais (R$)': 'Preço do Produto (R$)',
               'Quantidade em Estoque': 'Quantidade em Estoque',
               'Nome da Loja': 'Filial',
               'Data da Venda': 'Data da Venda'
               }

dados_empresa_B.rename_columns(key_mapping)
print(f'\n Dados de colunas foram atualizados na Empresa B: \n{dados_empresa_B.nome_colunas}\n com {dados_empresa_B.qtd_linhas} linhas\n')

dados_fusao = Dados.join(dados_empresa_A, dados_empresa_B)
print(f'\n Dados da fusão: \n Colunas -> {dados_fusao.nome_colunas}\n com {dados_fusao.qtd_linhas} linhas')


# LOAD
path_dados_combinados = 'data_processed/dados_combinados.csv'
dados_fusao.salvar_dados(path_dados_combinados)
print(f'\n\n Arquivo salvo com sucesso! -> -> {path_dados_combinados}\n')