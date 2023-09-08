# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM `hive_metastore`.`default`.`municipios_2021`;

# COMMAND ----------

df_spark = spark.sql("SELECT * FROM `hive_metastore`.`default`.`municipios_2021`")


# COMMAND ----------

df_pandas = df_spark.toPandas()


# COMMAND ----------

df_pandas

# COMMAND ----------

df_pandas.count()

# COMMAND ----------

# Importando as bibliotecas necessárias
from pyspark.sql import SparkSession

# Criando uma sessão Spark
spark = SparkSession.builder.appName("CombinarArquivos").getOrCreate()

# Lista dos arquivos que você quer juntar
arquivos = [
    "dbfs:/user/hive/warehouse/escolas_atendidas_2018",
    "dbfs:/user/hive/warehouse/escolas_atendidas_2019",
    "dbfs:/user/hive/warehouse/escolas_atendidas_2020",
    "dbfs:/user/hive/warehouse/escolas_atendidas_2021",
    "dbfs:/user/hive/warehouse/escolas_atendidas_2022"
]

# Para armazenar todos os DataFrames após a leitura
lista_dfs = []

# Ler os arquivos no formato Delta e combinar num df
for arquivo in arquivos:
    df = spark.read.format("delta").load(arquivo)  # Agora estamos lendo no formato Delta
    lista_dfs.append(df)

# Juntando todos os arquivos
df_combinado = lista_dfs[0]
for df in lista_dfs[1:]:
    df_combinado = df_combinado.union(df)

# Mostrar as primeiras linhas do DataFrame combinado
df_combinado.show()

df_combinado = df_combinado.toPandas()
df_combinado

# COMMAND ----------

#Total de linhas do df - checar se realmente foram lidos todos os conteúdos dos arquivos
numero_de_linhas = len(df_combinado)
print("Total de linhas:", numero_de_linhas)


# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

# Definindo o estilo do gráfico
sns.set(style="whitegrid")

# Filtrando os dados para incluir apenas as colunas relevantes ('Ano', 'UF' e 'QtdEscolasAtendidas')
dados_filtrados = df_combinado[['Ano', 'UF', 'QtdEscolasAtendidas']]

# Agrupando os dados por 'Ano' e 'UF', e somando a 'QtdEscolasAtendidas'
dados_agrupados = dados_filtrados.groupby(['Ano', 'UF']).sum().reset_index()

# Criando um gráfico de barras
plt.figure(figsize=(15, 10))

# Criar uma paleta de cores para distinguir os anos
palette = sns.color_palette("husl", len(dados_agrupados['Ano'].unique()))

# Criando o gráfico de barras
sns.barplot(x='UF', y='QtdEscolasAtendidas', hue='Ano', data=dados_agrupados, palette=palette)

# Adicionando títulos e rótulos
plt.title('Distribuição da Quantidade de Escolas Atendidas com Merenda Escolar por Estado e Ano')
plt.xlabel('Estados')
plt.ylabel('Quantidade de Escolas Atendidas')
plt.legend(title='Ano')

# Exibindo o gráfico
plt.show()


# COMMAND ----------

#Houve crescimento no número de escolas atendidas com merenda escolar ao longo dos anos em Estados específicos?

import matplotlib.pyplot as plt
import seaborn as sns

# Definindo o estilo do gráfico
sns.set(style="whitegrid")

# Filtrando os dados para incluir apenas as colunas relevantes ('Ano', 'UF' e 'QtdEscolasAtendidas')
dados_filtrados = df_combinado[['Ano', 'UF', 'QtdEscolasAtendidas']]

# Agrupando os dados por 'Ano' e 'UF', e somando a 'QtdEscolasAtendidas'
dados_agrupados = dados_filtrados.groupby(['Ano', 'UF']).sum().reset_index()

# Convertendo a coluna 'Ano' para string
dados_agrupados['Ano'] = dados_agrupados['Ano'].astype(str)

# Para facilitar a visualização, vamos pegar apenas alguns estados como exemplo.
estados_selecionados = ['AC', 'SP', 'RJ', 'MG', 'RS']

dados_selecionados = dados_agrupados[dados_agrupados['UF'].isin(estados_selecionados)]

# Criando um gráfico de linhas
plt.figure(figsize=(15, 10))

plot = sns.lineplot(x='Ano', y='QtdEscolasAtendidas', hue='UF', data=dados_selecionados, marker='o')

# Adicionando títulos e rótulos
plt.title('Crescimento no Número de Escolas Atendidas com Merenda Escolar por Estado e Ano')
plt.xlabel('Ano')
plt.ylabel('Quantidade de Escolas Atendidas')

# Adicionando anotações nos pontos
for line in plot.lines:
    for x_val, y_val in zip(line.get_xdata(), line.get_ydata()):
        plot.text(x_val, y_val, int(y_val), horizontalalignment='left', verticalalignment='bottom')

# Ajustando a legenda para ficar fora do gráfico, no centro à direita
plt.legend(title='Estado', loc='center left', bbox_to_anchor=(1, 0.5))

# Exibindo o gráfico
plt.tight_layout()
plt.show()


# COMMAND ----------

#Quais são os municípios com o maior crescimento na quantidade de escolas atendidas com merenda entre 2018 e 2021?
#Fiz os top 10

# Primeiro, vamos "reverter" a transposição
dados_agrupados = df_combinado.groupby(['Ano', 'Municipio'])['QtdEscolasAtendidas'].sum().unstack('Ano').reset_index()

# Agora, calculamos a diferença entre 2022 e 2018
dados_agrupados['Diferença'] = dados_agrupados[2022] - dados_agrupados[2018]

# Ordenamos os municípios pela maior diferença e selecionamos os top 10
top_municipios = dados_agrupados.sort_values(by='Diferença', ascending=False).head(10)

# Plotando o gráfico
plt.figure(figsize=(15, 10))
sns.barplot(x='Diferença', y='Municipio', data=top_municipios, palette="viridis")

plt.title('Top 10 Municípios com Maior Crescimento na Quantidade de Escolas Atendidas com Merenda (2018-2022)')
plt.xlabel('Crescimento no Número de Escolas Atendidas')
plt.ylabel('Município')
plt.tight_layout()
plt.show()


# COMMAND ----------

#Quais estados apresentaram uma diminuição no número de escolas atendidas com merenda em qualquer ano durante o período?
#Fiz os top 10

import matplotlib.pyplot as plt
import seaborn as sns

# Supondo que você já tenha df_combinado definido em algum lugar do seu código:
dados_estados = df_combinado.groupby(['Ano', 'UF'])['QtdEscolasAtendidas'].sum().unstack('Ano').reset_index()

# Calculando a diferença entre 2022 e 2018 para cada estado
dados_estados['Diferença'] = dados_estados[2022] - dados_estados[2018]

# Ordenando os estados pela maior diminuição e selecionando os top 10
top_diminuicao = dados_estados.sort_values(by='Diferença').head(10)

# Plotando o gráfico
plt.figure(figsize=(15, 10))

for index, row in top_diminuicao.iterrows():
    anos = row.index[1:-1].astype(str)  
    valores = row.values[1:-1]  
    
    plt.plot(anos, valores, marker='o', label=row['UF'])
    
    # Colocando a sigla do estado na frente da linha (com um deslocamento maior)
    plt.annotate(row['UF'], 
                 (anos[0], valores[0]), 
                 textcoords="offset points", 
                 xytext=(-25,0),
                 ha='center',
                 fontsize=10,
                 color='black',
                 weight='bold')
    
    # Adicionando números nos pontos
    for ano, valor in zip(anos, valores):
        plt.annotate(str(int(valor)), 
                     (ano, valor), 
                     textcoords="offset points", 
                     xytext=(0,5), 
                     ha='center', 
                     fontsize=9)

plt.title('Top 10 Estados com Maior Diminuição no Número de Escolas Atendidas com Merenda (2018-2022)')
plt.xlabel('Ano')
plt.ylabel('Número de Escolas Atendidas')
plt.grid(True, which='both', linestyle='--', linewidth=0.5)
plt.legend(loc="upper left", bbox_to_anchor=(1,1))
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `hive_metastore`.`default`.`escolas_atendidas_2021`;

# COMMAND ----------

#Atribuindo a um dataframe o que foi carregado anteriormente via sql
df_2021 = _sqldf

# COMMAND ----------

df_2021.count()

# COMMAND ----------

#Normalizar as duas tabelas:
#escolas_atendidas_2021 = possui dados de quant escolas atendidas por município. Campo do join: Municipio
#municipios_2021 = possui dados de quant população por muncípios do Brasil em 2021. Campo do join: Name
#É necessário normalizar os dados devido as tabelas estarem com caixa alta e acentuados e outro caixa alta sem acentos. Não encontrei outra forma de fazer o join entre elas.

from pyspark.sql.functions import lower, col, regexp_replace, translate

# Carregar o dataframe da tabela 'a'
df_spark = spark.sql("SELECT * FROM `hive_metastore`.`default`.`municipios_2021`")

# Supondo que o dataframe do ano 2021 seja df_2021
# df_2021 = ... (carregar ou definir seu dataframe de 2021 aqui, se ainda não o tiver feito)

# Lista de acentos
accents = "áÁâÂàÀäÄãÃéÉêÊëËíÍîÎïÏóÓôÔöÖõÕúÚûÛüÜçÇ"
no_accents = "aAaAaAaAaAeEeEeEiIiIiIoOoOoOoOuUuUuUcC"

# Normalize o dataframe df_spark (tabela 'a')
df_spark_normalized = (df_spark.withColumn("normalized_name", 
                 lower(translate(col("name"), accents, no_accents))
                 .alias("normalized_name")))

# Removendo caracteres especiais e espaços do df_spark_normalized
df_spark_normalized = df_spark_normalized.withColumn("normalized_name", regexp_replace(col("normalized_name"), "[^a-z]", ""))

# Normalize o dataframe df_2021 (tabela 'b')
df_2021_normalized = (df_2021.withColumn("normalized_municipio", 
                 lower(translate(col("Municipio"), accents, no_accents))
                 .alias("normalized_municipio")))

# Removendo caracteres especiais e espaços do df_2021_normalized
df_2021_normalized = df_2021_normalized.withColumn("normalized_municipio", regexp_replace(col("normalized_municipio"), "[^a-z]", ""))

# Join entre os dataframes
joined_data = df_spark_normalized.join(df_2021_normalized, df_spark_normalized.normalized_name == df_2021_normalized.normalized_municipio)


# COMMAND ----------

display(joined_data)

# COMMAND ----------

joined_data.columns

# COMMAND ----------

#Melhorando a notação da quantidade da população
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

# Função para formatar os números do eixo y
def millions(x, pos):
    'Formata os números do eixo y'
    return '%1.0f' % x

formatter = FuncFormatter(millions)

# Filtrando e ordenando os municípios pela população, pegando o top 10
top_10_cities = joined_data.orderBy(col("pop_21").desc()).limit(10)

# Convertendo para um DataFrame pandas para melhor manipulação com matplotlib
top_10_pd = top_10_cities.toPandas()

# Definindo a posição das barras no eixo X
barWidth = 0.3
r1 = range(len(top_10_pd))
r2 = [x + barWidth for x in r1]

# Criando o gráfico de barras
fig, ax1 = plt.subplots(figsize=(14, 7))

# Barras para população
ax1.bar(r1, top_10_pd['pop_21'], color='blue', width=barWidth, edgecolor='white', label='população')
ax1.set_xlabel('Municípios', fontweight='bold', fontsize=15)
ax1.set_ylabel('População', color='blue')
ax1.tick_params(axis='y', labelcolor='blue')
ax1.set_title('Relação entre População e Escolas Atendidas nos Top 10 Municípios por População', fontweight='bold')

# Configuração do formato do eixo y para mostrar números inteiros
ax1.yaxis.set_major_formatter(formatter)

# Crie um segundo eixo y
ax2 = ax1.twinx()
ax2.bar(r2, top_10_pd['QtdEscolasAtendidas'], color='red', width=barWidth, edgecolor='white', label='escolas atendidas')
ax2.set_ylabel('Escolas Atendidas', color='red')
ax2.tick_params(axis='y', labelcolor='red')

ax1.set_xticks([r + barWidth for r in range(len(top_10_pd))])
ax1.set_xticklabels(top_10_pd['name'], rotation=45)

fig.tight_layout()
plt.show()


# COMMAND ----------

teste2

# COMMAND ----------


