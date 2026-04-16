from leitura_dados import ler_dados
from tratamento_dados import tratar_dados
from pyspark.sql.functions import sum, avg, count, desc, round

caminho_csv = "dados/pagamentos.csv"

df = ler_dados(caminho_csv)

df_tratado = tratar_dados(df)

df_tratado.show(5)
df_tratado.printSchema()

df_tratado.agg (
    sum("valor_parcela").alias("Total Pago"),
    round(avg("valor_parcela"), 2).alias("media_pagamento")
).show()

ranking_favorecido = df_tratado.groupBy("CPF_favorecido", "nome_favorecido")\
    .agg(
        sum("valor_parcela").alias("valor_total_acumulado"),
        count("valor_parcela").alias("quantidade_parcelas")
    )\
    .orderBy(desc("valor_total_acumulado"))

print ("Ranking dos 10 Primeiros com Valores Acumulados")
ranking_favorecido.show(10, truncate = False)

media_geral = df_tratado.agg(avg("valor_parcela")).collect()[0][0]
print (f"A Média do 190 é R$: {media_geral:.2f}")

def analise_parcelas_pessoa (df, cpf, nome):
    df_pessoa = (
        df.filter((df.CPF_favorecido == cpf) & (df.nome_favorecido == nome))\
        .orderBy("ano_competencia", "mes_competencia"))
    
    resumo = (
        df_pessoa.agg(
            count("*").alias("quantidade_parcelas"),
            sum("valor_parcela").alias("total_recebido"),
            avg("valor_parcela").alias("valor_medio_parcela")
        )
    )
    return df_pessoa, resumo

cpf_exemplo = "***.600.238-**"
nome = "CRISTIANE FERNANDES DA SILVA"

df_pessoa, resumo_pessoa = analise_parcelas_pessoa (df_tratado, cpf_exemplo, nome)

df_pessoa.show ()
resumo_pessoa.show ()