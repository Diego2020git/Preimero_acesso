"""Pipeline passo a passo para distribuicao de contratos sem depender do pacote.

O roteiro foi pensado para ser executado diretamente em notebooks do Databricks
onde os DataFrames ``PATH_BASE``, ``PATH_LEG``, ``PATH_PCT1``, ``PATH_PCT0`` e
``PATH_VAL1`` ja estejam carregados. Todos os calculos sao feitos seguindo a
sequencia Concentracao -> Fidelizacao -> Meritocracia, respeitando a regra de
ouro (mesmo CPF deve ficar no mesmo escritorio sempre que houver capacidade).

O codigo prioriza legibilidade: variaveis com nomes descritivos, comentarios em
cada etapa e funcoes auxiliares pequenas que documentam a intencao de cada
passo. Nao ha dependencia do modulo ``distribuicao_contratos``: todo o fluxo
esta contido neste arquivo.
"""

from __future__ import annotations

from typing import List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructField, StructType, StringType

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")


# ---------------------------------------------------------------------------
# Utilitarios de conversao e normalizacao
# ---------------------------------------------------------------------------


def converter_para_spark(pdf: pd.DataFrame):
    """Converte um DataFrame Pandas em Spark preservando o schema mesmo vazio."""

    if pdf is None:
        return spark.createDataFrame([], StructType([]))
    if pdf.empty:
        schema = StructType([StructField(coluna, StringType(), True) for coluna in pdf.columns])
        return spark.createDataFrame([], schema)
    return spark.createDataFrame(pdf.astype(str))


def normalizar_dataframe_spark(df):
    """Padroniza colunas chave em Spark (trim + tipos string)."""

    if "Região" in df.columns and "Regiao" not in df.columns:
        df = df.withColumnRenamed("Região", "Regiao")
    colunas_chave = [
        "Carteira",
        "Regiao",
        "Escritorio",
        "COD_escritorio",
        "CPF",
        "Numero_de_contrato",
        "Escritorio_origem_cod",
        "Escritorio_origem",
        "Escritorio_legado_cod",
        "Escritorio_legado",
    ]
    for coluna in colunas_chave:
        if coluna in df.columns:
            df = df.withColumn(coluna, F.trim(F.col(coluna).cast("string")))
    return df


# ---------------------------------------------------------------------------
# Garantia de que os dataframes operacionais estao carregados
# ---------------------------------------------------------------------------


globs = globals()
nomes_esperados = ["PATH_BASE", "PATH_LEG", "PATH_PCT1", "PATH_PCT0", "PATH_VAL1"]
if not all(nome in globs for nome in nomes_esperados):
    raise RuntimeError(
        "Este roteiro pressupoe que PATH_BASE, PATH_LEG, PATH_PCT1, PATH_PCT0 e PATH_VAL1 "
        "ja estejam carregados como DataFrames Spark."
    )


df_contratos = normalizar_dataframe_spark(globs["PATH_BASE"])
df_legado = normalizar_dataframe_spark(globs["PATH_LEG"])
df_pct_flag1 = normalizar_dataframe_spark(globs["PATH_PCT1"])
df_pct_flag0 = normalizar_dataframe_spark(globs["PATH_PCT0"])
df_validos = normalizar_dataframe_spark(globs["PATH_VAL1"])

if "data_cadastrato" in df_legado.columns:
    df_legado = df_legado.withColumn("data_cadastrato", F.to_timestamp("data_cadastrato"))


contratos_pdf = df_contratos.toPandas()
legado_pdf = df_legado.toPandas()
pct_flag1_pdf = df_pct_flag1.toPandas()
pct_flag0_pdf = df_pct_flag0.toPandas()
validos_pdf = df_validos.toPandas()


# ---------------------------------------------------------------------------
# Padronizacao dos DataFrames Pandas
# ---------------------------------------------------------------------------


def limpar_dataframe(pdf: pd.DataFrame) -> pd.DataFrame:
    tabela = pdf.copy()
    if "Região" in tabela.columns and "Regiao" not in tabela.columns:
        tabela.rename(columns={"Região": "Regiao"}, inplace=True)
    colunas = [
        "Carteira",
        "Regiao",
        "Escritorio",
        "COD_escritorio",
        "CPF",
        "Numero_de_contrato",
        "Escritorio_origem_cod",
        "Escritorio_origem",
        "Escritorio_legado_cod",
        "Escritorio_legado",
    ]
    for coluna in colunas:
        if coluna in tabela.columns:
            tabela[coluna] = tabela[coluna].astype(str).str.strip()
    return tabela


contratos_pdf = limpar_dataframe(contratos_pdf)
legado_pdf = limpar_dataframe(legado_pdf)
pct_flag1_pdf = limpar_dataframe(pct_flag1_pdf)
pct_flag0_pdf = limpar_dataframe(pct_flag0_pdf)
validos_pdf = limpar_dataframe(validos_pdf)

if "data_cadastrato" in legado_pdf.columns:
    legado_pdf["data_cadastrato"] = pd.to_datetime(legado_pdf["data_cadastrato"], errors="coerce")


# ---------------------------------------------------------------------------
# Preparacao das tabelas de percentuais
# ---------------------------------------------------------------------------


def preparar_percentuais(df_pct: pd.DataFrame) -> pd.DataFrame:
    tabela = df_pct.copy()
    if "COD_escritorio" in tabela.columns:
        tabela["id_escritorio"] = tabela["COD_escritorio"].astype(str).str.strip()
    elif "Escritorio_cod" in tabela.columns:
        tabela["id_escritorio"] = tabela["Escritorio_cod"].astype(str).str.strip()
    else:
        tabela["id_escritorio"] = tabela.get("id_escritorio", "").astype(str).str.strip()

    if "Escritorio" in tabela.columns:
        tabela["nome_escritorio"] = tabela["Escritorio"].astype(str).str.strip()
    elif "Escritorio_nome" in tabela.columns:
        tabela["nome_escritorio"] = tabela["Escritorio_nome"].astype(str).str.strip()
    else:
        tabela["nome_escritorio"] = tabela["id_escritorio"].astype(str)

    if "percentual" in tabela.columns:
        tabela["percentual"] = tabela["percentual"].astype(str).str.replace(",", ".", regex=False)
        tabela["percentual"] = pd.to_numeric(tabela["percentual"], errors="coerce")
    else:
        tabela["percentual"] = np.nan

    if "Nota" not in tabela.columns:
        tabela["Nota"] = 0.0

    return tabela[["Carteira", "Regiao", "id_escritorio", "nome_escritorio", "percentual", "Nota"]]


percentual_flag1 = preparar_percentuais(pct_flag1_pdf)
percentual_flag0 = preparar_percentuais(pct_flag0_pdf)


def normalizar_percentuais(df_pct: pd.DataFrame) -> pd.DataFrame:
    if df_pct.empty:
        return df_pct
    tabela = df_pct.copy()
    grupos = tabela.groupby(["Carteira", "Regiao"], dropna=False)
    todos_nulos = grupos["percentual"].transform(lambda serie: serie.isna().all())
    quantidade_escritorios = grupos["id_escritorio"].transform("count")
    tabela.loc[todos_nulos, "percentual"] = 100.0 / quantidade_escritorios[todos_nulos]
    soma = grupos["percentual"].transform("sum")
    precisa_ajuste = (soma > 0) & (~np.isclose(soma, 100.0, atol=1e-6))
    tabela.loc[precisa_ajuste, "percentual"] = tabela.loc[precisa_ajuste, "percentual"] * (
        100.0 / soma[precisa_ajuste]
    )
    tabela["percentual"] = tabela["percentual"].fillna(0.0).astype(float)
    return tabela


percentual_flag1 = normalizar_percentuais(percentual_flag1)
percentual_flag0 = normalizar_percentuais(percentual_flag0)


# ---------------------------------------------------------------------------
# Preparacao da base de contratos
# ---------------------------------------------------------------------------

flag_rastreador = contratos_pdf.get("Flag_rastreador", 0)
contratos_pdf["Flag_rastreador"] = np.where(
    flag_rastreador.astype(str).isin(["1", "True", "true", "SIM", "Y"]),
    1,
    0,
)

contratos_flag1 = contratos_pdf[contratos_pdf["Flag_rastreador"] == 1].copy()
contratos_flag0 = contratos_pdf[contratos_pdf["Flag_rastreador"] == 0].copy()

chaves_pct1 = percentual_flag1[["Carteira", "Regiao"]].drop_duplicates()
chaves_pct0 = percentual_flag0[["Carteira", "Regiao"]].drop_duplicates()
contratos_flag1 = contratos_flag1.merge(chaves_pct1, on=["Carteira", "Regiao"], how="inner")
contratos_flag0 = contratos_flag0.merge(chaves_pct0, on=["Carteira", "Regiao"], how="inner")

contagem_flag1 = contratos_flag1.groupby(["Carteira", "Regiao"]).size().reset_index(name="quantidade_total")
contagem_flag0 = contratos_flag0.groupby(["Carteira", "Regiao"]).size().reset_index(name="quantidade_total")


# ---------------------------------------------------------------------------
# Metodo de Hamilton para transformar percentuais em cotas
# ---------------------------------------------------------------------------


def calcular_cotas_hamilton(pct: pd.DataFrame, totais: pd.DataFrame) -> pd.DataFrame:
    if pct.empty or totais.empty:
        return pd.DataFrame(
            columns=[
                "Carteira",
                "Regiao",
                "id_escritorio",
                "quota_total",
                "allocado_ate_agora",
                "percentual",
                "Nota",
                "nome_escritorio",
            ]
        )
    tabela = pct.merge(totais, on=["Carteira", "Regiao"], how="inner").copy()
    tabela["quota_teorica"] = tabela["percentual"] * tabela["quantidade_total"] / 100.0
    tabela["quota_base"] = np.floor(tabela["quota_teorica"]).astype(int)
    tabela["fracao"] = tabela["quota_teorica"] - tabela["quota_base"]
    soma_floor = tabela.groupby(["Carteira", "Regiao"])["quota_base"].transform("sum")
    tabela["saldo_para_distribuir"] = tabela["quantidade_total"] - soma_floor
    tabela = tabela.sort_values(
        ["Carteira", "Regiao", "fracao", "Nota", "percentual", "nome_escritorio"],
        ascending=[True, True, False, False, False, True],
    )
    tabela["posicao"] = tabela.groupby(["Carteira", "Regiao"]).cumcount() + 1
    tabela["tamanho_grupo"] = tabela.groupby(["Carteira", "Regiao"])["posicao"].transform("max")
    tabela["bonus"] = np.where(
        tabela["saldo_para_distribuir"] > 0,
        (tabela["posicao"] <= tabela["saldo_para_distribuir"]).astype(int),
        0,
    )
    tabela["penalidade"] = np.where(
        tabela["saldo_para_distribuir"] < 0,
        ((tabela["tamanho_grupo"] - tabela["posicao"]) < (-tabela["saldo_para_distribuir"]))
        .astype(int),
        0,
    )
    tabela["quota_total"] = (tabela["quota_base"] + tabela["bonus"] - tabela["penalidade"]).clip(lower=0)
    tabela["allocado_ate_agora"] = 0
    return tabela[[
        "Carteira",
        "Regiao",
        "id_escritorio",
        "quota_total",
        "allocado_ate_agora",
        "percentual",
        "Nota",
        "nome_escritorio",
    ]]


cotas_flag1 = calcular_cotas_hamilton(percentual_flag1, contagem_flag1)
cotas_flag0 = calcular_cotas_hamilton(percentual_flag0, contagem_flag0)
# ---------------------------------------------------------------------------
# Legado: ultimo escritorio por CPF
# ---------------------------------------------------------------------------

legado_tratado = legado_pdf.copy()
if "COD_escritorio" in legado_tratado.columns:
    legado_tratado["escritorio_legado"] = legado_tratado["COD_escritorio"].astype(str)
elif "Escritorio_legado_cod" in legado_tratado.columns:
    legado_tratado["escritorio_legado"] = legado_tratado["Escritorio_legado_cod"].astype(str)
else:
    legado_tratado["escritorio_legado"] = ""

legado_tratado["data_cadastrato"] = pd.to_datetime(
    legado_tratado.get("data_cadastrato", pd.NaT), errors="coerce"
)
legado_tratado = legado_tratado.sort_values(["CPF", "data_cadastrato"])
legado_ultimo = legado_tratado.groupby("CPF").tail(1)[["CPF", "escritorio_legado"]].drop_duplicates()


# ---------------------------------------------------------------------------
# Funcoes de apoio (capacidade e ranking)
# ---------------------------------------------------------------------------


def capacidade_restante(capacidade_df: pd.DataFrame, carteira: str, regiao: str, escritorio_id: str) -> int:
    filtro = (
        (capacidade_df["Carteira"] == carteira)
        & (capacidade_df["Regiao"] == regiao)
        & (capacidade_df["id_escritorio"] == str(escritorio_id))
    )
    linha = capacidade_df.loc[filtro]
    if linha.empty:
        return 0
    total = int(linha.iloc[0]["quota_total"])
    usado = int(linha.iloc[0]["allocado_ate_agora"])
    return total - usado


def consegue_atender_todos(
    capacidade_df: pd.DataFrame,
    quantidade_por_combo: Sequence[Tuple[str, str, int]],
    escritorio_id: str,
) -> bool:
    for carteira, regiao, quantidade in quantidade_por_combo:
        if capacidade_restante(capacidade_df, carteira, regiao, escritorio_id) < quantidade:
            return False
    return True


def debitar_capacidade(
    capacidade_df: pd.DataFrame,
    quantidade_por_combo: Sequence[Tuple[str, str, int]],
    escritorio_id: str,
) -> None:
    for carteira, regiao, quantidade in quantidade_por_combo:
        filtro = (
            (capacidade_df["Carteira"] == carteira)
            & (capacidade_df["Regiao"] == regiao)
            & (capacidade_df["id_escritorio"] == str(escritorio_id))
        )
        capacidade_df.loc[filtro, "allocado_ate_agora"] = (
            capacidade_df.loc[filtro, "allocado_ate_agora"].astype(int) + int(quantidade)
        )


def listar_escritorios_validos(pct_df: pd.DataFrame, carteira: str, regiao: str) -> pd.DataFrame:
    candidatos = pct_df[(pct_df["Carteira"] == carteira) & (pct_df["Regiao"] == regiao)].copy()
    return candidatos.sort_values(
        ["Nota", "percentual", "nome_escritorio", "id_escritorio"],
        ascending=[False, False, True, True],
    )


def selecionar_escritorio_intersecao(contratos_cpf: pd.DataFrame, pct_df: pd.DataFrame) -> List[str]:
    conjuntos = []
    for (carteira, regiao), _ in contratos_cpf.groupby(["Carteira", "Regiao"]):
        validos = pct_df[
            (pct_df["Carteira"] == carteira) & (pct_df["Regiao"] == regiao)
        ]["id_escritorio"].astype(str)
        conjuntos.append(set(validos))
    if not conjuntos:
        return []
    intersecao = set.intersection(*conjuntos) if len(conjuntos) > 1 else conjuntos[0]
    if not intersecao:
        return []
    ranking = pct_df[pct_df["id_escritorio"].astype(str).isin(intersecao)].copy()
    ranking = ranking.groupby("id_escritorio")[['Nota', 'percentual']].mean().reset_index()
    ranking = ranking.sort_values(["Nota", "percentual", "id_escritorio"], ascending=[False, False, True])
    return ranking["id_escritorio"].astype(str).tolist()


def preparar_quantidades_por_combo(contratos_cpf: pd.DataFrame) -> List[Tuple[str, str, int]]:
    agregados = contratos_cpf.groupby(["Carteira", "Regiao"]).size().reset_index(name="quantidade")
    return [
        (linha["Carteira"], linha["Regiao"], int(linha["quantidade"]))
        for _, linha in agregados.iterrows()
    ]


# ---------------------------------------------------------------------------
# Camada de concentracao (respeitando cotas)
# ---------------------------------------------------------------------------


def aplicar_concentracao(
    contratos_flag: pd.DataFrame,
    pct_df: pd.DataFrame,
    capacidade_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if contratos_flag.empty:
        return contratos_flag.iloc[0:0].copy(), capacidade_df

    capacidade_atual = capacidade_df.copy()
    multiplos = contratos_flag.groupby("CPF").size().reset_index(name="total_cpf")
    multiplos = multiplos[multiplos["total_cpf"] >= 2]
    if multiplos.empty:
        return contratos_flag.iloc[0:0].copy(), capacidade_atual

    dados = contratos_flag.merge(multiplos[["CPF"]], on="CPF", how="inner")
    dados = dados.merge(legado_ultimo, on="CPF", how="left")
    registros = []

    for cpf, blocos_cpf in dados.groupby("CPF"):
        combos = preparar_quantidades_por_combo(blocos_cpf)
        escritorio_legado = str(blocos_cpf.get("escritorio_legado", "").iloc[0] or "")

        candidatos_prioritarios: List[Tuple[str, str]] = []
        intersecao = selecionar_escritorio_intersecao(blocos_cpf, pct_df)
        if escritorio_legado and escritorio_legado in intersecao:
            candidatos_prioritarios.append((escritorio_legado, "Concentracao com legado"))
        for escritorio_id in intersecao:
            if escritorio_id == escritorio_legado:
                continue
            candidatos_prioritarios.append((escritorio_id, "Concentracao intersecao"))

        escolhido: Optional[Tuple[str, str]] = None
        for escritorio_id, sub_regra in candidatos_prioritarios:
            if consegue_atender_todos(capacidade_atual, combos, escritorio_id):
                escolhido = (escritorio_id, sub_regra)
                break

        if escolhido is not None:
            escritorio_id, sub_regra = escolhido
            bloco = blocos_cpf.copy()
            bloco["escritorio_destino_cod"] = str(escritorio_id)
            bloco["tipo_distribuicao"] = "Concentracao"
            bloco["sub_regra"] = sub_regra
            bloco["fonte_regra"] = "concentracao_total"
            registros.append(bloco.drop(columns=["escritorio_legado"], errors="ignore"))
            debitar_capacidade(capacidade_atual, combos, escritorio_id)
            continue

        registros_fallback = []
        for carteira, regiao, quantidade in combos:
            ranking = listar_escritorios_validos(pct_df, carteira, regiao)
            escritorio_escolhido = None
            for _, linha in ranking.iterrows():
                candidato_id = str(linha["id_escritorio"])
                if capacidade_restante(capacidade_atual, carteira, regiao, candidato_id) >= quantidade:
                    escritorio_escolhido = candidato_id
                    break
            if escritorio_escolhido is None:
                continue
            selecao = blocos_cpf[
                (blocos_cpf["Carteira"] == carteira) & (blocos_cpf["Regiao"] == regiao)
            ].copy()
            selecao["escritorio_destino_cod"] = escritorio_escolhido
            selecao["tipo_distribuicao"] = "Concentracao"
            selecao["sub_regra"] = "Concentracao fallback com quota"
            selecao["fonte_regra"] = "sem_intersecao_com_capacidade"
            registros_fallback.append(
                selecao.drop(columns=["escritorio_legado"], errors="ignore")
            )
            debitar_capacidade(capacidade_atual, [(carteira, regiao, quantidade)], escritorio_escolhido)

        if registros_fallback:
            registros.append(pd.concat(registros_fallback, ignore_index=True))

    if registros:
        return pd.concat(registros, ignore_index=True), capacidade_atual
    return contratos_flag.iloc[0:0].copy(), capacidade_atual


# ---------------------------------------------------------------------------
# Ancoragem de flag 0 em escritorios ja alocados para flag 1
# ---------------------------------------------------------------------------


def aplicar_ancoragem_flag_zero(
    contratos_flag0_df: pd.DataFrame,
    alocacoes_flag1: pd.DataFrame,
    pct_flag0_df: pd.DataFrame,
    capacidade_flag0_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if contratos_flag0_df.empty or alocacoes_flag1.empty:
        return contratos_flag0_df.iloc[0:0].copy(), capacidade_flag0_df

    capacidade_atual = capacidade_flag0_df.copy()
    destinos_flag1 = alocacoes_flag1[["CPF", "escritorio_destino_cod"]].drop_duplicates()
    candidatos = contratos_flag0_df.merge(destinos_flag1, on="CPF", how="inner")
    if candidatos.empty:
        return contratos_flag0_df.iloc[0:0].copy(), capacidade_atual

    registros = []
    for cpf, blocos_cpf in candidatos.groupby("CPF"):
        combos = preparar_quantidades_por_combo(blocos_cpf)
        escritorio_ancora = str(blocos_cpf["escritorio_destino_cod"].iloc[0])
        atende = True
        for carteira, regiao, quantidade in combos:
            atua = pct_flag0_df[
                (pct_flag0_df["Carteira"] == carteira)
                & (pct_flag0_df["Regiao"] == regiao)
                & (pct_flag0_df["id_escritorio"].astype(str) == escritorio_ancora)
            ]
            if atua.empty or capacidade_restante(capacidade_atual, carteira, regiao, escritorio_ancora) < quantidade:
                atende = False
                break
        if not atende:
            continue
        bloco = blocos_cpf.copy()
        bloco["escritorio_destino_cod"] = escritorio_ancora
        bloco["tipo_distribuicao"] = "Concentracao"
        bloco["sub_regra"] = "Concentracao prio escritorio rast"
        bloco["fonte_regra"] = "ancora_flag1"
        registros.append(bloco.drop(columns=["escritorio_destino_cod"], errors="ignore"))
        debitar_capacidade(capacidade_atual, combos, escritorio_ancora)

    if registros:
        return pd.concat(registros, ignore_index=True), capacidade_atual
    return contratos_flag0_df.iloc[0:0].copy(), capacidade_atual


# ---------------------------------------------------------------------------
# Fidelizacao (mantem escritorio de origem se houver capacidade)
# ---------------------------------------------------------------------------


def aplicar_fidelizacao(
    contratos_flag_df: pd.DataFrame,
    contratos_jah_alocados: pd.DataFrame,
    pct_df: pd.DataFrame,
    capacidade_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if contratos_flag_df.empty:
        return contratos_flag_df.iloc[0:0].copy(), capacidade_df

    capacidade_atual = capacidade_df.copy()
    pendentes = contratos_flag_df.copy()
    if contratos_jah_alocados is not None and not contratos_jah_alocados.empty:
        pendentes = pendentes.merge(
            contratos_jah_alocados[["Numero_de_contrato"]],
            on="Numero_de_contrato",
            how="left",
            indicator=True,
        )
        pendentes = pendentes[pendentes["_merge"] == "left_only"].drop(columns=["_merge"])
    if pendentes.empty:
        return pendentes, capacidade_atual

    if "COD_escritorio" in pendentes.columns:
        pendentes["origem_id"] = pendentes["COD_escritorio"].astype(str)
    elif "Escritorio_origem_cod" in pendentes.columns:
        pendentes["origem_id"] = pendentes["Escritorio_origem_cod"].astype(str)
    else:
        pendentes["origem_id"] = pendentes.get("Escritorio", "").astype(str)

    validos = pendentes.merge(
        pct_df[["Carteira", "Regiao", "id_escritorio"]].drop_duplicates(),
        left_on=["Carteira", "Regiao", "origem_id"],
        right_on=["Carteira", "Regiao", "id_escritorio"],
        how="inner",
    )
    if validos.empty:
        return validos.iloc[0:0].copy(), capacidade_atual

    registros = []
    for (carteira, regiao, escritorio_id), bloco in validos.groupby(
        ["Carteira", "Regiao", "origem_id"]
    ):
        quantidade = len(bloco)
        if capacidade_restante(capacidade_atual, carteira, regiao, escritorio_id) < quantidade:
            continue
        destino = bloco.copy()
        destino["escritorio_destino_cod"] = escritorio_id
        destino["tipo_distribuicao"] = "Fidelizacao"
        destino["sub_regra"] = "Fidelizacao"
        destino["fonte_regra"] = "origem_atuante"
        registros.append(destino.drop(columns=["id_escritorio"], errors="ignore"))
        debitar_capacidade(capacidade_atual, [(carteira, regiao, quantidade)], escritorio_id)

    if registros:
        return pd.concat(registros, ignore_index=True), capacidade_atual
    return validos.iloc[0:0].copy(), capacidade_atual
# ---------------------------------------------------------------------------
# Meritocracia (preenche slots remanescentes)
# ---------------------------------------------------------------------------


def aplicar_meritocracia(
    contratos_flag_df: pd.DataFrame,
    contratos_concentrados_df: pd.DataFrame,
    contratos_fidelizados_df: pd.DataFrame,
    pct_df: pd.DataFrame,
    capacidade_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    capacidade_atual = capacidade_df.copy()
    pendentes = contratos_flag_df.copy()
    for tabela in [contratos_concentrados_df, contratos_fidelizados_df]:
        if tabela is not None and not tabela.empty:
            pendentes = pendentes.merge(
                tabela[["Numero_de_contrato"]],
                on="Numero_de_contrato",
                how="left",
                indicator=True,
            )
            pendentes = pendentes[pendentes["_merge"] == "left_only"].drop(columns=["_merge"])
    if pendentes.empty:
        return pendentes.iloc[0:0].copy(), capacidade_atual

    registros = []
    for (carteira, regiao), bloco in pendentes.groupby(["Carteira", "Regiao"], sort=False):
        ranking = listar_escritorios_validos(pct_df, carteira, regiao)
        if ranking.empty:
            continue
        ranking = ranking.reset_index(drop=True)
        for _, contrato in bloco.iterrows():
            escritorio_escolhido = None
            for _, linha in ranking.iterrows():
                candidato_id = str(linha["id_escritorio"])
                if capacidade_restante(capacidade_atual, carteira, regiao, candidato_id) > 0:
                    escritorio_escolhido = candidato_id
                    break
            if escritorio_escolhido is None:
                continue
            registro = contrato.to_frame().T
            registro["escritorio_destino_cod"] = escritorio_escolhido
            registro["tipo_distribuicao"] = "Meritocracia"
            registro["sub_regra"] = "Meritocracia"
            registro["fonte_regra"] = "ranking_percentual"
            registros.append(registro)
            debitar_capacidade(capacidade_atual, [(carteira, regiao, 1)], escritorio_escolhido)

    if registros:
        return pd.concat(registros, ignore_index=True), capacidade_atual
    return pendentes.iloc[0:0].copy(), capacidade_atual


# ---------------------------------------------------------------------------
# Execucao das camadas para Flag 1
# ---------------------------------------------------------------------------

concentracao_flag1, capacidade_flag1_atualizada = aplicar_concentracao(
    contratos_flag1, percentual_flag1, cotas_flag1
)

fidelizacao_flag1, capacidade_flag1_atualizada = aplicar_fidelizacao(
    contratos_flag1, concentracao_flag1, percentual_flag1, capacidade_flag1_atualizada
)

merito_flag1, capacidade_flag1_atualizada = aplicar_meritocracia(
    contratos_flag1,
    concentracao_flag1,
    fidelizacao_flag1,
    percentual_flag1,
    capacidade_flag1_atualizada,
)

aprovados_flag1 = [
    df
    for df in [concentracao_flag1, fidelizacao_flag1, merito_flag1]
    if df is not None and not df.empty
]
aprovados_flag1_df = pd.concat(aprovados_flag1, ignore_index=True) if aprovados_flag1 else contratos_flag1.iloc[0:0].copy()


# ---------------------------------------------------------------------------
# Execucao das camadas para Flag 0
# ---------------------------------------------------------------------------

ancora_flag0, capacidade_flag0_atualizada = aplicar_ancoragem_flag_zero(
    contratos_flag0, aprovados_flag1_df, percentual_flag0, cotas_flag0
)

contratos_flag0_sem_ancora = contratos_flag0.merge(
    ancora_flag0[["Numero_de_contrato"]],
    on="Numero_de_contrato",
    how="left",
    indicator=True,
)
contratos_flag0_sem_ancora = contratos_flag0_sem_ancora[
    contratos_flag0_sem_ancora["_merge"] == "left_only"
].drop(columns=["_merge"])

concentracao_flag0, capacidade_flag0_atualizada = aplicar_concentracao(
    contratos_flag0_sem_ancora, percentual_flag0, capacidade_flag0_atualizada
)

base_para_fid_flag0 = pd.concat([ancora_flag0, concentracao_flag0], ignore_index=True, sort=False)

fidelizacao_flag0, capacidade_flag0_atualizada = aplicar_fidelizacao(
    contratos_flag0, base_para_fid_flag0, percentual_flag0, capacidade_flag0_atualizada
)

merito_flag0, capacidade_flag0_atualizada = aplicar_meritocracia(
    contratos_flag0,
    base_para_fid_flag0,
    fidelizacao_flag0,
    percentual_flag0,
    capacidade_flag0_atualizada,
)

aprovados_flag0 = [
    df
    for df in [ancora_flag0, concentracao_flag0, fidelizacao_flag0, merito_flag0]
    if df is not None and not df.empty
]
aprovados_flag0_df = pd.concat(aprovados_flag0, ignore_index=True) if aprovados_flag0 else contratos_flag0.iloc[0:0].copy()


# ---------------------------------------------------------------------------
# Consolidacao do resultado final
# ---------------------------------------------------------------------------

resultado_df = pd.concat([aprovados_flag1_df, aprovados_flag0_df], ignore_index=True, sort=False)

if "Flag_rastreador" not in resultado_df.columns:
    resultado_df = resultado_df.merge(
        contratos_pdf[["Numero_de_contrato", "Flag_rastreador"]],
        on="Numero_de_contrato",
        how="left",
    )

mapa_nomes = pd.concat([percentual_flag1, percentual_flag0], ignore_index=True).drop_duplicates(
    subset=["Carteira", "Regiao", "id_escritorio"]
)
mapa_nomes = mapa_nomes.rename(
    columns={"id_escritorio": "escritorio_destino_cod", "nome_escritorio": "escritorio_destino"}
)
resultado_df = resultado_df.merge(
    mapa_nomes[["Carteira", "Regiao", "escritorio_destino_cod", "escritorio_destino"]],
    on=["Carteira", "Regiao", "escritorio_destino_cod"],
    how="left",
)
resultado_df["escritorio_destino"] = resultado_df["escritorio_destino"].fillna(
    resultado_df["escritorio_destino_cod"].astype(str)
)

resultado_df["tipo_regra_final"] = resultado_df.get("tipo_distribuicao", "").astype(str)
resultado_df["regra_aplicada"] = resultado_df["tipo_regra_final"]
resultado_df["sub_regra"] = resultado_df.get("sub_regra", "").astype(str)
resultado_df["fonte_regra"] = resultado_df.get("fonte_regra", "").astype(str)
resultado_df["data_processamento"] = pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S")
resultado_df["algoritmo_versao"] = "mvp1_plus_ancora_v2"

colunas_contratos = list(contratos_pdf.columns)
for coluna in colunas_contratos:
    if coluna not in resultado_df.columns:
        resultado_df[coluna] = np.nan

colunas_extra = [coluna for coluna in resultado_df.columns if coluna not in colunas_contratos]
resultado_df = resultado_df[colunas_contratos + colunas_extra]


# ---------------------------------------------------------------------------
# Construcoes auxiliares: auditoria, resumo e pendencias
# ---------------------------------------------------------------------------

auditoria_df = contratos_pdf.merge(
    resultado_df[
        [
            "Numero_de_contrato",
            "escritorio_destino_cod",
            "escritorio_destino",
            "tipo_regra_final",
            "sub_regra",
            "fonte_regra",
            "data_processamento",
            "algoritmo_versao",
        ]
    ],
    on="Numero_de_contrato",
    how="left",
)
auditoria_df = auditoria_df.rename(columns={"tipo_regra_final": "tipo_regra_final"})
auditoria_df["motivo_nao_concentrado"] = np.nan

descricao_capacidade = pd.concat([cotas_flag1.assign(Flag_rastreador=1), cotas_flag0.assign(Flag_rastreador=0)])
capacidade_atual = pd.concat([capacidade_flag1_atualizada.assign(Flag_rastreador=1), capacidade_flag0_atualizada.assign(Flag_rastreador=0)])
capacidade_final = descricao_capacidade[[
    "Carteira",
    "Regiao",
    "id_escritorio",
    "quota_total",
    "percentual",
    "Nota",
    "Flag_rastreador",
]].merge(
    capacidade_atual[["Carteira", "Regiao", "id_escritorio", "allocado_ate_agora", "Flag_rastreador"]],
    on=["Carteira", "Regiao", "id_escritorio", "Flag_rastreador"],
    how="left",
)
capacidade_final = capacidade_final.rename(
    columns={
        "id_escritorio": "escritorio_destino_cod",
        "percentual": "percentual_meta",
        "allocado_ate_agora": "alocado",
    }
)
total_por_grupo = contratos_pdf.groupby(["Carteira", "Regiao", "Flag_rastreador"]).size().reset_index(name="total_contratos")
capacidade_final = capacidade_final.merge(
    total_por_grupo,
    on=["Carteira", "Regiao", "Flag_rastreador"],
    how="left",
)
capacidade_final["percentual_meta_pp"] = capacidade_final["percentual_meta"]
capacidade_final["percentual_real_pp"] = np.where(
    capacidade_final["total_contratos"] > 0,
    capacidade_final["alocado"] / capacidade_final["total_contratos"] * 100,
    0.0,
)
capacidade_final["desvio_pp"] = capacidade_final["percentual_real_pp"] - capacidade_final["percentual_meta_pp"]

pendentes_df = contratos_pdf.merge(
    resultado_df[["Numero_de_contrato"]],
    on="Numero_de_contrato",
    how="left",
    indicator=True,
)
pendentes_df = pendentes_df[pendentes_df["_merge"] == "left_only"].drop(columns=["_merge"])
if not pendentes_df.empty:
    pendentes_df["motivo"] = "Sem capacidade/mapeamento"

mapeamento_disponivel = pd.concat(
    [percentual_flag1[["Carteira", "Regiao"]], percentual_flag0[["Carteira", "Regiao"]]],
    ignore_index=True,
).drop_duplicates()
grupos_sem_depara_df = contratos_pdf.merge(
    mapeamento_disponivel,
    on=["Carteira", "Regiao"],
    how="left",
    indicator=True,
)
grupos_sem_depara_df = grupos_sem_depara_df[grupos_sem_depara_df["_merge"] == "left_only"].drop(columns=["_merge"])
grupos_sem_depara_df = grupos_sem_depara_df[["Carteira", "Regiao"]].drop_duplicates()

base_concentracao = resultado_df[[
    "Carteira",
    "Regiao",
    "Flag_rastreador",
    "CPF",
    "Numero_de_contrato",
    "escritorio_destino_cod",
]]
quantidade_por_cpf = base_concentracao.groupby(["CPF"]).size().reset_index(name="total_cpf")
quantidade_por_cpf_ofc = base_concentracao.groupby(["CPF", "escritorio_destino_cod"]).size().reset_index(name="total_no_escritorio")
nao_concentrados = quantidade_por_cpf.merge(quantidade_por_cpf_ofc, on="CPF", how="left")
nao_concentrados = nao_concentrados[nao_concentrados["total_cpf"] > 1]
nao_concentrados = nao_concentrados[
    nao_concentrados["total_no_escritorio"] != nao_concentrados["total_cpf"]
]
nao_concentrados = nao_concentrados.merge(
    base_concentracao,
    on=["CPF", "escritorio_destino_cod"],
    how="left",
)

export_df = resultado_df[
    resultado_df["escritorio_destino_cod"] != resultado_df.get("COD_escritorio", "").astype(str)
].copy()
export_df = export_df.sort_values(
    ["Carteira", "Regiao", "Flag_rastreador", "CPF", "escritorio_destino_cod"]
)


# ---------------------------------------------------------------------------
# Conversao final para Spark (entregaveis)
# ---------------------------------------------------------------------------

results = {
    "resultado": converter_para_spark(resultado_df),
    "auditoria": converter_para_spark(auditoria_df),
    "resumo": converter_para_spark(capacidade_final),
    "pendentes": converter_para_spark(pendentes_df),
    "grupos_sem_depara": converter_para_spark(grupos_sem_depara_df),
    "nao_concentrados_analitico": converter_para_spark(nao_concentrados),
    "export": converter_para_spark(export_df),
}

print("Total distribuido:", results["resultado"].count())
print("Pendentes:", results["pendentes"].count())
