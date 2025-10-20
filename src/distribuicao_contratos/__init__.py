"""Distribui contratos em escritorios seguindo regras de Concentracao,
Fidelizacao e Meritocracia.

O algoritmo foi organizado em etapas claramente separadas e comentadas:
1. Normalizacao das entradas.
2. Calculo das quotas (percentuais -> quantidades) por Carteira/Regiao/Flag.
3. Selecoes de candidatos conforme as regras de concentracao.
4. Aplicacao dos candidatos respeitando o controle incremental de quota.
5. Fidelizacao dos casos remanescentes.
6. Meritocracia como ultima camada de preenchimento.
7. Construcoes de auditoria, resumo e export final.

Todas as operacoes ocorrem exclusivamente em PySpark, explorando Window
functions para manter determinismo e evitar UDFs Python. A funcao publica
``distribuir_contratos`` entrega todas as tabelas solicitadas. Para facilitar
o uso operacional, ha helpers que carregam diretamente os arquivos Excel
compartilhados no ``/Workspace`` e devolvem DataFrames PySpark prontos para o
pipeline.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
import re
import unicodedata
from typing import Dict, Iterable, Optional, Tuple, Union

import pandas as pd
from pyspark.sql import DataFrame, Window
from pyspark.sql import SparkSession, functions as F, types as T


# ---------------------------------------------------------------------------
# Parametros oficiais do algoritmo
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DistribuicaoParams:
    """Parametros de configuracao utilizados durante a distribuicao."""

    tolerancia_pp: float = 0.1
    algoritmo_versao: str = "1.0.0"
    quota_minima: int = 0


# ---------------------------------------------------------------------------
# Helpers de carregamento de arquivos padrao do Workspace
# ---------------------------------------------------------------------------


def _ler_excel_para_spark(
    spark: SparkSession,
    caminho: str,
    *,
    normalizar_percentual: bool = False,
) -> DataFrame:
    """Le um Excel com pandas e devolve DataFrame Spark tipado automaticamente."""

    pdf = pd.read_excel(caminho, engine="openpyxl")
    if "data_cadastrato" in pdf.columns:
        pdf["data_cadastrato"] = pd.to_datetime(pdf["data_cadastrato"], errors="coerce").dt.date
    if normalizar_percentual and "percentual" in pdf.columns:
        pdf["percentual"] = pd.to_numeric(pdf["percentual"], errors="coerce")
    return spark.createDataFrame(pdf)


def montar_depara_padrao(
    df_validos: DataFrame,
    df_percentual_flag1: DataFrame,
    df_percentual_flag0: DataFrame,
) -> DataFrame:
    """Combina planilhas de atuacao e percentuais em um unico de/para."""

    def _preparar_percentual(df_pct: DataFrame, flag: int) -> DataFrame:
        resultado = df_pct
        if "percentual" in resultado.columns and "Quota_percentual" not in resultado.columns:
            resultado = resultado.withColumnRenamed("percentual", "Quota_percentual")
        resultado = resultado.withColumn("Flag_rastreador", F.lit(flag).cast("int"))
        return resultado

    def _combinar(df_pct: DataFrame, flag: int) -> DataFrame:
        preparado = _preparar_percentual(df_pct, flag)
        if "Flag_rastreador" in preparado.columns:
            preparado = preparado.drop("Flag_rastreador")
        chaves_possiveis = ["Escritorio_cod", "Carteira", "Regiao"]
        chaves_join = [c for c in chaves_possiveis if c in df_validos.columns and c in preparado.columns]
        base = df_validos.withColumn("Flag_rastreador", F.lit(flag).cast("int"))
        if chaves_join:
            combinado = base.join(preparado, chaves_join, "left")
        else:
            combinado = base
        if "Quota_percentual" not in combinado.columns:
            combinado = combinado.withColumn("Quota_percentual", F.lit(0.0).cast("double"))
        else:
            combinado = combinado.withColumn("Quota_percentual", F.col("Quota_percentual").cast("double"))
        return combinado

    depara_flag1 = _combinar(df_percentual_flag1, 1)
    depara_flag0 = _combinar(df_percentual_flag0, 0)

    combinado = depara_flag1.unionByName(depara_flag0, allowMissingColumns=True)
    chaves_unicas = [
        col
        for col in ["Escritorio_cod", "Carteira", "Regiao", "Flag_rastreador"]
        if col in combinado.columns
    ]
    if chaves_unicas:
        combinado = combinado.dropDuplicates(chaves_unicas)

    return combinado


def carregar_bases_workspace(
    spark: SparkSession,
    workspace_dir: str = "/Workspace",
) -> Dict[str, DataFrame]:
    """Carrega os arquivos Excel padrao e devolve DFs preparados para o fluxo."""

    caminhos = {
        "contratos": os.path.join(workspace_dir, "base_contratos_distribuir.xlsx"),
        "legado": os.path.join(workspace_dir, "Base_legado_passado.xlsx"),
        "validos": os.path.join(workspace_dir, "Depara_escri_aten_rastreador.xlsx"),
        "pct_flag1": os.path.join(workspace_dir, "Depara_rastreador.xlsx"),
        "pct_flag0": os.path.join(workspace_dir, "depara_sem_rastreador.xlsx"),
    }

    df_contratos = _ler_excel_para_spark(spark, caminhos["contratos"])
    df_legado = _ler_excel_para_spark(spark, caminhos["legado"])
    df_validos = _ler_excel_para_spark(spark, caminhos["validos"])
    df_pct_flag1 = _ler_excel_para_spark(spark, caminhos["pct_flag1"], normalizar_percentual=True)
    df_pct_flag0 = _ler_excel_para_spark(spark, caminhos["pct_flag0"], normalizar_percentual=True)

    df_depara = montar_depara_padrao(df_validos, df_pct_flag1, df_pct_flag0)

    return {
        "df_contratos": df_contratos,
        "df_legado": df_legado,
        "df_depara_validos": df_validos,
        "df_percentual_flag1": df_pct_flag1,
        "df_percentual_flag0": df_pct_flag0,
        "df_depara": df_depara,
    }


# ---------------------------------------------------------------------------
# Helpers de normalizacao
# ---------------------------------------------------------------------------


def _ensure_columns(df: DataFrame, specs: Iterable[Tuple[str, T.DataType]]) -> DataFrame:
    """Garante que colunas existam e estejam tipadas conforme especificacao."""

    result = df
    for name, dtype in specs:
        if name not in result.columns:
            result = result.withColumn(name, F.lit(None).cast(dtype))
        else:
            result = result.withColumn(name, F.col(name).cast(dtype))
    return result


def _normalize_boolean(df: DataFrame, column: str) -> DataFrame:
    if column not in df.columns:
        return df
    normalized = (
        F.when(F.col(column).isin("1", "true", "True", "Y", "Sim", "SIM"), F.lit(True))
        .when(F.col(column).isin("0", "false", "False", "N", "Nao", "NAO"), F.lit(False))
        .otherwise(F.col(column).cast("boolean"))
    )
    return df.withColumn(column, normalized)


def _slugify(text: str) -> str:
    """Cria chave simplificada para identificar colunas equivalentes."""

    normalized = unicodedata.normalize("NFKD", text)
    normalized = "".join(char for char in normalized if not unicodedata.combining(char))
    normalized = normalized.lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    return normalized.strip("_")


def _padronizar_colunas(df: DataFrame, mapa: Dict[str, Iterable[str]]) -> DataFrame:
    """Renomeia colunas variantes para o padrao oficial usando chaves flexiveis."""

    resultado = df
    slug_para_nome = {_slugify(col): col for col in resultado.columns}

    for destino, chaves in mapa.items():
        if destino in resultado.columns:
            continue
        for chave in chaves:
            if chave in slug_para_nome:
                origem = slug_para_nome[chave]
                resultado = resultado.withColumnRenamed(origem, destino)
                slug_para_nome = {_slugify(col): col for col in resultado.columns}
                break

    return resultado


def _trim_string_columns(df: DataFrame, colunas: Iterable[str]) -> DataFrame:
    """Remove espacos excedentes das colunas string utilizadas como chaves."""

    tipos = {campo.name: campo.dataType for campo in df.schema}
    resultado = df
    for coluna in colunas:
        tipo = tipos.get(coluna)
        if isinstance(tipo, T.StringType):
            resultado = resultado.withColumn(coluna, F.trim(F.col(coluna)))
    return resultado


def normalizar_entradas(
    df_contratos: DataFrame,
    df_legado: DataFrame,
    df_depara_escritorios: DataFrame,
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """Padroniza colunas essenciais para todo o fluxo."""

    mapa_contratos = {
        "CPF": {"cpf"},
        "Numero_de_contrato": {
            "numero_de_contrato",
            "numero_do_contrato",
            "numero_contrato",
            "numero_de_casos",
            "numero_do_caso",
        },
        "Carteira": {"carteira"},
        "Regiao": {"regiao"},
        "Flag_rastreador": {"flag_rastreador", "flag", "flagrastreador"},
        "Escritorio_origem_cod": {"escritorio_origem_cod", "cod_escritorio_origem"},
        "Escritorio_origem": {"escritorio_origem", "escritorio_origem_nome"},
        "COD_escritorio": {"cod_escritorio", "codigo_escritorio"},
        "Escritorio": {"escritorio", "nome_escritorio"},
    }
    mapa_legado = {
        "CPF": {"cpf"},
        "Escritorio_legado_cod": {"escritorio_legado_cod", "cod_escritorio_legado"},
        "Escritorio_legado": {"escritorio_legado", "nome_escritorio_legado"},
        "Carteira": {"carteira"},
        "Regiao": {"regiao"},
    }
    mapa_depara = {
        "Escritorio_cod": {"escritorio_cod", "cod_escritorio", "codigo_escritorio"},
        "Escritorio_nome": {"escritorio_nome", "escritorio"},
        "Carteira": {"carteira"},
        "Regiao": {"regiao"},
        "Atua_na_carteira_regiao": {"atua_na_carteira_regiao", "atua"},
        "Quota_percentual": {"quota_percentual", "percentual", "meta_percentual"},
        "Flag_rastreador": {"flag_rastreador", "flag", "flagrastreador"},
    }

    contratos_brutos = _padronizar_colunas(df_contratos, mapa_contratos)
    legado_bruto = _padronizar_colunas(df_legado, mapa_legado)
    depara_bruto = _padronizar_colunas(df_depara_escritorios, mapa_depara)

    contratos = _ensure_columns(
        contratos_brutos,
        [
            ("CPF", T.StringType()),
            ("Numero_de_contrato", T.StringType()),
            ("Carteira", T.StringType()),
            ("Regiao", T.StringType()),
            ("Flag_rastreador", T.IntegerType()),
            ("Escritorio_origem_cod", T.StringType()),
            ("Escritorio_origem", T.StringType()),
        ],
    )
    contratos = contratos.fillna({"Flag_rastreador": 0})
    contratos = contratos.withColumn("Flag_rastreador", F.col("Flag_rastreador").cast("int"))

    # Quando a base nao traz explicitamente o escritorio de origem, utiliza as
    # colunas operacionais ``COD_escritorio``/``Escritorio`` como fallback para
    # manter a informacao disponivel na trilha e habilitar a fidelizacao.
    if "COD_escritorio" in df_contratos.columns:
        contratos = contratos.withColumn(
            "Escritorio_origem_cod",
            F.when(
                F.col("Escritorio_origem_cod").isNull()
                | (F.trim(F.col("Escritorio_origem_cod")) == ""),
                F.col("COD_escritorio").cast("string"),
            ).otherwise(F.col("Escritorio_origem_cod")),
        )
    if "Escritorio" in df_contratos.columns:
        contratos = contratos.withColumn(
            "Escritorio_origem",
            F.when(
                F.col("Escritorio_origem").isNull()
                | (F.trim(F.col("Escritorio_origem")) == ""),
                F.col("Escritorio").cast("string"),
            ).otherwise(F.col("Escritorio_origem")),
        )

    contratos = _trim_string_columns(
        contratos,
        [
            "CPF",
            "Numero_de_contrato",
            "Carteira",
            "Regiao",
            "Escritorio_origem_cod",
            "Escritorio_origem",
            "COD_escritorio",
            "Escritorio",
        ],
    )

    legado = _ensure_columns(
        legado_bruto,
        [
            ("CPF", T.StringType()),
            ("Escritorio_legado_cod", T.StringType()),
            ("Escritorio_legado", T.StringType()),
            ("Carteira", T.StringType()),
            ("Regiao", T.StringType()),
        ],
    )

    legado = _trim_string_columns(
        legado,
        ["CPF", "Escritorio_legado_cod", "Escritorio_legado", "Carteira", "Regiao"],
    )

    depara = _ensure_columns(
        depara_bruto,
        [
            ("Escritorio_cod", T.StringType()),
            ("Escritorio_nome", T.StringType()),
            ("Carteira", T.StringType()),
            ("Regiao", T.StringType()),
            ("Atua_na_carteira_regiao", T.BooleanType()),
            ("Quota_percentual", T.DoubleType()),
            ("Flag_rastreador", T.IntegerType()),
        ],
    )
    depara = _normalize_boolean(depara, "Atua_na_carteira_regiao")
    # Bases operacionais muitas vezes nao trazem explicitamente a flag de atuacao;
    # quando a coluna estiver ausente ou completamente nula, assumimos que o
    # escritorio esta habilitado para receber contratos daquela carteira/regiao.
    if "Atua_na_carteira_regiao" in depara.columns:
        depara = depara.withColumn(
            "Atua_na_carteira_regiao",
            F.when(F.col("Atua_na_carteira_regiao").isNull(), F.lit(True)).otherwise(
                F.col("Atua_na_carteira_regiao")
            ),
        )
    depara = _trim_string_columns(
        depara,
        ["Escritorio_cod", "Escritorio_nome", "Carteira", "Regiao"],
    )

    depara = depara.fillna({"Quota_percentual": 0.0})
    depara = depara.withColumn("Flag_rastreador", F.col("Flag_rastreador").cast("int"))

    return contratos, legado, depara


# ---------------------------------------------------------------------------
# Calculo das quotas por escritorio
# ---------------------------------------------------------------------------


def calcular_capacidades(
    contratos: DataFrame,
    depara: DataFrame,
    params: DistribuicaoParams,
) -> DataFrame:
    """Converte percentuais em quotas absolutas por Carteira/Regiao/Flag."""

    # Quando o de/para nao trouxer Flag_rastreador especifico replicamos para ambos.
    if "Flag_rastreador" not in depara.columns or depara.select("Flag_rastreador").distinct().count() <= 1:
        flags = contratos.select("Flag_rastreador").distinct()
        depara_expandido = depara.drop("Flag_rastreador").distinct().crossJoin(flags)
    else:
        depara_expandido = depara

    depara_valid = depara_expandido.filter(F.col("Atua_na_carteira_regiao") == F.lit(True))

    totais = (
        contratos.groupBy("Carteira", "Regiao", "Flag_rastreador")
        .agg(F.count("Numero_de_contrato").alias("total_contratos"))
    )

    capacidade = depara_valid.join(totais, ["Carteira", "Regiao", "Flag_rastreador"], "left")
    capacidade = capacidade.fillna({"total_contratos": 0})

    capacidade = capacidade.withColumn(
        "percentual_meta",
        F.when(F.col("Quota_percentual") > 1, F.col("Quota_percentual") / 100.0).otherwise(
            F.col("Quota_percentual")
        ),
    )
    capacidade = capacidade.fillna({"percentual_meta": 0.0})

    janela = Window.partitionBy("Carteira", "Regiao", "Flag_rastreador")
    capacidade = capacidade.withColumn(
        "percentual_meta",
        F.when(
            F.sum("percentual_meta").over(janela) <= 0,
            F.when(
                F.count("Escritorio_cod").over(janela) > 0,
                F.lit(1.0) / F.count("Escritorio_cod").over(janela),
            ).otherwise(F.lit(0.0)),
        ).otherwise(F.col("percentual_meta")),
    )

    capacidade = capacidade.withColumn(
        "quota_float",
        F.col("percentual_meta") * F.col("total_contratos"),
    )
    capacidade = capacidade.withColumn("quota_floor", F.floor(F.col("quota_float")))
    capacidade = capacidade.withColumn("frac", F.col("quota_float") - F.col("quota_floor"))

    janela = Window.partitionBy("Carteira", "Regiao", "Flag_rastreador")
    capacidade = capacidade.withColumn(
        "somatorio_floor", F.sum("quota_floor").over(janela)
    ).withColumn(
        "delta",
        F.col("total_contratos") - F.col("somatorio_floor"),
    )
    capacidade = capacidade.withColumn(
        "rank_frac",
        F.row_number().over(janela.orderBy(F.col("frac").desc(), F.col("percentual_meta").desc(), F.col("Escritorio_cod"))),
    )
    capacidade = capacidade.withColumn(
        "ajuste",
        F.when(F.col("delta") > 0, F.when(F.col("rank_frac") <= F.col("delta"), F.lit(1)).otherwise(0))
        .when(
            F.col("delta") < 0,
            -F.when(F.col("rank_frac") <= -F.col("delta"), F.lit(1)).otherwise(0),
        )
        .otherwise(0),
    )
    capacidade = capacidade.withColumn(
        "quota_total",
        F.col("quota_floor") + F.col("ajuste"),
    )

    capacidade = capacidade.withColumn(
        "quota_total",
        F.when(
            F.col("percentual_meta") > 0,
            F.greatest(F.col("quota_total"), F.lit(params.quota_minima)),
        ).otherwise(F.lit(0)),
    )
    capacidade = capacidade.drop("quota_float", "quota_floor", "frac", "somatorio_floor", "delta", "rank_frac", "ajuste")
    capacidade = capacidade.withColumn("alocado", F.lit(0))

    return capacidade


def _capacidade_disponivel(capacidade: DataFrame) -> DataFrame:
    return capacidade.withColumn(
        "quota_disponivel",
        F.when((F.col("quota_total") - F.col("alocado")) < 0, F.lit(0)).otherwise(
            F.col("quota_total") - F.col("alocado")
        ),
    )


# ---------------------------------------------------------------------------
# Construcoes auxiliares para concentracao
# ---------------------------------------------------------------------------


def _escritorios_validos(contratos: DataFrame, depara: DataFrame) -> DataFrame:
    """Retorna relacao contrato ↔ escritorios aptos."""

    validos = depara.filter(F.col("Atua_na_carteira_regiao") == F.lit(True))
    contrato_cols = [F.col(f"c.{col}").alias(col) for col in contratos.columns]
    relacao = (
        contratos.alias("c")
        .join(
            validos.alias("d"),
            [
                F.col("c.Carteira") == F.col("d.Carteira"),
                F.col("c.Regiao") == F.col("d.Regiao"),
                F.col("c.Flag_rastreador") == F.col("d.Flag_rastreador"),
            ],
            "inner",
        )
        .select(
            *contrato_cols,
            "d.Escritorio_cod",
            "d.Escritorio_nome",
            "d.Quota_percentual",
        )
        .distinct()
    )
    return relacao


def _cobertura_total_por_cpf(contratos: DataFrame) -> DataFrame:
    combos = (
        contratos.select("CPF", "Carteira", "Regiao", "Flag_rastreador").distinct()
        .groupBy("CPF")
        .agg(F.count(F.struct("Carteira", "Regiao", "Flag_rastreador")).alias("combos_totais"))
    )
    return combos


def _cobertura_por_escritorio(relacao_validos: DataFrame) -> DataFrame:
    cobertura = (
        relacao_validos.select("CPF", "Escritorio_cod", "Carteira", "Regiao", "Flag_rastreador")
        .distinct()
        .groupBy("CPF", "Escritorio_cod")
        .agg(F.count(F.struct("Carteira", "Regiao", "Flag_rastreador")).alias("combos_cobertos"))
    )
    return cobertura


def _ranking_escritorios(
    relacao_validos: DataFrame,
    cobertura_total: DataFrame,
    cobertura_por_escritorio: DataFrame,
) -> DataFrame:
    peso = (
        relacao_validos.groupBy("CPF", "Escritorio_cod")
        .agg(F.sum(F.col("Quota_percentual")).alias("peso_total"))
    )
    ranking = cobertura_por_escritorio.join(cobertura_total, "CPF", "inner")
    ranking = ranking.join(peso, ["CPF", "Escritorio_cod"], "left")
    ranking = ranking.fillna({"peso_total": 0.0})
    ranking = ranking.withColumn(
        "cobre_todas",
        F.col("combos_cobertos") == F.col("combos_totais"),
    )
    window_rank = Window.partitionBy("CPF").orderBy(
        F.col("cobre_todas").desc(),
        F.col("peso_total").desc(),
        F.col("Escritorio_cod"),
    )
    ranking = ranking.withColumn("ordem", F.row_number().over(window_rank))
    return ranking


def gerar_candidatos_concentracao(
    contratos: DataFrame,
    legado: DataFrame,
    depara: DataFrame,
) -> Tuple[DataFrame, DataFrame]:
    """Gera candidatos de concentracao e motivos de falha."""

    relacao_validos = _escritorios_validos(contratos, depara)
    cobertura_total = _cobertura_total_por_cpf(contratos)
    cobertura_escritorio = _cobertura_por_escritorio(relacao_validos)
    ranking = _ranking_escritorios(relacao_validos, cobertura_total, cobertura_escritorio)

    estatisticas_cpf = contratos.groupBy("CPF").agg(
        F.count("Numero_de_contrato").alias("qtd_contratos")
    )

    ranking_completo = ranking.filter(F.col("cobre_todas") == F.lit(True))

    legado_unico = legado.select("CPF", "Escritorio_legado_cod").dropDuplicates(["CPF"])

    candidatos_base = (
        contratos.alias("c")
        .join(estatisticas_cpf.alias("e"), "CPF", "inner")
        .join(ranking_completo.alias("r"), "CPF", "inner")
        .join(legado_unico.alias("l"), "CPF", "left")
        .select(
            "c.*",
            F.col("e.qtd_contratos"),
            F.col("r.Escritorio_cod").alias("escritorio_candidato"),
            F.col("r.ordem").alias("ordem_preferencia"),
            F.col("l.Escritorio_legado_cod").alias("legado_cod"),
        )
        .filter(
            (F.col("qtd_contratos") >= 2)
            | (
                (F.col("qtd_contratos") == 1)
                & F.col("legado_cod").isNotNull()
                & (F.trim(F.col("legado_cod")) != "")
            )
        )
    )

    candidatos_base = candidatos_base.withColumn(
        "prioridade_escritorio",
        F.when(
            (F.col("legado_cod").isNotNull())
            & (F.trim(F.col("legado_cod")) != "")
            & (F.col("legado_cod") == F.col("escritorio_candidato")),
            F.lit(0),
        ).otherwise(F.col("ordem_preferencia")),
    )

    candidatos_base = candidatos_base.withColumn(
        "sub_regra",
        F.when(
            (F.col("qtd_contratos") >= 2)
            & (F.col("legado_cod") == F.col("escritorio_candidato"))
            & F.col("legado_cod").isNotNull(),
            F.lit("Concentracao com legado"),
        )
        .when(
            (F.col("qtd_contratos") == 1)
            & (F.col("legado_cod") == F.col("escritorio_candidato"))
            & F.col("legado_cod").isNotNull(),
            F.lit("Concentracao com legado ativo"),
        )
        .otherwise(F.lit("Concentracao")),
    )

    candidatos_base = candidatos_base.drop("qtd_contratos", "ordem_preferencia", "legado_cod")

    janela_preferencia = Window.partitionBy("CPF", "escritorio_candidato").orderBy("prioridade_escritorio")
    candidatos_base = candidatos_base.withColumn(
        "prioridade_escritorio",
        F.row_number().over(janela_preferencia) + F.col("prioridade_escritorio") - 1,
    )

    candidatos_base = candidatos_base.dropDuplicates(
        ["Numero_de_contrato", "escritorio_candidato", "prioridade_escritorio"]
    )

    cpfs_sem_cobertura = ranking.filter(F.col("cobre_todas") == F.lit(False)).select("CPF").distinct()

    if not _is_dataframe_empty(cpfs_sem_cobertura):
        janela_grupo = Window.partitionBy("Carteira", "Regiao", "Flag_rastreador").orderBy(
            F.col("Quota_percentual").desc(),
            F.col("Escritorio_cod"),
        )
        melhor_por_grupo = (
            relacao_validos.withColumn("ordem_grupo", F.row_number().over(janela_grupo))
            .filter(F.col("ordem_grupo") == 1)
            .select(
                "Carteira",
                "Regiao",
                "Flag_rastreador",
                F.col("Escritorio_cod").alias("escritorio_candidato"),
            )
        )

        fallback = (
            contratos.alias("c")
            .join(cpfs_sem_cobertura.alias("s"), "CPF", "inner")
            .join(
                melhor_por_grupo.alias("m"),
                ["Carteira", "Regiao", "Flag_rastreador"],
                "left",
            )
            .select("c.*", F.col("m.escritorio_candidato"))
            .withColumn("prioridade_escritorio", F.lit(999))
            .withColumn("sub_regra", F.lit("Concentracao fallback por grupo"))
        )

        candidatos = candidatos_base.unionByName(fallback, allowMissingColumns=True)
    else:
        candidatos = candidatos_base

    motivos = (
        contratos.join(cpfs_sem_cobertura, "CPF", "inner")
        .select(
            "Numero_de_contrato",
            "CPF",
            "Carteira",
            "Regiao",
            "Flag_rastreador",
            F.lit("sem_escritorio_valido").alias("motivo_nao_concentrado"),
        )
    )

    return candidatos, motivos


# ---------------------------------------------------------------------------
# Motor de aplicacao de candidatos respeitando quotas
# ---------------------------------------------------------------------------


def _is_dataframe_empty(df: DataFrame) -> bool:
    """Retorna ``True`` quando o DataFrame nao possui linhas."""

    return df.select(F.lit(1)).limit(1).count() == 0


def aplicar_candidatos(
    candidatos: DataFrame,
    capacidade: DataFrame,
    descricao_regra: str,
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """Materializa candidatos limitados pela capacidade e preservando CPFs."""

    if _is_dataframe_empty(candidatos):
        vazio = candidatos.sparkSession.createDataFrame([], candidatos.schema)
        return vazio, vazio, capacidade

    capacidade_disponivel = _capacidade_disponivel(capacidade)

    candidatos_join = candidatos.join(
        capacidade_disponivel.select(
            "Carteira",
            "Regiao",
            "Flag_rastreador",
            F.col("Escritorio_cod").alias("escritorio_candidato"),
            "quota_disponivel",
        ),
        ["Carteira", "Regiao", "Flag_rastreador", "escritorio_candidato"],
        "left",
    )
    candidatos_join = candidatos_join.fillna({"quota_disponivel": 0})

    janela_escr = Window.partitionBy(
        "Carteira", "Regiao", "Flag_rastreador", "escritorio_candidato"
    ).orderBy("CPF", "Numero_de_contrato")
    candidatos_rank = candidatos_join.withColumn("ordem", F.row_number().over(janela_escr))
    candidatos_rank = candidatos_rank.withColumn(
        "dentro_da_quota",
        F.when(F.col("ordem") <= F.col("quota_disponivel"), F.lit(1)).otherwise(F.lit(0)),
    )

    janela_cpf = Window.partitionBy("CPF")
    candidatos_rank = candidatos_rank.withColumn(
        "cpf_respeitado",
        F.min("dentro_da_quota").over(janela_cpf),
    )

    aprovados = candidatos_rank.filter(F.col("cpf_respeitado") == 1).drop(
        "ordem", "dentro_da_quota", "cpf_respeitado", "quota_disponivel"
    )
    rejeitados = candidatos_rank.filter(F.col("cpf_respeitado") != 1).drop(
        "ordem", "dentro_da_quota", "cpf_respeitado", "quota_disponivel"
    )

    if _is_dataframe_empty(aprovados):
        return aprovados, candidatos, capacidade

    consumo = (
        aprovados.groupBy("Carteira", "Regiao", "Flag_rastreador", "escritorio_candidato")
        .agg(F.count("Numero_de_contrato").alias("consumo"))
    )
    capacidade_ajustada = capacidade.join(
        consumo.select(
            "Carteira",
            "Regiao",
            "Flag_rastreador",
            F.col("escritorio_candidato").alias("Escritorio_cod"),
            "consumo",
        ),
        ["Carteira", "Regiao", "Flag_rastreador", "Escritorio_cod"],
        "left",
    )
    capacidade_ajustada = capacidade_ajustada.fillna({"consumo": 0})
    capacidade_ajustada = capacidade_ajustada.withColumn(
        "alocado",
        F.col("alocado") + F.col("consumo"),
    ).drop("consumo")

    aprovados = aprovados.withColumn("tipo_regra_final", F.lit(descricao_regra))

    return aprovados, rejeitados, capacidade_ajustada


def aplicar_candidatos_por_prioridade(
    candidatos: DataFrame,
    capacidade: DataFrame,
    descricao_regra: str,
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """Aplica candidatos em ondas respeitando a prioridade de escritorios."""

    if _is_dataframe_empty(candidatos):
        vazio = candidatos.sparkSession.createDataFrame([], candidatos.schema)
        return vazio, vazio, capacidade

    prioridades = [
        row.prioridade_escritorio
        for row in candidatos.select("prioridade_escritorio")
        .distinct()
        .orderBy("prioridade_escritorio")
        .collect()
    ]

    aprovados_total = candidatos.sparkSession.createDataFrame([], candidatos.schema)
    rejeitados_total = candidatos.sparkSession.createDataFrame([], candidatos.schema)
    pendentes = candidatos
    capacidade_atual = capacidade

    for prioridade in prioridades:
        lote = pendentes.filter(F.col("prioridade_escritorio") == prioridade)
        aprovados_lote, rejeitados_lote, capacidade_atual = aplicar_candidatos(
            lote, capacidade_atual, descricao_regra
        )

        aprovados_total = aprovados_total.unionByName(
            aprovados_lote, allowMissingColumns=True
        )
        rejeitados_total = rejeitados_total.unionByName(
            rejeitados_lote, allowMissingColumns=True
        )

        if not _is_dataframe_empty(aprovados_lote):
            cpfs_atendidos = aprovados_lote.select("CPF").distinct()
            pendentes = pendentes.join(cpfs_atendidos, "CPF", "left_anti")

        pendentes = pendentes.filter(F.col("prioridade_escritorio") > prioridade)

    if not _is_dataframe_empty(pendentes):
        rejeitados_total = rejeitados_total.unionByName(
            pendentes, allowMissingColumns=True
        )

    return aprovados_total, rejeitados_total, capacidade_atual


def _empty_aprovados_like(contratos: DataFrame) -> DataFrame:
    """Cria DataFrame vazio compatível com o formato esperado pelos aprovados."""

    base = contratos
    for coluna, tipo in [
        ("escritorio_candidato", T.StringType()),
        ("sub_regra", T.StringType()),
        ("tipo_regra_final", T.StringType()),
    ]:
        base = base.withColumn(coluna, F.lit(None).cast(tipo))
    return base.limit(0)


def alocar_meritocracia(
    contratos_pendentes: DataFrame,
    depara: DataFrame,
    capacidade: DataFrame,
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """Distribui contratos remanescentes por meritocracia com fallback ordenado."""

    if _is_dataframe_empty(contratos_pendentes):
        vazio = _empty_aprovados_like(contratos_pendentes)
        return vazio, capacidade, contratos_pendentes

    relacao_validos = _escritorios_validos(contratos_pendentes, depara)
    if _is_dataframe_empty(relacao_validos):
        vazio = _empty_aprovados_like(contratos_pendentes)
        return vazio, capacidade, contratos_pendentes

    cobertura_total = _cobertura_total_por_cpf(contratos_pendentes)
    cobertura_escritorio = _cobertura_por_escritorio(relacao_validos)
    ranking = _ranking_escritorios(relacao_validos, cobertura_total, cobertura_escritorio)
    ranking = ranking.filter(F.col("cobre_todas") == F.lit(True))

    if _is_dataframe_empty(ranking):
        vazio = _empty_aprovados_like(contratos_pendentes)
        return vazio, capacidade, contratos_pendentes

    candidatos = (
        contratos_pendentes.alias("c")
        .join(ranking.alias("r"), "CPF", "inner")
        .select(
            "c.*",
            F.col("r.Escritorio_cod").alias("escritorio_candidato"),
            F.col("r.ordem").alias("ordem_preferencia"),
        )
        .withColumn("sub_regra", F.lit("Meritocracia"))
        .dropDuplicates(["Numero_de_contrato", "escritorio_candidato", "ordem_preferencia"])
    )

    preferencias = [
        row.ordem_preferencia
        for row in candidatos.select("ordem_preferencia").distinct().orderBy("ordem_preferencia").collect()
    ]

    aprovados_total = None
    capacidade_atual = capacidade
    pendentes_atual = contratos_pendentes

    for pref in preferencias:
        candidatos_pref = candidatos.filter(F.col("ordem_preferencia") == pref)
        candidatos_pref = candidatos_pref.join(
            pendentes_atual.select("Numero_de_contrato").distinct(),
            "Numero_de_contrato",
            "inner",
        )

        aprovados_pref, _, capacidade_atual = aplicar_candidatos(
            candidatos_pref.drop("ordem_preferencia"), capacidade_atual, "Meritocracia"
        )

        if not _is_dataframe_empty(aprovados_pref):
            pendentes_atual = pendentes_atual.join(
                aprovados_pref.select("Numero_de_contrato").distinct(),
                "Numero_de_contrato",
                "left_anti",
            )
            aprovados_pref = aprovados_pref.drop("ordem_preferencia", "quota_disponivel")
            aprovados_total = (
                aprovados_pref
                if aprovados_total is None
                else aprovados_total.unionByName(aprovados_pref, allowMissingColumns=True)
            )

    if aprovados_total is None:
        aprovados_total = _empty_aprovados_like(contratos_pendentes)

    return aprovados_total, capacidade_atual, pendentes_atual


# ---------------------------------------------------------------------------
# Pipeline completo
# ---------------------------------------------------------------------------


def distribuir_contratos(
    df_contratos: DataFrame,
    df_legado: DataFrame,
    df_depara_escritorios: DataFrame,
    params: Optional[Union[DistribuicaoParams, Dict[str, object]]] = None,
) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame]:
    """Aplica todo o fluxo de distribuicao e retorna as tabelas oficiais."""

    if params is None:
        params_obj = DistribuicaoParams()
    elif isinstance(params, DistribuicaoParams):
        params_obj = params
    elif isinstance(params, dict):
        params_obj = DistribuicaoParams(**params)
    else:
        raise TypeError(
            "params deve ser None, um dict com chaves validas ou uma instancia de DistribuicaoParams"
        )

    contratos, legado, depara = normalizar_entradas(df_contratos, df_legado, df_depara_escritorios)
    capacidade = calcular_capacidades(contratos, depara, params_obj)

    candidatos_concentracao, motivos_conc = gerar_candidatos_concentracao(contratos, legado, depara)

    # 1) Concentracao – prioridade para flag 1
    candidatos_concentracao_flag1 = candidatos_concentracao.filter(F.col("Flag_rastreador") == 1)
    aprovados_concentracao_flag1, _, capacidade = aplicar_candidatos_por_prioridade(
        candidatos_concentracao_flag1, capacidade, "Concentracao"
    )

    # Mantem CPFs com rastreador para contratos sem rastreador
    destino_flag1 = aprovados_concentracao_flag1.select("CPF", "escritorio_candidato").distinct()
    candidatos_ancora_flag0 = (
        contratos.filter(F.col("Flag_rastreador") == 0)
        .join(destino_flag1, "CPF", "inner")
        .select(
            "Numero_de_contrato",
            "CPF",
            "Carteira",
            "Regiao",
            "Flag_rastreador",
            F.col("escritorio_candidato"),
        )
        .withColumn("prioridade_escritorio", F.lit(0))
        .withColumn("sub_regra", F.lit("Concentracao prio escritorio rast"))
    )
    aprovados_ancora_flag0, _, capacidade = aplicar_candidatos(
        candidatos_ancora_flag0, capacidade, "Concentracao"
    )

    # Demais flag 0 seguem concentracao padrao
    candidatos_concentracao_flag0 = candidatos_concentracao.filter(F.col("Flag_rastreador") == 0)
    candidatos_concentracao_flag0 = candidatos_concentracao_flag0.join(
        aprovados_ancora_flag0.select("Numero_de_contrato").distinct(),
        "Numero_de_contrato",
        "left_anti",
    )
    aprovados_concentracao_flag0, _, capacidade = aplicar_candidatos_por_prioridade(
        candidatos_concentracao_flag0, capacidade, "Concentracao"
    )

    aprovados_concentracao = aprovados_concentracao_flag1.unionByName(
        aprovados_ancora_flag0, allowMissingColumns=True
    )
    aprovados_concentracao = aprovados_concentracao.unionByName(
        aprovados_concentracao_flag0, allowMissingColumns=True
    )

    contratos_pendentes = contratos.join(
        aprovados_concentracao.select("Numero_de_contrato").distinct(),
        "Numero_de_contrato",
        "left_anti",
    )

    # 2) Fidelizacao – escritorio de origem valido e com quota
    origem_valida = _escritorios_validos(contratos_pendentes, depara)
    origem_valida = origem_valida.join(
        contratos_pendentes.select(
            "Numero_de_contrato",
            F.col("Escritorio_origem_cod").alias("escritorio_candidato"),
        ),
        "Numero_de_contrato",
        "inner",
    )
    origem_valida = origem_valida.filter(F.col("Escritorio_cod") == F.col("escritorio_candidato"))
    origem_valida = origem_valida.select(
        "Numero_de_contrato",
        "CPF",
        "Carteira",
        "Regiao",
        "Flag_rastreador",
        "escritorio_candidato",
    ).distinct()
    origem_valida = origem_valida.withColumn("sub_regra", F.lit("Fidelizacao"))

    aprovados_fid, rejeitados_fid, capacidade = aplicar_candidatos(
        origem_valida, capacidade, "Fidelizacao"
    )

    contratos_pendentes = contratos_pendentes.join(
        aprovados_fid.select("Numero_de_contrato").distinct(),
        "Numero_de_contrato",
        "left_anti",
    )

    # 3) Meritocracia – atribui slots restantes
    aprovados_merito, capacidade, contratos_pendentes = alocar_meritocracia(
        contratos_pendentes, depara, capacidade
    )

    resultado = aprovados_concentracao.unionByName(aprovados_fid, allowMissingColumns=True)
    resultado = resultado.unionByName(aprovados_merito, allowMissingColumns=True)
    resultado = resultado.withColumnRenamed("escritorio_candidato", "escritorio_destino_cod")

    # Evita colisoes de nomes antes de anexar as informacoes oficiais do escritorio
    if "Escritorio_nome" in resultado.columns:
        resultado = resultado.drop("Escritorio_nome")
    if "escritorio_destino" in resultado.columns:
        resultado = resultado.drop("escritorio_destino")

    nomes_escritorios = depara.select("Escritorio_cod", "Escritorio_nome").distinct()
    resultado = resultado.join(
        nomes_escritorios,
        resultado["escritorio_destino_cod"] == nomes_escritorios["Escritorio_cod"],
        "left",
    ).drop("Escritorio_cod")
    resultado = resultado.withColumnRenamed("Escritorio_nome", "escritorio_destino")

    extras_resultado = [col for col in contratos.columns if col not in resultado.columns]
    if extras_resultado:
        contratos_extras = contratos.select("Numero_de_contrato", *extras_resultado).dropDuplicates(
            ["Numero_de_contrato"]
        )
        resultado = resultado.join(contratos_extras, "Numero_de_contrato", "left")

    # Garante que todas as colunas do dataframe de entrada estejam presentes
    tipos_contratos = {campo.name: campo.dataType for campo in contratos.schema}
    for coluna in contratos.columns:
        if coluna not in resultado.columns:
            resultado = resultado.withColumn(coluna, F.lit(None).cast(tipos_contratos[coluna]))

    colunas_base = contratos.columns
    colunas_extra = [c for c in resultado.columns if c not in colunas_base]
    resultado = resultado.select(*colunas_base, *colunas_extra)

    data_execucao = F.current_timestamp()
    resultado = resultado.withColumn("data_processamento", data_execucao)
    resultado = resultado.withColumn("algoritmo_versao", F.lit(params_obj.algoritmo_versao))

    # Auditoria detalhada
    auditoria_colunas_fixas = [
        c
        for c in ["Numero_de_contrato", "CPF", "Carteira", "Regiao", "Flag_rastreador"]
        if c in contratos.columns
    ]
    auditoria_colunas_origem = [
        col_name
        for col_name in ["Escritorio_origem_cod", "Escritorio_origem"]
        if col_name in contratos.columns
    ]
    auditoria_colunas_extras = [
        c
        for c in contratos.columns
        if c not in set(auditoria_colunas_fixas + auditoria_colunas_origem)
    ]
    auditoria_select = [F.col(c) for c in auditoria_colunas_fixas]
    auditoria_select.extend(F.col(c) for c in auditoria_colunas_origem)
    auditoria_select.extend(F.col(c).alias(f"orig_{c}") for c in auditoria_colunas_extras)
    auditoria_base = contratos.select(*auditoria_select)
    auditoria = auditoria_base.join(resultado, "Numero_de_contrato", "left")
    auditoria = auditoria.withColumn(
        "motivo_nao_concentrado",
        F.lit(None).cast("string"),
    )
    motivos_conc_enriquecidos = motivos_conc.withColumn("data_processamento", data_execucao)
    motivos_conc_enriquecidos = motivos_conc_enriquecidos.withColumn(
        "algoritmo_versao", F.lit(params_obj.algoritmo_versao)
    )
    auditoria = auditoria.unionByName(motivos_conc_enriquecidos, allowMissingColumns=True)

    auditoria = auditoria.select(
        "Numero_de_contrato",
        "CPF",
        "Carteira",
        "Regiao",
        "Flag_rastreador",
        "escritorio_origem_cod",
        "escritorio_origem",
        "escritorio_destino_cod",
        "escritorio_destino",
        "tipo_regra_final",
        "sub_regra",
        "motivo_nao_concentrado",
        "data_processamento",
        "algoritmo_versao",
    )

    # Resumo por Carteira/Regiao/Escritorio
    resumo_base = (
        resultado.groupBy("Carteira", "Regiao", "Flag_rastreador", "escritorio_destino_cod", "escritorio_destino")
        .agg(F.count("Numero_de_contrato").alias("qtd_alocada"))
    )
    capacidade_resumo = capacidade.select(
        F.col("Carteira").alias("cap_Carteira"),
        F.col("Regiao").alias("cap_Regiao"),
        F.col("Flag_rastreador").alias("cap_Flag"),
        F.col("Escritorio_cod").alias("cap_Escritorio"),
        "quota_total",
        "percentual_meta",
    ).distinct()
    resumo = resumo_base.join(
        capacidade_resumo,
        (
            (resumo_base["Carteira"] == capacidade_resumo["cap_Carteira"]) &
            (resumo_base["Regiao"] == capacidade_resumo["cap_Regiao"]) &
            (resumo_base["Flag_rastreador"] == capacidade_resumo["cap_Flag"]) &
            (resumo_base["escritorio_destino_cod"] == capacidade_resumo["cap_Escritorio"])
        ),
        "left",
    ).drop("cap_Carteira", "cap_Regiao", "cap_Flag", "cap_Escritorio")
    total_por_grupo = contratos.groupBy("Carteira", "Regiao", "Flag_rastreador").agg(
        F.count("Numero_de_contrato").alias("total_grupo")
    )
    resumo = resumo.join(total_por_grupo, ["Carteira", "Regiao", "Flag_rastreador"], "left")
    resumo = resumo.withColumn(
        "percentual_meta_pp",
        F.col("percentual_meta") * 100,
    )
    resumo = resumo.withColumn(
        "percentual_real_pp",
        F.when(F.col("total_grupo") > 0, F.col("qtd_alocada") / F.col("total_grupo") * 100).otherwise(0.0),
    )
    resumo = resumo.withColumn(
        "desvio_pp",
        F.col("percentual_real_pp") - F.col("percentual_meta_pp"),
    )

    # Pendentes (nao alocados)
    pendentes = contratos.join(
        resultado.select("Numero_de_contrato").distinct(),
        "Numero_de_contrato",
        "left_anti",
    )
    pendentes = pendentes.withColumn("motivo", F.lit("quota_atingida"))

    # Grupos sem de/para
    grupos_sem_depara = contratos.join(
        depara.select("Carteira", "Regiao").distinct(),
        ["Carteira", "Regiao"],
        "left_anti",
    ).select("Carteira", "Regiao").distinct()

    # Nao concentrados analitico
    janela_cpf = Window.partitionBy("CPF")
    janela_cpf_escr = Window.partitionBy("CPF", "escritorio_destino_cod")
    nao_concentrados = resultado.withColumn(
        "qtd_total_cpf", F.count("Numero_de_contrato").over(janela_cpf)
    ).withColumn(
        "qtd_no_escr", F.count("Numero_de_contrato").over(janela_cpf_escr)
    )
    nao_concentrados = nao_concentrados.filter(F.col("qtd_total_cpf") > 1)
    nao_concentrados = nao_concentrados.filter(F.col("qtd_no_escr") != F.col("qtd_total_cpf"))

    # Export final (apenas alteracoes)
    export = resultado.filter(F.col("Escritorio_origem_cod") != F.col("escritorio_destino_cod"))

    return resultado, auditoria, resumo, pendentes, grupos_sem_depara, nao_concentrados, export


__all__ = [
    "DistribuicaoParams",
    "carregar_bases_workspace",
    "montar_depara_padrao",
    "distribuir_contratos",
    "normalizar_entradas",
    "calcular_capacidades",
]
