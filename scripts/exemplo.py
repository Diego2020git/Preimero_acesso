"""Exemplo simples de uso do algoritmo de distribuicao de contratos."""

from pathlib import Path
import sys

from pyspark.sql import SparkSession
from pyspark.sql import types as T


ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from distribuicao_contratos import DistribuicaoParams, distribuir_contratos


SCHEMA_CONTRATOS = T.StructType(
    [
        T.StructField("CPF", T.StringType(), True),
        T.StructField("Numero_de_contrato", T.StringType(), True),
        T.StructField("Carteira", T.StringType(), True),
        T.StructField("Regiao", T.StringType(), True),
        T.StructField("Flag_rastreador", T.IntegerType(), True),
        T.StructField("Escritorio_origem_cod", T.StringType(), True),
        T.StructField("Escritorio_origem", T.StringType(), True),
        T.StructField("Observacao_livre", T.StringType(), True),
    ]
)

SCHEMA_LEGADO = T.StructType(
    [
        T.StructField("CPF", T.StringType(), True),
        T.StructField("Escritorio_legado_cod", T.StringType(), True),
        T.StructField("Escritorio_legado", T.StringType(), True),
        T.StructField("Carteira", T.StringType(), True),
        T.StructField("Regiao", T.StringType(), True),
    ]
)

SCHEMA_DEPARA = T.StructType(
    [
        T.StructField("Escritorio_cod", T.StringType(), True),
        T.StructField("Escritorio_nome", T.StringType(), True),
        T.StructField("Carteira", T.StringType(), True),
        T.StructField("Regiao", T.StringType(), True),
        T.StructField("Atua_na_carteira_regiao", T.BooleanType(), True),
        T.StructField("Quota_percentual", T.DoubleType(), True),
        T.StructField("Flag_rastreador", T.IntegerType(), True),
    ]
)


def criar_dados_exemplo(spark: SparkSession):
    contratos = spark.createDataFrame(
        [
            ("111", "C1", "CarA", "Sul", 1, "E1", "Alpha", "vip"),
            ("111", "C2", "CarA", "Sul", 0, "E1", "Alpha", "vip"),
            ("222", "C3", "CarA", "Sul", 1, "E2", "Beta", "antigo"),
            ("333", "C4", "CarB", "Norte", 0, "E3", "Gamma", None),
        ],
        schema=SCHEMA_CONTRATOS,
    )

    legado = spark.createDataFrame(
        [
            ("111", "E1", "Alpha", "CarA", "Sul"),
            ("222", "E2", "Beta", "CarA", "Sul"),
        ],
        schema=SCHEMA_LEGADO,
    )

    depara = spark.createDataFrame(
        [
            ("E1", "Alpha", "CarA", "Sul", True, 50.0, 1),
            ("E1", "Alpha", "CarA", "Sul", True, 50.0, 0),
            ("E2", "Beta", "CarA", "Sul", True, 50.0, 1),
            ("E2", "Beta", "CarA", "Sul", True, 50.0, 0),
            ("E3", "Gamma", "CarB", "Norte", True, 100.0, 1),
            ("E3", "Gamma", "CarB", "Norte", True, 100.0, 0),
        ],
        schema=SCHEMA_DEPARA,
    )

    return contratos, legado, depara


def main() -> None:
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("exemplo_distribuicao")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    try:
        contratos, legado, depara = criar_dados_exemplo(spark)

        params = DistribuicaoParams(tolerancia_pp=0.1, algoritmo_versao="demo")
        tabelas = distribuir_contratos(
            contratos,
            legado,
            depara,
            params_dict=params.__dict__,
        )

        nomes = [
            "resultado",
            "auditoria",
            "resumo",
            "pendentes",
            "grupos_sem_depara",
            "nao_concentrados_analitico",
            "export",
        ]

        for nome, df in zip(nomes, tabelas):
            print(f"\n===== {nome.upper()} =====")
            df.show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
