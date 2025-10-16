from pathlib import Path
import sys

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from distribuicao_contratos import DistribuicaoParams, distribuir_contratos


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("tests_distribuicao")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def _schema_contratos():
    return T.StructType(
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


def _schema_legado():
    return T.StructType(
        [
            T.StructField("CPF", T.StringType(), True),
            T.StructField("Escritorio_legado_cod", T.StringType(), True),
            T.StructField("Escritorio_legado", T.StringType(), True),
            T.StructField("Carteira", T.StringType(), True),
            T.StructField("Regiao", T.StringType(), True),
        ]
    )


def _schema_depara():
    return T.StructType(
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


def _base_depara_rows():
    return [
        # Carteira A / Sul
        ("E1", "Alpha", "CarA", "Sul", True, 60.0, 1),
        ("E1", "Alpha", "CarA", "Sul", True, 60.0, 0),
        ("E2", "Beta", "CarA", "Sul", True, 40.0, 1),
        ("E2", "Beta", "CarA", "Sul", True, 40.0, 0),
        # Carteira B / Norte
        ("E3", "Gamma", "CarB", "Norte", True, 100.0, 1),
        ("E3", "Gamma", "CarB", "Norte", True, 100.0, 0),
        # E3 tambem atende CarA/Sul
        ("E3", "Gamma", "CarA", "Sul", True, 10.0, 1),
        ("E3", "Gamma", "CarA", "Sul", True, 10.0, 0),
    ]


def test_concentracao_sem_legado(spark):
    contratos = spark.createDataFrame(
        [
            ("111", "C1", "CarA", "Sul", 1, None, None, "obs"),
            ("111", "C2", "CarA", "Sul", 1, None, None, "obs"),
        ],
        _schema_contratos(),
    )
    legado = spark.createDataFrame([], _schema_legado())
    depara = spark.createDataFrame(_base_depara_rows(), _schema_depara())

    resultado, *_ = distribuir_contratos(contratos, legado, depara)
    destinos = resultado.filter(F.col("CPF") == "111").select("escritorio_destino_cod").distinct().collect()
    assert len(destinos) == 1


def test_concentracao_com_legado(spark):
    contratos = spark.createDataFrame(
        [
            ("222", "C3", "CarA", "Sul", 1, None, None, "info"),
            ("222", "C4", "CarA", "Sul", 1, None, None, "info"),
        ],
        _schema_contratos(),
    )
    legado = spark.createDataFrame(
        [("222", "E2", "Beta", "CarA", "Sul")],
        _schema_legado(),
    )
    depara = spark.createDataFrame(_base_depara_rows(), _schema_depara())

    resultado, *_ = distribuir_contratos(contratos, legado, depara)
    destino = resultado.select("escritorio_destino_cod").collect()[0][0]
    assert destino == "E2"


def test_concentracao_legado_single(spark):
    contratos = spark.createDataFrame(
        [("333", "C5", "CarA", "Sul", 1, None, None, "legado")],
        _schema_contratos(),
    )
    legado = spark.createDataFrame(
        [("333", "E2", "Beta", "CarA", "Sul")],
        _schema_legado(),
    )
    depara = spark.createDataFrame(_base_depara_rows(), _schema_depara())

    resultado, *_ = distribuir_contratos(contratos, legado, depara)
    destino = resultado.select("escritorio_destino_cod").collect()[0][0]
    assert destino == "E2"


def test_prioridade_rastreador(spark):
    contratos = spark.createDataFrame(
        [
            ("444", "C6", "CarA", "Sul", 1, None, None, "rastro"),
            ("444", "C7", "CarA", "Sul", 0, None, None, "sem_rastro"),
        ],
        _schema_contratos(),
    )
    legado = spark.createDataFrame([], _schema_legado())
    depara = spark.createDataFrame(_base_depara_rows(), _schema_depara())

    resultado, *_ = distribuir_contratos(contratos, legado, depara)
    destinos = resultado.groupBy("CPF").agg(F.collect_set("escritorio_destino_cod").alias("destinos")).collect()
    destinos_cpf444 = [row.destinos for row in destinos if row.CPF == "444"][0]
    assert len(destinos_cpf444) == 1


def test_concentracao_multiplas_carteiras(spark):
    contratos = spark.createDataFrame(
        [
            ("555", "C8", "CarA", "Sul", 0, None, None, "multi"),
            ("555", "C9", "CarB", "Norte", 0, None, None, "multi"),
        ],
        _schema_contratos(),
    )
    legado = spark.createDataFrame([], _schema_legado())
    depara = spark.createDataFrame(_base_depara_rows(), _schema_depara())

    resultado, *_ = distribuir_contratos(contratos, legado, depara)
    destinos = resultado.groupBy("CPF").agg(F.collect_set("escritorio_destino_cod").alias("destinos")).collect()
    destinos_cpf555 = [row.destinos for row in destinos if row.CPF == "555"][0]
    assert destinos_cpf555 == ["E3"]


def test_quota_estourada_gera_pendente(spark):
    contratos = spark.createDataFrame(
        [
            ("666", "C10", "CarA", "Sul", 1, None, None, "lotado"),
            ("666", "C11", "CarA", "Sul", 1, None, None, "lotado"),
        ],
        _schema_contratos(),
    )
    legado = spark.createDataFrame([], _schema_legado())
    depara = spark.createDataFrame(
        [
            ("E1", "Alpha", "CarA", "Sul", True, 50.0, 1),
        ],
        _schema_depara(),
    )

    params = DistribuicaoParams(quota_minima=0)
    resultado, auditoria, resumo, pendentes, *_ = distribuir_contratos(
        contratos, legado, depara, params.__dict__
    )

    assert resultado.count() == 0
    assert pendentes.count() == 2
    motivos = {row.motivo for row in pendentes.collect()}
    assert motivos == {"quota_atingida"}


def test_colunas_extras_preservadas(spark):
    contratos = spark.createDataFrame(
        [
            ("777", "C12", "CarA", "Sul", 1, "E1", "Alpha", "comentario"),
        ],
        _schema_contratos(),
    )
    legado = spark.createDataFrame([], _schema_legado())
    depara = spark.createDataFrame(_base_depara_rows(), _schema_depara())

    resultado, auditoria, *_ = distribuir_contratos(contratos, legado, depara)

    assert "Observacao_livre" in resultado.columns
    assert "orig_Observacao_livre" in auditoria.columns
    assert set(contratos.columns).issubset(set(resultado.columns))
    assert resultado.columns[: len(contratos.columns)] == contratos.columns
    valor_resultado = resultado.select("Observacao_livre").collect()[0][0]
    valor_auditoria = auditoria.select("orig_Observacao_livre").collect()[0][0]
    assert valor_resultado == "comentario"
    assert valor_auditoria == "comentario"
