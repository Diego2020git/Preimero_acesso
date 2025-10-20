from pathlib import Path
import sys

import pandas as pd
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
            T.StructField("COD_escritorio", T.StringType(), True),
            T.StructField("Escritorio", T.StringType(), True),
            T.StructField("Observacao_livre", T.StringType(), True),
        ]
    )


def _contrato(
    cpf,
    numero,
    carteira,
    regiao,
    flag,
    escritorio_origem_cod=None,
    escritorio_origem=None,
    cod_escritorio=None,
    escritorio=None,
    observacao=None,
):
    return (
        cpf,
        numero,
        carteira,
        regiao,
        flag,
        escritorio_origem_cod,
        escritorio_origem,
        cod_escritorio,
        escritorio,
        observacao,
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
            _contrato("111", "C1", "CarA", "Sul", 1, observacao="obs"),
            _contrato("111", "C2", "CarA", "Sul", 1, observacao="obs"),
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
            _contrato("222", "C3", "CarA", "Sul", 1, observacao="info"),
            _contrato("222", "C4", "CarA", "Sul", 1, observacao="info"),
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
        [_contrato("333", "C5", "CarA", "Sul", 1, observacao="legado")],
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
            _contrato("444", "C6", "CarA", "Sul", 1, observacao="rastro"),
            _contrato("444", "C7", "CarA", "Sul", 0, observacao="sem_rastro"),
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
            _contrato("555", "C8", "CarA", "Sul", 0, observacao="multi"),
            _contrato("555", "C9", "CarB", "Norte", 0, observacao="multi"),
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
            _contrato("666", "C10", "CarA", "Sul", 1, observacao="lotado"),
            _contrato("666", "C11", "CarA", "Sul", 1, observacao="lotado"),
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
            _contrato(
                "777",
                "C12",
                "CarA",
                "Sul",
                1,
                escritorio_origem_cod="E1",
                escritorio_origem="Alpha",
                cod_escritorio="E1",
                escritorio="Alpha",
                observacao="comentario",
            ),
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


def test_origem_preenchida_a_partir_de_cod_escritorio(spark):
    contratos = spark.createDataFrame(
        [
            _contrato(
                "888",
                "C99",
                "CarA",
                "Sul",
                0,
                cod_escritorio="E1",
                escritorio="Alpha",
            ),
        ],
        _schema_contratos(),
    )
    legado = spark.createDataFrame([], _schema_legado())
    depara = spark.createDataFrame(_base_depara_rows(), _schema_depara())

    resultado, *_ = distribuir_contratos(contratos, legado, depara)

    linha = resultado.select("Escritorio_origem_cod", "Escritorio_origem").collect()[0]
    assert linha.Escritorio_origem_cod == "E1"
    assert linha.Escritorio_origem == "Alpha"


def test_depara_sem_coluna_atuacao(spark):
    contratos = spark.createDataFrame(
        [
            _contrato("999", "C20", "CarA", "Sul", 1, observacao="sem_flag"),
        ],
        _schema_contratos(),
    )
    legado = spark.createDataFrame([], _schema_legado())

    schema_sem_atuacao = T.StructType(
        [
            T.StructField("Escritorio_cod", T.StringType(), True),
            T.StructField("Escritorio_nome", T.StringType(), True),
            T.StructField("Carteira", T.StringType(), True),
            T.StructField("Regiao", T.StringType(), True),
            T.StructField("Quota_percentual", T.DoubleType(), True),
            T.StructField("Flag_rastreador", T.IntegerType(), True),
        ]
    )
    depara = spark.createDataFrame(
        [("E1", "Alpha", "CarA", "Sul", 100.0, 1)],
        schema_sem_atuacao,
    )

    resultado, *_ = distribuir_contratos(contratos, legado, depara)
    assert resultado.count() == 1


def test_percentual_zero_distribui_todos(spark):
    contratos = spark.createDataFrame(
        [
            _contrato("123", "C30", "CarA", "Sul", 1, observacao="sem_percentual"),
            _contrato("456", "C31", "CarA", "Sul", 0, observacao="sem_percentual"),
        ],
        _schema_contratos(),
    )
    legado = spark.createDataFrame([], _schema_legado())
    depara = spark.createDataFrame(
        [
            ("E1", "Alpha", "CarA", "Sul", True, 0.0, 1),
            ("E1", "Alpha", "CarA", "Sul", True, 0.0, 0),
            ("E2", "Beta", "CarA", "Sul", True, 0.0, 1),
            ("E2", "Beta", "CarA", "Sul", True, 0.0, 0),
        ],
        _schema_depara(),
    )

    resultado, *_ = distribuir_contratos(contratos, legado, depara)
    assert resultado.count() == contratos.count()


def test_colunas_com_acentos_e_espacos_sao_normalizadas(spark):
    contratos_pdf = pd.DataFrame(
        {
            "cpf": ["999"],
            "numero_do_contrato": ["C10"],
            "Carteira ": ["CarA "],
            "Região": ["Sul "],
            "Flag_Rastreador": [1],
            "cod_escritorio": ["E1"],
            "Escritorio": ["Alpha"],
        }
    )
    contratos = spark.createDataFrame(contratos_pdf)
    legado = spark.createDataFrame([], _schema_legado())

    depara_pdf = pd.DataFrame(
        {
            "COD_escritorio": ["E1"],
            "escritorio_nome": ["Alpha"],
            "Carteira": ["CarA"],
            "Região": ["Sul"],
            "atua_na_carteira_regiao": ["Sim"],
            "percentual": [100.0],
            "flag": [1],
        }
    )
    depara = spark.createDataFrame(depara_pdf)

    resultado, *_ = distribuir_contratos(contratos, legado, depara)

    assert resultado.count() == 1
    destino = resultado.select("escritorio_destino_cod").collect()[0][0]
    assert destino == "E1"
    assert resultado.select("Regiao").collect()[0][0] == "Sul"


def test_meritocracia_fallback_para_segundo_escritorio(spark):
    contratos = spark.createDataFrame(
        [
            _contrato("701", "M1", "CarA", "Sul", 0, observacao="merito"),
            _contrato("702", "M2", "CarA", "Sul", 0, observacao="merito"),
            _contrato("703", "M3", "CarA", "Sul", 0, observacao="merito"),
        ],
        _schema_contratos(),
    )
    legado = spark.createDataFrame([], _schema_legado())
    depara = spark.createDataFrame(
        [
            ("E1", "Alpha", "CarA", "Sul", True, 34.0, 0),
            ("E2", "Beta", "CarA", "Sul", True, 66.0, 0),
        ],
        _schema_depara(),
    )

    resultado, *_ = distribuir_contratos(contratos, legado, depara)

    resumo = resultado.groupBy("escritorio_destino_cod").agg(
        F.count("Numero_de_contrato").alias("qtd")
    )
    contagens = {row.escritorio_destino_cod: row.qtd for row in resumo.collect()}

    assert contagens.get("E2", 0) == 2
    assert contagens.get("E1", 0) == 1
