# Preimero_acesso

Este repositório contém o algoritmo de distribuição de contratos por escritório com as regras de Concentração → Fidelização → Meritocracia.

## Requisitos

- Python 3.10+
- Java Runtime (necessário para executar o PySpark)

Instale as dependências em um ambiente virtual e registre o projeto como pacote
local (garante que `import distribuicao_contratos` funcione em qualquer diretório):

```bash
python -m venv .venv
source .venv/bin/activate  # No Windows use: .venv\\Scripts\\activate
pip install -e .[dev]
```

## Como executar o algoritmo

1. Ajuste ou substitua os dados de exemplo em `scripts/exemplo.py` conforme a sua necessidade.
2. Execute o script para gerar as tabelas de saída (resultado, auditoria, resumo etc.):

```bash
python scripts/exemplo.py
```

As tabelas são exibidas no console por meio de `show()` e podem ser adaptadas para gravação em arquivos/parquet conforme a necessidade.

> Dica: se o ambiente corporativo não permitir `pip install -e .`, utilize `pip install -r requirements.txt` e adicione o diretório `src`
> ao `PYTHONPATH` manualmente (`export PYTHONPATH=$(pwd)/src:$PYTHONPATH`).

Para uso programático, importe a função principal diretamente:

```python
from pyspark.sql import SparkSession
from distribuicao_contratos import (
    DistribuicaoParams,
    carregar_bases_workspace,
    distribuir_contratos,
)

spark = SparkSession.builder.getOrCreate()
bases = carregar_bases_workspace(spark)

resultado, auditoria, resumo, pendentes, grupos_sem_depara, nao_concentrados, export = distribuir_contratos(
    bases["df_contratos"],
    bases["df_legado"],
    bases["df_depara"],
    params=DistribuicaoParams(tolerancia_pp=0.1),
)
```

O helper `carregar_bases_workspace` aplica as mesmas conversões sugeridas pela
equipe (datas com `pd.to_datetime`, percentuais como `float` e criação dos
DataFrames Spark) para os arquivos localizados em `/Workspace`:

| Arquivo Excel                           | Finalidade                                     |
| -------------------------------------- | ---------------------------------------------- |
| `base_contratos_distribuir.xlsx`       | Base de contratos a distribuir                 |
| `Base_legado_passado.xlsx`             | Histórico de concentração                      |
| `Depara_escri_aten_rastreador.xlsx`    | Escritórios aptos por Carteira/Região          |
| `Depara_rastreador.xlsx`               | Percentuais alvo para contratos com rastreador |
| `depara_sem_rastreador.xlsx`           | Percentuais alvo para contratos sem rastreador |

Caso os arquivos estejam em outro diretório, basta informar `workspace_dir`
no helper (`carregar_bases_workspace(spark, workspace_dir="/caminho" )`).

## Como rodar os testes

Os testes garantem os principais cenários de concentração e quota:

```bash
pytest -q
```

> Observação: caso esteja rodando em ambiente corporativo sem acesso à internet, certifique-se de que o pacote `pyspark` esteja instalado previamente no espelho interno da organização.

## Como subir as alterações no Git

Após validar o algoritmo localmente, utilize o fluxo padrão de Git para versionar e compartilhar as alterações. Exemplo:

```bash
git status                # verifique os arquivos modificados
git add <arquivos>        # inclua os arquivos relevantes no commit
git commit -m "mensagem"  # descreva objetivamente a mudança
git push origin <branch>  # envie a branch para o repositório remoto
```

Caso a organização utilize fluxo de Pull Request, abra a PR na plataforma de versionamento para revisão antes do merge.

