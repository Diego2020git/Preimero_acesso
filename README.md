# Preimero_acesso

Este repositório contém o algoritmo de distribuição de contratos por escritório com as regras de Concentração → Fidelização → Meritocracia.

## Requisitos

- Python 3.10+
- Java Runtime (necessário para executar o PySpark)

Instale as dependências em um ambiente virtual:

```bash
python -m venv .venv
source .venv/bin/activate  # No Windows use: .venv\\Scripts\\activate
pip install -r requirements.txt
```

## Como executar o algoritmo

1. Ajuste ou substitua os dados de exemplo em `scripts/exemplo.py` conforme a sua necessidade.
2. Execute o script para gerar as tabelas de saída (resultado, auditoria, resumo etc.):

```bash
python scripts/exemplo.py
```

As tabelas são exibidas no console por meio de `show()` e podem ser adaptadas para gravação em arquivos/parquet conforme a necessidade.

Para uso programático, importe a função principal diretamente:

```python
from distribuicao_contratos import distribuir_contratos, DistribuicaoParams

resultado, auditoria, resumo, pendentes, grupos_sem_depara, nao_concentrados, export = distribuir_contratos(
    df_contratos,
    df_legado,
    df_depara,
    params=DistribuicaoParams(tolerancia_pp=0.1),
)
```

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

