# Databricks notebook source
# pipeline/monitoring/freshness_check.py
# Vigia de atualização: roda FORA do pipeline diário, num job próprio às 07:00.
#
# Por que existe: o e-mail de falha só dispara para run que começa e falha.
# Sem este vigia, três cenários ficam em silêncio:
#   1. schedule pausado ou removido (ex.: um deploy que volta a PAUSED);
#   2. run que nunca começa (scheduler, cota do workspace);
#   3. run VERDE com dado velho: o extrator não fechou a partição do dia, a
#      Silver não encontra partição madura nova e termina sem erro.
# O check de timeliness do Silver QG não cobre 1 e 2 (roda dentro do job que
# não rodou) e, com tolerância de 2 dias, deixa passar um dia perdido.
#
# O que verifica (hora e data de negócio em America/Fortaleza):
#   A. Silver: cada watermark esperado existe uma vez, está COMMITTED, sem
#      candidate pendente, e committed >= hoje - MAX_LAG_DAYS.
#   B. Gold: cada fato foi reconstruído hoje. Conta só a reconstrução
#      (CREATE OR REPLACE ... AS SELECT); OPTIMIZE e VACUUM também escrevem no
#      histórico, mas não trazem dado novo e dariam um falso "atualizado".
# Qualquer falha levanta exceção -> o job falha -> e-mail on_failure.
#
# Limite aceito (common mode): mesmo workspace e mesmo scheduler do pipeline.
# Se o workspace inteiro parar, ou se este job for pausado, o silêncio volta.
# Só lê: nenhuma escrita em controle, Silver ou Gold.

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from pyspark.sql import functions as F


def job_param(nome: str, default: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        return default


def required_param(nome: str) -> str:
    """Parâmetro obrigatório do job: falha cedo em vez de cair num default de ambiente."""
    try:
        value = dbutils.widgets.get(nome)
    except Exception:
        value = ""
    if not value:
        raise ValueError(
            f"Parâmetro obrigatório ausente: '{nome}'. Execute via job do bundle, "
            "que injeta o catalog por target."
        )
    return value


CATALOG = required_param("catalog")

# Às 07:00 do dia T o normal é: facts com committed = T-1 (a partição de T-1 fecha
# por volta de 01:00 de T e o run das 03:00 a processa); SCD2 com committed = T ou T-1.
# 1 é o limiar que detecta UM run perdido. Com 2, o primeiro dia sem run passaria.
# Em teste, max_lag_days=0 força a falha e prova que o e-mail chega.
MAX_LAG_DAYS = int(job_param("max_lag_days", "1"))

BUSINESS_TZ = ZoneInfo("America/Fortaleza")
NOW = datetime.now(BUSINESS_TZ)
TODAY = NOW.date()
OLDEST_OK = TODAY - timedelta(days=MAX_LAG_DAYS)

# Conjuntos esperados explícitos: uma entidade que some da tabela de controle é
# falha, não "nada a verificar".
EXPECTED_WATERMARKS = {
    "fact_watermark": [
        "venda", "notaentrada", "notaentradaitem", "perda", "logestoque",
        "promocao", "promocaoitem", "pedido", "pedidoitem", "oferta",
        "pagarfornecedor", "pagarfornecedorparcela",
        "pagaroutrasdespesas", "pagaroutrasdespesasimposto",
    ],
    "scd2_watermark": ["produto", "fornecedor", "mercadologico"],
}
GOLD_FACTS = [
    "fato_vendas", "fato_compras", "fato_perdas", "fato_movimento_estoque",
    "fato_promocoes", "fato_oferta", "fato_contas_pagar",
    "fato_outras_despesas", "fato_curva_abc",
]

linhas = []
falhas = []


def registrar(nome: str, ok: bool, detalhe: str) -> None:
    linhas.append(f"{'✅' if ok else '❌'} {nome} {detalhe}")
    if not ok:
        falhas.append(f"{nome} {detalhe}")


# ── A. Silver: watermarks ─────────────────────────────────────────────────
for tabela, entidades in EXPECTED_WATERMARKS.items():
    por_entidade = {}
    for row in spark.table(f"{CATALOG}.control.{tabela}").collect():
        por_entidade.setdefault(row["entity"], []).append(row)

    for entidade in entidades:
        achados = por_entidade.get(entidade, [])
        if len(achados) != 1:
            registrar(f"{tabela}.{entidade}", False, f"(linhas: {len(achados)}; esperado: 1)")
            continue

        row = achados[0]
        committed = row["last_processed_snapshot"]
        atraso = (TODAY - committed).days if committed else None
        ok = (
            row["status"] == "COMMITTED"
            and row["candidate_snapshot"] is None
            and committed is not None
            and committed >= OLDEST_OK
        )
        registrar(
            f"{tabela}.{entidade}",
            ok,
            f"(committed: {committed} | atraso: {atraso}d | status: {row['status']} | "
            f"candidate: {row['candidate_snapshot']})",
        )

# ── B. Gold: fatos reconstruídos hoje ─────────────────────────────────────
for fato in GOLD_FACTS:
    historico = spark.sql(f"DESCRIBE HISTORY {CATALOG}.gold.{fato}")
    # unix_timestamp de uma coluna TIMESTAMP devolve o instante em segundos,
    # sem depender do timezone da sessão; a conversão para Fortaleza é explícita.
    epoch = (
        historico
        .filter(F.col("operation").endswith("TABLE AS SELECT"))
        .agg(F.max(F.unix_timestamp("timestamp")).alias("epoch"))
        .collect()[0]["epoch"]
    )
    if epoch is None:
        operacoes = sorted({r["operation"] for r in historico.select("operation").collect()})
        registrar(f"gold.{fato}", False, f"(nenhuma reconstrução no histórico; operações vistas: {operacoes})")
        continue

    reconstruida = datetime.fromtimestamp(epoch, BUSINESS_TZ)
    registrar(
        f"gold.{fato}",
        reconstruida.date() == TODAY,
        f"(última reconstrução: {reconstruida:%Y-%m-%d %H:%M})",
    )

# ── Resultado ─────────────────────────────────────────────────────────────
print(f"=== VIGIA DE ATUALIZAÇÃO — {CATALOG} ===")
print(
    f"Agora: {NOW:%Y-%m-%d %H:%M} (America/Fortaleza) | hoje: {TODAY} | "
    f"committed mínimo aceito: {OLDEST_OK} (max_lag_days={MAX_LAG_DAYS})\n"
)
for linha in linhas:
    print(linha)

if falhas:
    raise Exception(
        f"Dados desatualizados: {len(falhas)} de {len(linhas)} verificações falharam.\n"
        + "\n".join(falhas)
    )

print(f"\n✅ {len(linhas)}/{len(linhas)} verificações: Silver e Gold atualizadas.")
