# Transformações — Varejinho Data Platform

> **Autora:** Zara Louise  
> **Versão:** 1.2 — Setembro 2026  
> **Repositório:** github.com/zaraluz/varejinho-data-platform

Este documento descreve as transformações aplicadas em cada camada da plataforma de dados do Grupo Varejinho, do dado bruto até o modelo dimensional consumido pelo Power BI.

---

## Princípios gerais

| Camada | Responsabilidade | Formato | Partição |
|---|---|---|---|
| **Bronze** | Espelho fiel da origem — aceita tudo, sem transformação | CSV (Pentaho Community sem Parquet nativo) | `ingestion_date` — quando o dado chegou |
| **Silver** | Limpeza, tipagem, deduplicação, contratos, SCD2 | Delta Lake | `ano`, `mes` — data de negócio |
| **Gold** | Modelagem dimensional — Star Schema para consumo analítico | Delta Lake | `ano`, `mes` — data de negócio |

**Regra fundamental:** Bronze particiona por quando o dado chegou (`ingestion_date`). Silver e Gold particionam por quando o evento aconteceu. Isso resolve o problema original: registros do último dia do mês eram perdidos quando o job virava de mês, porque a partição era por mês corrente.

**Cast de decimais:** o ERP grava decimais com vírgula (`3,14`). Toda coluna numérica passa por `regexp_replace(col, ',', '.')` antes do cast para `DECIMAL(14,3)`.

**Cast de timestamps:** o ERP grava datas no formato `yyyy/MM/dd HH:mm:ss.SSS`. Para colunas nullable usa-se `try_to_timestamp` para retornar NULL em vez de erro.

---

## Critério de seleção de colunas Silver → Gold

1. **Métricas** — colunas que serão somadas, agregadas ou comparadas
2. **Chaves** — necessárias para joins com dimensões e filtros no Power BI
3. **Atributos degenerados** — não justificam dimensão própria mas adicionam contexto analítico

**O que fica de fora:** colunas administrativas sem uso analítico, colunas de integração entre sistemas, IDs de workflow interno, `ingestion_date` (controle de pipeline).

---

## venda

### Bronze (`varejinho.bronze.venda`)

**Extração:** incremental com watermark em `data`. Janela de segurança de 2 dias.

| Coluna | Tipo Bronze | Observação |
|---|---|---|
| id | string | PK |
| id_loja | string | FK loja |
| id_produto | string | FK produto |
| data | string | Data da venda — formato `yyyy/MM/dd HH:mm:ss.SSS` |
| precovenda | string | Decimal brasileiro |
| quantidade | string | Decimal brasileiro |
| id_comprador | string | |
| custocomimposto | string | Decimal brasileiro |
| piscofins | string | Decimal brasileiro |
| operacional | string | Flag |
| icmscredito | string | Decimal brasileiro |
| icmsdebito | string | Decimal brasileiro |
| valortotal | string | Decimal brasileiro |
| custosemimposto | string | Decimal brasileiro |
| oferta | string | Flag — produto em oferta? |
| perda | string | Flag — venda gerou perda? |
| customediosemimposto | string | Decimal brasileiro |
| customediocomimposto | string | Decimal brasileiro |
| piscofinscredito | string | Decimal brasileiro |
| cupomfiscal | string | |
| id_tipooferta | string | FK tipooferta |
| ingestion_date | string | Partição — data de ingestão |

### Silver (`varejinho.silver.venda`)

| Transformação | Detalhe |
|---|---|
| Cast decimal | `valortotal`, `precovenda`, `quantidade`, `custocomimposto`, `custosemimposto`, `customediocomimposto`, `customediosemimposto`, `piscofins`, `piscofinscredito`, `icmscredito`, `icmsdebito` |
| Cast timestamp | `data` |
| Colunas derivadas | `ano = YEAR(data)`, `mes = MONTH(data)` |
| Chave de dedup | `id` |
| Partição | `ano`, `mes` |
| Contrato | `id` not_null, `data` not_null, `id_loja` not_null, `valor_total >= 0` |

### Gold (`varejinho.gold.fato_vendas`)

| Decisão | Detalhe |
|---|---|
| Grão | 1 linha por item vendido por transação |
| SK | `MD5(id \|\| id_loja)` |
| Join dim_produto | LEFT JOIN temporal: `venda.data >= valid_from AND venda.data < COALESCE(valid_to, '2999-12-31')` |
| Por que LEFT JOIN | Venda com produto sem correspondência fica com `sk_produto = NULL` — visível, não perdida |
| Por que join temporal | Venda de 2023 deve se ligar à versão do produto vigente em 2023 |
| Colunas excluídas | `id_comprador`, `cupomfiscal`, `id_tipooferta` |
| Partição | `ano`, `mes` |

---

## notaentrada

### Bronze (`varejinho.bronze.notaentrada`)

**Extração:** incremental com watermark em `dataentrada`. Janela de segurança de 2 dias.
**Colunas removidas no Pentaho:** `observacao` (text), `informacaocomplementar` (varchar 1000) — OutOfMemoryError.

| Coluna | Tipo Bronze | Observação |
|---|---|---|
| id | string | PK |
| id_loja | string | FK loja |
| numeronota | string | Número da NF — não único sozinho |
| id_fornecedor | string | FK fornecedor |
| dataentrada | string | Watermark — formato `yyyy/MM/dd HH:mm:ss.SSS` |
| id_tipoentrada | string | FK tipoentrada |
| dataemissao | string | Data de emissão da NF |
| datahoralancamento | string | Data/hora do lançamento no sistema |
| valoripi | string | Decimal brasileiro |
| valorfrete | string | Decimal brasileiro |
| valordesconto | string | Decimal brasileiro |
| valoroutradespesa | string | Decimal brasileiro |
| valordespesaadicional | string | Decimal brasileiro |
| valormercadoria | string | Decimal brasileiro |
| valortotal | string | Decimal brasileiro |
| valoricms | string | Decimal brasileiro |
| valoricmssubstituicao | string | Decimal brasileiro |
| id_usuario | string | Usuário que lançou |
| impressao | string | Flag |
| produtorrural | string | Flag |
| aplicacustodesconto | string | Flag |
| aplicaicmsdesconto | string | Flag |
| aplicacustoencargo | string | Flag |
| aplicaicmsencargo | string | Flag |
| aplicadespesaadicional | string | Flag |
| id_situacaonotaentrada | string | FK situacaonotaentrada |
| serie | string | Série da NF |
| valorguiasubstituicao | string | Decimal brasileiro |
| valorbasecalculo | string | Decimal brasileiro |
| aplicaaliquota | string | Flag |
| valorbasesubstituicao | string | Decimal brasileiro |
| valorfunrural | string | Decimal brasileiro |
| valordescontoboleto | string | Decimal brasileiro |
| chavenfe | string | Chave de acesso NF-e |
| conferido | string | Flag |
| id_tipofretenotafiscal | string | FK tipo frete |
| id_notasaida | string | FK nota de saída vinculada |
| id_tiponota | string | FK tipo nota |
| modelo | string | Modelo fiscal (55, 65, etc.) |
| liberadopedido | string | Flag |
| datahorafinalizacao | string | Data/hora de finalização |
| importadoxml | string | Flag — importado via XML NF-e |
| aplicaicmsipi | string | Flag |
| liberadobonificacao | string | Flag |
| valoricmssn | string | Decimal brasileiro |
| datahoraalteracao | string | Última alteração |
| liberadovencimento | string | Flag |
| justificativadivergencia | string | Texto |
| consistido | string | Flag |
| quantidadepaletes | string | |
| id_notadespesa | string | FK nota de despesa |
| valordespesafrete | string | Decimal brasileiro |
| liberadovalidadeproduto | string | Flag |
| valorfcp | string | Decimal brasileiro — Fundo de Combate à Pobreza |
| valorfcpst | string | Decimal brasileiro |
| valoricmsdesonerado | string | Decimal brasileiro |
| liberadodivergenciacoletor | string | Flag |
| valorsuframa | string | Decimal brasileiro |
| basecalculoicmsstretido | string | Decimal brasileiro |
| valoricmsstretido | string | Decimal brasileiro |
| basecalculofcpstretido | string | Decimal brasileiro |
| valorfcpstretido | string | Decimal brasileiro |
| aplicadescontoicmsdesonerado | string | Flag |
| valorguiafcpst | string | Decimal brasileiro |
| valoricmssubstitutoretido | string | Decimal brasileiro |
| justificativaliberacaodivergenciacoletor | string | Texto |
| justificativadivergenciacustoanterior | string | Texto |
| liberadodivergenciacustoanterior | string | Flag |
| valorseguro | string | Decimal brasileiro |
| valortotalbruto | string | Decimal brasileiro |
| valordescontofiscal | string | Decimal brasileiro |
| valoroutrasdespesasfiscal | string | Decimal brasileiro |
| aplicaicmsstencargo | string | Flag |
| aplicaicmsstdesconto | string | Flag |
| aplicaipivencimento | string | Flag |
| aplicaicmsstvencimento | string | Flag |
| valorfretefiscal | string | Decimal brasileiro |
| valoriss | string | Decimal brasileiro |
| valorservico | string | Decimal brasileiro |
| valorfunruralinss | string | Decimal brasileiro |
| valorfunruraloutrasentidades | string | Decimal brasileiro |
| valorfunruralsenar | string | Decimal brasileiro |
| liberadodivergenciadataemissao | string | Flag |
| aplicaipiicmsst | string | Flag |
| ingestion_date | string | Partição |

### Silver (`varejinho.silver.notaentrada`)

| Transformação | Detalhe |
|---|---|
| Cast decimal | `valortotal`, `valormercadoria`, `valordesconto` |
| Cast timestamp | `dataentrada` |
| Chave de dedup | `numeronota + id_loja + id_fornecedor` — fornecedores NFP-PRODUTOR reutilizam numeração entre lojas |
| Partição | `ano`, `mes` |

**Nota:** não tem fato correspondente na Gold ainda.

---

## notaentradaitem

### Bronze (`varejinho.bronze.notaentradaitem`)

**Extração:** subquery filtrando por `notaentrada.dataentrada` — sem coluna de data própria.
**Colunas removidas no Pentaho:** `descricaoxml` (varchar 120) — OutOfMemoryError.

| Coluna | Tipo Bronze | Observação |
|---|---|---|
| id | string | PK |
| id_notaentrada | string | FK notaentrada |
| id_produto | string | FK produto |
| quantidade | string | Decimal brasileiro |
| qtdembalagem | string | |
| valor | string | Decimal brasileiro — valor unitário |
| valortotal | string | Decimal brasileiro |
| valoripi | string | Decimal brasileiro |
| id_aliquota | string | FK aliquota |
| custocomimposto | string | Decimal brasileiro |
| valortotalfinal | string | Decimal brasileiro |
| valorbasecalculo | string | Decimal brasileiro |
| valoricms | string | Decimal brasileiro |
| valoricmssubstituicao | string | Decimal brasileiro |
| custocomimpostoanterior | string | Decimal brasileiro |
| valorbonificacao | string | Decimal brasileiro |
| valorverba | string | Decimal brasileiro |
| quantidadedevolvida | string | |
| valorpiscofins | string | Decimal brasileiro |
| valorbasesubstituicao | string | Decimal brasileiro |
| valorembalagem | string | Decimal brasileiro |
| cfop | string | Código Fiscal de Operações |
| valoricmssubstituicaoxml | string | Decimal brasileiro |
| valorisento | string | Decimal brasileiro |
| valoroutras | string | Decimal brasileiro |
| situacaotributaria | string | CST/CSOSN |
| valorfrete | string | Decimal brasileiro |
| valoroutrasdespesas | string | Decimal brasileiro |
| valordesconto | string | Decimal brasileiro |
| id_tipopiscofins | string | FK |
| id_aliquotacreditoforaestado | string | FK |
| id_aliquotapautafiscal | string | FK |
| id_tipoentrada | string | FK tipoentrada |
| valoroutrassubstituicao | string | Decimal brasileiro |
| quantidadebonificacao | string | |
| valorsubstituicaoestadual | string | Decimal brasileiro |
| valordespesafrete | string | Decimal brasileiro |
| cfopnota | string | CFOP da nota |
| valorbasefcp | string | Decimal brasileiro |
| valorfcp | string | Decimal brasileiro |
| valorbasefcpst | string | Decimal brasileiro |
| valorfcpst | string | Decimal brasileiro |
| valoricmsdesonerado | string | Decimal brasileiro |
| idmotivodesoneracao | string | |
| valorbasecalculoicmsdesonerado | string | Decimal brasileiro |
| valoricmsdiferido | string | Decimal brasileiro |
| basecalculoicmsstretido | string | Decimal brasileiro |
| porcentagemicmsstretido | string | Decimal brasileiro |
| valoricmsstretido | string | Decimal brasileiro |
| basecalculofcpstretido | string | Decimal brasileiro |
| porcentagemfcpstretido | string | Decimal brasileiro |
| valorfcpstretido | string | Decimal brasileiro |
| porcentagemfcp | string | Decimal brasileiro |
| porcentagemfcpst | string | Decimal brasileiro |
| valoricmsoperacao | string | Decimal brasileiro |
| codigobeneficio | string | Código de benefício fiscal |
| valoricmssubstitutoretido | string | Decimal brasileiro |
| valorseguro | string | Decimal brasileiro |
| valordescontofiscal | string | Decimal brasileiro |
| valortotalbruto | string | Decimal brasileiro |
| valoroutrasdespesasfiscal | string | Decimal brasileiro |
| custocontabil | string | Decimal brasileiro |
| utilizapautaespecialcarne | string | Flag |
| valorfretefiscal | string | Decimal brasileiro |
| valorbasecalculoiss | string | Decimal brasileiro |
| percentualaliquotaiss | string | Decimal brasileiro |
| valoriss | string | Decimal brasileiro |
| codigoservico | string | |
| valoricmsfronteira | string | Decimal brasileiro |
| valorbasecalculopiscofins | string | Decimal brasileiro |
| valorpis | string | Decimal brasileiro |
| valorcofins | string | Decimal brasileiro |
| valordespesaadicional | string | Decimal brasileiro |
| valorguiafcpst | string | Decimal brasileiro |
| valorguiasubstituicao | string | Decimal brasileiro |
| valoricmsmonofasico | string | Decimal brasileiro |
| valorbasecalculoicmsmonofasico | string | Decimal brasileiro |
| ingestion_date | string | Partição |

### Silver (`varejinho.silver.notaentradaitem`)

| Transformação | Detalhe |
|---|---|
| Cast decimal | `quantidade`, `valor`, `valortotal` |
| Chave de dedup | `id` |
| Sem partição | Sem coluna de data própria |

---

## perda

### Bronze (`varejinho.bronze.perda`)

**Extração:** incremental com watermark em `data`. Janela de segurança de 2 dias.
**Colunas removidas no Pentaho:** `observacao` (text) — OutOfMemoryError.

| Coluna | Tipo Bronze | Observação |
|---|---|---|
| id | string | PK |
| id_loja | string | FK loja |
| id_produto | string | FK produto |
| data | string | Watermark — formato `yyyy/MM/dd HH:mm:ss.SSS` |
| quantidade | string | Decimal brasileiro |
| custocomimposto | string | Decimal brasileiro |
| custosemimposto | string | Decimal brasileiro |
| id_tipopiscofins | string | FK |
| id_tipomotivoperda | string | FK tipomotivoperda |
| customediocomimposto | string | Decimal brasileiro |
| customediosemimposto | string | Decimal brasileiro |
| id_aliquota | string | FK aliquota |
| valoripi | string | Decimal brasileiro |
| valoricmssubstituicao | string | Decimal brasileiro |
| id_notasaida | string | FK nota de saída gerada |
| valorbasepiscofins | string | Decimal brasileiro |
| valorpis | string | Decimal brasileiro |
| valorcofins | string | Decimal brasileiro |
| emitenota | string | Flag — gera nota de saída? |
| ingestion_date | string | Partição |

### Silver (`varejinho.silver.perda`)

| Transformação | Detalhe |
|---|---|
| Cast decimal | `quantidade`, `valor`, `custocomimposto`, `custosemimposto`, `customediocomimposto`, `customediosemimposto`, `valorpis`, `valorcofins`, `valoripi`, `valoricmssubstituicao`, `valorbasepiscofins` |
| Cast timestamp | `data` |
| Chave de dedup | `id` |
| Partição | `ano`, `mes` |

### Gold (`varejinho.gold.fato_perdas`)

| Decisão | Detalhe |
|---|---|
| Grão | 1 linha por registro de perda |
| SK | `MD5(id \|\| id_loja)` |
| Join dim_produto | LEFT JOIN temporal |
| Colunas excluídas | `id_notasaida`, `emitenota`, `id_aliquota`, `id_tipopiscofins` |
| Partição | `ano`, `mes` |

---

## logestoque

### Bronze (`varejinho.bronze.logestoque`)

**Extração:** incremental com watermark em `datamovimento`. Janela de segurança de 2 dias.

| Coluna | Tipo Bronze | Observação |
|---|---|---|
| id | string | PK |
| id_loja | string | FK loja |
| id_produto | string | FK produto |
| quantidade | string | Decimal brasileiro |
| id_tipomovimentacao | string | FK tipomovimentacao |
| datahora | string | Data/hora do evento |
| id_usuario | string | Usuário que gerou |
| estoqueanterior | string | Decimal brasileiro |
| estoqueatual | string | Decimal brasileiro |
| id_tipoentradasaida | string | Entrada ou saída |
| custosemimposto | string | Decimal brasileiro |
| custocomimposto | string | Decimal brasileiro |
| datamovimento | string | Watermark — data da movimentação |
| customediocomimposto | string | Decimal brasileiro |
| customediosemimposto | string | Decimal brasileiro |
| id_venda | string | FK venda — se movimentação foi gerada por venda |
| ingestion_date | string | Partição |

### Silver (`varejinho.silver.logestoque`)

| Transformação | Detalhe |
|---|---|
| Cast decimal | `quantidade`, `estoqueanterior`, `estoqueatual`, `custocomimposto`, `custosemimposto`, `customediocomimposto`, `customediosemimposto` |
| Cast timestamp | `datamovimento` |
| Chave de dedup | `id` |
| Partição | `ano`, `mes` |
| Fix | Schema recriado via DROP TABLE + recreate — MERGE não altera tipo de colunas existentes |

### Gold (`varejinho.gold.fato_movimento_estoque`)

| Decisão | Detalhe |
|---|---|
| Grão | 1 linha por movimentação de estoque |
| SK | `MD5(id \|\| id_loja)` |
| Coluna calculada | `variacao_estoque = estoqueatual - estoqueanterior` |
| Join dim_produto | LEFT JOIN temporal |
| Colunas excluídas | `datahora`, `id_usuario` |
| Partição | `ano`, `mes` |

---

## promocao

### Bronze (`varejinho.bronze.promocao`)

**Extração:** FULL LOAD — promoções futuras já cadastradas precisam estar visíveis.

| Coluna | Tipo Bronze | Observação |
|---|---|---|
| id | string | PK |
| id_loja | string | FK loja |
| descricao | string | Nome da promoção |
| datainicio | string | Watermark — formato `yyyy/MM/dd HH:mm:ss.SSS` |
| datatermino | string | |
| pontuacao | string | |
| quantidade | string | Quantidade mínima |
| qtdcupom | string | |
| id_situacaocadastro | string | FK situacaocadastro |
| id_tipopromocao | string | FK tipopromocao |
| valor | string | Decimal brasileiro |
| controle | string | |
| id_tipopercentualvalor | string | FK |
| id_tipoquantidade | string | FK |
| aplicatodos | string | Flag |
| valordesconto | string | Decimal brasileiro |
| valorreferenteitenslista | string | Decimal brasileiro |
| verificaprodutosauditados | string | Flag |
| datalimiteresgatecupom | string | |
| id_tipopercentualvalordesconto | string | FK |
| valorpaga | string | Decimal brasileiro |
| desconsideraritem | string | Flag |
| qtdlimite | string | |
| somenteclubevantagens | string | Flag — só para clientes clube |
| diasexpiracao | string | |
| utilizaquantidadeproporcional | string | Flag |
| desconsideraprodutoemoferta | string | Flag |
| ingestion_date | string | Partição |

### Silver (`varejinho.silver.promocao`)

| Transformação | Detalhe |
|---|---|
| Cast decimal | `valor`, `valordesconto` |
| Cast timestamp | `datainicio` |
| Chave de dedup | `id` |
| Partição | `ano`, `mes` |

---

## promocaoitem

### Bronze (`varejinho.bronze.promocaoitem`)

**Extração:** FULL LOAD junto com `promocao`.

| Coluna | Tipo Bronze | Observação |
|---|---|---|
| id | string | PK |
| id_promocao | string | FK promocao |
| id_produto | string | FK produto |
| precovenda | string | Decimal brasileiro — preço promocional |
| ingestion_date | string | Partição |

### Silver (`varejinho.silver.promocaoitem`)

| Transformação | Detalhe |
|---|---|
| Cast decimal | `precovenda` |
| Chave de dedup | `id` |
| Sem partição | Sem coluna de data própria |

### Gold (`varejinho.gold.fato_promocoes`)

| Decisão | Detalhe |
|---|---|
| Grão | 1 linha por produto em promoção |
| SK | `MD5(id_promocaoitem \|\| id_loja)` |
| Join dim_produto | LEFT JOIN `is_current = true` — versão atual, não temporal |
| Colunas excluídas | `controle`, `verificaprodutosauditados`, `desconsideraritem`, `diasexpiracao` |
| Partição | `ano`, `mes` |

---

## oferta

### Bronze (`varejinho.bronze.oferta`)

**Extração:** FULL LOAD — ofertas futuras precisam estar visíveis.

| Coluna | Tipo Bronze | Observação |
|---|---|---|
| id | string | PK |
| id_loja | string | FK loja |
| id_produto | string | FK produto |
| datainicio | string | Formato `yyyy/MM/dd HH:mm:ss.SSS` |
| datatermino | string | |
| precooferta | string | Decimal brasileiro |
| preconormal | string | Decimal brasileiro |
| id_situacaooferta | string | FK |
| id_tipooferta | string | FK tipooferta |
| precoimediato | string | Decimal brasileiro — contém valores 'N' (flag booleana do ERP) |
| ofertafamilia | string | Flag |
| ofertaassociado | string | Flag |
| controle | string | |
| aplicapercentualprecoassociado | string | Flag |
| encerraoferta | string | Flag |
| encerraofertaitens | string | Flag |
| bloquearvenda | string | Flag |
| bloquearvendaitens | string | Flag |
| cashback | string | Flag |
| enviaconnect | string | Flag |
| ingestion_date | string | Partição |

### Silver (`varejinho.silver.oferta`)

| Transformação | Detalhe |
|---|---|
| Cast decimal | `precooferta`, `preconormal` |
| Cast especial | `precoimediato` via `try_cast` — valores 'N' retornam NULL |
| Cast timestamp | `datainicio` |
| Chave de dedup | `id` |
| Partição | `ano`, `mes` |

### Gold (`varejinho.gold.fato_oferta`)

| Decisão | Detalhe |
|---|---|
| Grão | 1 linha por produto em oferta por loja |
| SK | `MD5(id \|\| id_loja)` |
| Colunas calculadas | `desconto_valor = preconormal - precooferta`, `desconto_percentual` |
| Join dim_produto | LEFT JOIN temporal |
| Sinal analítico | `desconto_valor < 0` → margem negativa em oferta |
| Colunas excluídas | `controle`, `encerraofertaitens`, `bloquearvendaitens`, `enviaconnect`, `aplicapercentualprecoassociado` |
| Partição | `ano`, `mes` |

---

## pedido

### Bronze (`varejinho.bronze.pedido`)

**Extração:** incremental com watermark em `datacompra`. Janela de segurança de 2 dias.

| Coluna | Tipo Bronze | Observação |
|---|---|---|
| id | string | PK |
| id_loja | string | FK loja |
| id_fornecedor | string | FK fornecedor |
| id_tipofretepedido | string | FK |
| datacompra | string | Watermark |
| dataentrega | string | Prevista — pode conter datas absurdas do ERP |
| valortotal | string | Decimal brasileiro |
| id_situacaopedido | string | FK situacaopedido |
| desconto | string | Decimal brasileiro |
| id_comprador | string | |
| id_divisaofornecedor | string | FK |
| valordesconto | string | Decimal brasileiro |
| email | string | |
| id_tipoatendidopedido | string | FK |
| enviado | string | Flag |
| liberadodivergenciacomprador | string | Flag |
| liberadodivergenciafornecedor | string | Flag |
| liberadodivergenciamercadologico | string | Flag |
| gerousugestao | string | Flag |
| justificativapedidosemagenda | string | Texto |
| id_usuario | string | |
| valorfrete | string | Decimal brasileiro |
| liberadodivergenciaestoquemaximo | string | Flag |
| conferido | string | Flag |
| ingestion_date | string | Partição |

### Silver (`varejinho.silver.pedido`)

| Transformação | Detalhe |
|---|---|
| Cast timestamp | `datacompra` |
| Chave de dedup | `id` |
| Partição | `ano`, `mes` |

---

## pedidoitem

### Bronze (`varejinho.bronze.pedidoitem`)

**Extração:** subquery filtrando por `pedido.datacompra`.

| Coluna | Tipo Bronze | Observação |
|---|---|---|
| id | string | PK |
| id_loja | string | FK loja |
| id_pedido | string | FK pedido |
| id_produto | string | FK produto |
| quantidade | string | Decimal brasileiro |
| qtdembalagem | string | |
| custocompra | string | Decimal brasileiro |
| dataentrega | string | Pode conter datas absurdas (`0001/09/10`, `2202/12/30`) |
| desconto | string | Decimal brasileiro |
| valortotal | string | Decimal brasileiro |
| quantidadeatendida | string | Decimal brasileiro |
| id_tipopedido | string | FK tipopedido |
| custofinal | string | Decimal brasileiro |
| id_tipoatendidopedido | string | FK |
| troca | string | Flag |
| quantidadebonificadorebaixa | string | |
| valorfrete | string | Decimal brasileiro |
| custoverba | string | Decimal brasileiro |
| valorrebaixa | string | Decimal brasileiro |
| verbavalor | string | Decimal brasileiro |
| ingestion_date | string | Partição |

### Silver (`varejinho.silver.pedidoitem`)

| Transformação | Detalhe |
|---|---|
| Cast decimal | `quantidade`, `custocompra`, `valortotal` |
| Chave de dedup | `id` |
| Sem partição | Sem coluna de data própria |

### Gold (`varejinho.gold.fato_compras`)

| Decisão | Detalhe |
|---|---|
| Grão | 1 linha por item de pedido de compra |
| SK | `MD5(id_pedidoitem \|\| id_loja)` |
| Join pedido | INNER JOIN — todo item tem obrigatoriamente um cabeçalho |
| Join dim_produto | LEFT JOIN `is_current = true` |
| Join dim_fornecedor | LEFT JOIN `is_current = true` |
| Colunas excluídas | `email`, `id_usuario`, `gerousugestao`, `justificativapedidosemagenda` |
| Partição | `ano`, `mes` do `pedido` |

---

## pagarfornecedor

### Bronze (`varejinho.bronze.pagarfornecedor`)

**Extração:** incremental por `dataemissao`.

| Coluna | Tipo Bronze | Observação |
|---|---|---|
| id | string | PK |
| id_loja | string | FK loja |
| id_fornecedor | string | FK fornecedor |
| id_tipoentrada | string | FK tipoentrada |
| numerodocumento | string | |
| dataentrada | string | |
| dataemissao | string | Watermark |
| valor | string | Decimal brasileiro |
| id_notadespesa | string | FK — administrativo |
| id_notaentrada | string | FK notaentrada vinculada |
| id_transferenciaentrada | string | FK — administrativo |
| id_pagaroutrasdespesas | string | FK — administrativo |
| id_geracaoretencaotributo | string | FK — administrativo |
| id_escritasaldo | string | FK — administrativo |
| ingestion_date | string | Partição |

### Silver (`varejinho.silver.pagarfornecedor`)

| Transformação | Detalhe |
|---|---|
| Cast decimal | `valor` |
| Cast timestamp | `dataemissao` |
| Chave de dedup | `id` |
| Partição | `ano`, `mes` |

---

## pagarfornecedorparcela

### Bronze (`varejinho.bronze.pagarfornecedorparcela`)

**Extração:** FULL LOAD — extração incremental por `datavencimento` excluía parcelas com vencimento futuro.

| Coluna | Tipo Bronze | Observação |
|---|---|---|
| id | string | PK |
| id_pagarfornecedor | string | FK pagarfornecedor |
| numeroparcela | string | |
| datavencimento | string | Watermark lógico |
| datapagamento | string | Nullable — NULL para não pagas |
| valor | string | Decimal brasileiro |
| id_situacaopagarfornecedorparcela | string | FK situacao |
| id_tipopagamento | string | FK tipopagamento |
| datapagamentocontabil | string | Nullable |
| id_banco | string | FK banco |
| agencia | string | |
| conta | string | |
| numerocheque | string | |
| conferido | string | Flag |
| valoracrescimo | string | Decimal brasileiro |
| id_contacontabilfinanceiro | string | FK — contábil |
| id_conciliacaobancarialancamento | string | FK — conciliação bancária |
| exportado | string | Flag |
| datahoraalteracao | string | |
| id_lojabaixa | string | FK loja de baixa |
| id_favorecido | string | FK |
| ingestion_date | string | Partição |

### Silver (`varejinho.silver.pagarfornecedorparcela`)

| Transformação | Detalhe |
|---|---|
| Cast decimal | `valor`, `valoracrescimo` |
| Cast timestamp principal | `datavencimento` |
| Cast timestamps extras | `datapagamento`, `datapagamentocontabil` via `try_to_timestamp` — nullable |
| Chave de dedup | `id` |
| Partição | `ano`, `mes` do vencimento |

### Gold (`varejinho.gold.fato_contas_pagar`)

| Decisão | Detalhe |
|---|---|
| Grão | 1 linha por parcela de pagamento a fornecedor |
| SK | `MD5(id_parcela \|\| id_loja)` |
| Join cabeçalho | INNER JOIN com `pagarfornecedor` |
| Join dim_fornecedor | LEFT JOIN `is_current = true` |
| Colunas nullable | `sk_tempo_pagamento`, `datapagamento` — NULL para parcelas não pagas |
| Colunas excluídas | `id_conciliacaobancarialancamento`, `id_contacontabilfinanceiro`, `id_favorecido`, `exportado` |
| Partição | `ano`, `mes` do vencimento |

---

## pagaroutrasdespesas

### Bronze (`varejinho.bronze.pagaroutrasdespesas`)

**Extração:** incremental por `datahoraalteracao`.

| Coluna | Tipo Bronze | Observação |
|---|---|---|
| id | string | PK |
| id_fornecedor | string | FK fornecedor — nullable |
| numerodocumento | string | |
| id_tipoentrada | string | FK tipoentrada |
| dataemissao | string | Watermark lógico |
| dataentrada | string | |
| valor | string | Decimal brasileiro |
| id_situacaopagaroutrasdespesas | string | FK situacao |
| id_loja | string | FK loja |
| id_tipopiscofins | string | FK |
| datahoraalteracao | string | Watermark de extração |
| pendenciaworkflow | string | Flag de workflow interno |
| valorbruto | string | Decimal brasileiro |
| id_tiposervico | string | FK |
| id_abastecimento | string | FK |
| id_tipopagamento | string | FK tipopagamento |
| ingestion_date | string | Partição |

### Silver (`varejinho.silver.pagaroutrasdespesas`)

| Transformação | Detalhe |
|---|---|
| Cast decimal | `valor`, `valorbruto` |
| Cast timestamp | `dataemissao` |
| Chave de dedup | `id` |
| Partição | `ano`, `mes` |

### Gold (`varejinho.gold.fato_outras_despesas`)

| Decisão | Detalhe |
|---|---|
| Grão | 1 linha por despesa operacional |
| SK | `MD5(id \|\| id_loja)` |
| Join dim_fornecedor | LEFT JOIN `is_current = true` — nullable |
| Colunas excluídas | `pendenciaworkflow`, `id_abastecimento`, `id_tiposervico`, `datahoraalteracao` |
| Partição | `ano`, `mes` |

---

## produto (SCD Tipo 2)

### Bronze (`varejinho.bronze.produto`)

**Extração:** FULL LOAD diário.
**Colunas removidas no Pentaho:** `motivoisencaoanvisa` (text) — OutOfMemoryError.

| Coluna | Tipo Bronze | Observação |
|---|---|---|
| id | string | PK |
| descricaocompleta | string | Monitorada no SCD2 |
| qtdembalagem | string | |
| id_tipoembalagem | string | FK tipoembalagem |
| mercadologico1 | string | Seção — monitorada no SCD2 |
| mercadologico2 | string | Grupo — monitorada no SCD2 |
| mercadologico3 | string | Subgrupo — monitorada no SCD2 |
| mercadologico4 | string | |
| mercadologico5 | string | |
| id_comprador | string | |
| custofinal | string | Decimal brasileiro |
| id_familiaproduto | string | FK |
| descricaoreduzida | string | Monitorada no SCD2 |
| pesoliquido | string | Decimal brasileiro |
| datacadastro | string | Usado como `valid_from` no SCD2 |
| pesobruto | string | Decimal brasileiro |
| comprimentoembalagem | string | Decimal brasileiro |
| larguraembalagem | string | Decimal brasileiro |
| alturaembalagem | string | Decimal brasileiro |
| perda | string | Flag |
| verificacustotabela | string | Flag |
| percentualipi | string | Decimal brasileiro |
| percentualfrete | string | Decimal brasileiro |
| percentualencargo | string | Decimal brasileiro |
| percentualperda | string | Decimal brasileiro |
| percentualsubstituicao | string | Decimal brasileiro |
| descricaogondola | string | |
| dataalteracao | string | Última alteração no ERP |
| id_produtovasilhame | string | FK |
| id_tipomercadoria | string | FK tipomercadoria |
| sugestaopedido | string | Flag |
| aceitamultiplicacaopdv | string | Flag |
| id_fornecedorfabricante | string | FK fornecedor fabricante |
| id_divisaofornecedor | string | FK |
| id_tipopiscofins | string | FK |
| sazonal | string | Flag |
| consignado | string | Flag |
| ncm1 | string | NCM — monitorado no SCD2 |
| ncm2 | string | NCM secundário |
| ncm3 | string | NCM terciário |
| ddv | string | Dias de validade |
| permitetroca | string | Flag |
| temperatura | string | Temperatura de armazenamento |
| id_tipoorigemmercadoria | string | FK |
| ipi | string | Decimal brasileiro |
| pesavel | string | Flag |
| id_tipopiscofinscredito | string | FK |
| vendacontrolada | string | Flag |
| tiponaturezareceita | string | |
| vendapdv | string | Flag |
| conferido | string | Flag |
| permitequebra | string | Flag |
| permiteperda | string | Flag |
| codigoanp | string | Código ANP (combustíveis) |
| impostomedionacional | string | Decimal brasileiro |
| impostomedioimportado | string | Decimal brasileiro |
| sugestaocotacao | string | Flag |
| tara | string | Decimal brasileiro |
| utilizatabelasubstituicaotributaria | string | Flag |
| id_tipolocaltroca | string | FK |
| qtddiasminimovalidade | string | |
| utilizavalidadeentrada | string | Flag |
| impostomedioestadual | string | Decimal brasileiro |
| id_tipocompra | string | FK |
| numeroparcela | string | |
| id_tipoembalagemvolume | string | FK |
| volume | string | Decimal brasileiro |
| id_normacompra | string | FK |
| lastro | string | |
| camadas | string | |
| promocaoauditada | string | Flag |
| substituicaoestadual | string | Decimal brasileiro |
| substituicaoestadualoutros | string | Decimal brasileiro |
| substituicaoestadualexterior | string | Decimal brasileiro |
| id_cest | string | Código CEST |
| permitedescontopdv | string | Flag |
| verificapesopdv | string | Flag |
| id_servico | string | FK |
| descontomaximo | string | Decimal brasileiro |
| produtoecommerce | string | Flag |
| id_codigoanp | string | FK |
| id_tipoorigemmercadoriaentrada | string | FK |
| percentualtoleranciaselfcheckout | string | Decimal brasileiro |
| alteradopaf | string | Flag |
| produtoassessorado | string | Flag |
| excecaotipi | string | Flag |
| controlepoliciacivil | string | Flag |
| operacaoprodutoperfumariape | string | Flag |
| id_codigogia | string | FK |
| isentoanvisa | string | Flag |
| codigoanvisa | string | Código ANVISA |
| precomaximoconsumidoranvisa | string | Decimal brasileiro |
| produtoincentivado | string | Flag |
| cestabasica | string | Flag |
| desativarenviomasterfiscobrasil | string | Flag |
| id_marca | string | FK marca |
| ingestion_date | string | Partição |

### Silver (`varejinho.silver.produto`) — SCD Tipo 2

| Transformação | Detalhe |
|---|---|
| Colunas monitoradas | `descricaocompleta`, `descricaoreduzida`, `mercadologico1/2/3`, `ncm1` |
| Hash de versão | `MD5(concat_ws('\|\|', coalesce(col, '<NULL>')))` |
| valid_from | `datacadastro` do ERP — formato `yyyy/MM/dd HH:mm:ss.SSSSSSSSS` (9 dígitos) |
| valid_to | NULL na versão atual |
| is_current | TRUE na versão atual |
| Fix crítico | Primeira carga usava `current_timestamp()` causando 100% de `sk_produto NULL` nos fatos |

### Gold (`varejinho.gold.dim_produto`)

| Decisão | Detalhe |
|---|---|
| SK | `MD5(id \|\| valid_from)` — identifica unicamente cada versão |
| Colunas incluídas | `descricao_completa/reduzida`, `ncm`, hierarquia mercadológica, `id_tipoembalagem`, `id_tipomercadoria`, `datacadastro`, `dataalteracao`, controle SCD2 |
| Colunas excluídas | Atributos fiscais, flags operacionais, dimensões físicas de embalagem |

---

## fornecedor (SCD Tipo 2)

### Bronze (`varejinho.bronze.fornecedor`)

**Extração:** FULL LOAD diário.
**Colunas removidas no Pentaho:** `observacao` (varchar 2500) — OutOfMemoryError.

| Coluna | Tipo Bronze | Observação |
|---|---|---|
| id | string | PK |
| razaosocial | string | Monitorada no SCD2 |
| nomefantasia | string | Monitorada no SCD2 |
| endereco | string | |
| bairro | string | |
| id_municipio | string | FK |
| cep | string | |
| id_estado | string | FK estado |
| telefone | string | |
| id_tipoinscricao | string | FK |
| inscricaoestadual | string | |
| cnpj | string | Monitorado no SCD2 — receberá column mask na Gold |
| revenda | string | Flag |
| id_situacaocadastro | string | Monitorada no SCD2 |
| id_tipopagamento | string | FK tipopagamento |
| numerodoc | string | |
| pedidominimoqtd | string | |
| pedidominimovalor | string | Decimal brasileiro |
| serienf | string | |
| descontofunrural | string | Decimal brasileiro |
| senha | string | Operacional — não vai para Gold |
| id_tiporecebimento | string | FK |
| agencia | string | |
| digitoagencia | string | |
| conta | string | |
| digitoconta | string | |
| id_banco | string | FK banco |
| id_fornecedorfavorecido | string | FK |
| enderecocobranca | string | |
| bairrocobranca | string | |
| cepcobranca | string | |
| id_municipiocobranca | string | FK |
| id_estadocobranca | string | FK |
| bloqueado | string | Flag |
| id_tipomotivofornecedor | string | FK |
| datasintegra | string | |
| id_tipoempresa | string | FK |
| inscricaosuframa | string | |
| utilizaiva | string | Flag |
| id_familiafornecedor | string | FK |
| id_tipoinspecao | string | FK |
| numeroinspecao | string | |
| id_tipotroca | string | FK |
| id_tipofornecedor | string | FK tipofornecedor |
| id_contacontabilfinanceiro | string | FK contábil |
| utilizanfe | string | Flag |
| datacadastro | string | Usado como `valid_from` no SCD2 |
| utilizaconferencia | string | Flag |
| numero | string | |
| permitenfsempedido | string | Flag |
| modelonf | string | Modelo de NF |
| emitenf | string | Flag |
| tiponegociacao | string | |
| utilizacrossdocking | string | Flag |
| id_lojacrossdocking | string | FK |
| id_pais | string | FK |
| inscricaomunicipal | string | |
| id_contacontabilfiscalpassivo | string | FK contábil |
| numerocobranca | string | |
| complemento | string | |
| complementocobranca | string | |
| id_contacontabilfiscalativo | string | FK contábil |
| utilizaedi | string | Flag |
| tiporegravencimento | string | |
| nfemitidapostofiscal | string | Flag |
| id_tipoindicadorie | string | FK |
| utilizaprodepe | string | Flag |
| id_tiponegociacaocompra | string | FK |
| id_indicativocprb | string | FK |
| id_tipocustodevolucaotroca | string | FK |
| alteradopaf | string | Flag |
| cpfprodutorrural | string | CPF produtor rural |
| id_indicativosenar | string | FK |
| antecipacaopagamento | string | Flag |
| percentualcreditoicmssn | string | Decimal brasileiro |
| valormaximoverbapedido | string | Decimal brasileiro |
| codigofornecedorbalanca | string | |
| recalcularnotafiscal | string | Flag |
| id_tipocustocompra | string | FK |
| id_tipoentradapadrao | string | FK tipoentrada |
| id_tipoentradadespesapadrao | string | FK tipoentrada |
| documento | string | |
| id_classerisco | string | FK |
| bloqueadoautomatico | string | Flag |
| ingestion_date | string | Partição |

### Silver (`varejinho.silver.fornecedor`) — SCD Tipo 2

| Transformação | Detalhe |
|---|---|
| Colunas monitoradas | `razaosocial`, `nomefantasia`, `cnpj`, `id_situacaocadastro` |
| valid_from | `datacadastro` do ERP — formato `yyyy/MM/dd HH:mm:ss.SSS` |
| Hash de versão | `MD5(concat_ws('\|\|', coalesce(col, '<NULL>')))` |

### Gold (`varejinho.gold.dim_fornecedor`)

| Decisão | Detalhe |
|---|---|
| SK | `MD5(id \|\| valid_from)` |
| CNPJ | Incluído — column mask pendente |
| Colunas incluídas | `razao_social`, `nome_fantasia`, `cnpj`, controle SCD2 |
| Colunas excluídas | Dados bancários, configurações de negociação, flags operacionais, `senha` |

---

## mercadologico (SCD Tipo 2)

### Bronze (`varejinho.bronze.mercadologico`)

**Extração:** FULL LOAD diário.

| Coluna | Tipo Bronze | Observação |
|---|---|---|
| id | string | PK |
| mercadologico1 | string | Seção — monitorada no SCD2 |
| mercadologico2 | string | Grupo — monitorada no SCD2 |
| mercadologico3 | string | Subgrupo — monitorada no SCD2 |
| mercadologico4 | string | Nível 4 — não utilizado |
| mercadologico5 | string | Nível 5 — não utilizado |
| nivel | string | Monitorado no SCD2 |
| descricao | string | Monitorada no SCD2 |
| id_centrocusto | string | FK |
| descricaolojavirtual | string | |
| ingestion_date | string | Partição |

### Silver/Gold

| Transformação | Detalhe |
|---|---|
| Colunas monitoradas SCD2 | `descricao`, `mercadologico1/2/3`, `nivel` |
| valid_from | `DATE '2020-01-01'` — sem `datacadastro` no ERP |
| Colunas excluídas na Gold | `mercadologico4/5`, `id_centrocusto`, `descricaolojavirtual` |

---

## loja (SCD Tipo 1)

### Bronze (`varejinho.bronze.loja`)

**Extração:** FULL LOAD diário.

| Coluna | Tipo Bronze | Observação |
|---|---|---|
| id | string | PK |
| descricao | string | Nome da loja |
| id_fornecedor | string | FK — fornecedor vinculado à loja |
| id_situacaocadastro | string | FK situacaocadastro |
| nomeservidor | string | Operacional — servidor da loja |
| id_regiao | string | FK região |
| servidorcentral | string | Flag |
| geraconcentrador | string | Flag |
| estoqueterceiro | string | Flag |
| lojavirtual | string | Flag |
| atacado | string | Flag |
| ingestion_date | string | Partição |

### Silver/Gold

| Decisão | Detalhe |
|---|---|
| SK | `id_loja` — estável, sem SCD2 |
| Colunas incluídas | `descricao`, `id_regiao`, `lojavirtual`, `atacado` |
| Colunas excluídas | `id_fornecedor`, `nomeservidor`, `servidorcentral`, `geraconcentrador`, `estoqueterceiro`, `id_situacaocadastro` |

---

## dim_tempo (gerada por código)

Não extraída do ERP — gerada no Databricks via `sequence()`.

| Coluna | Tipo | Observação |
|---|---|---|
| sk_tempo | int | PK — YYYYMMDD |
| data | date | Data |
| ano | int | |
| mes | int | |
| dia | int | |
| trimestre | int | |
| semana_ano | int | |
| dia_semana_num | int | 1=Dom, 7=Sáb |
| nome_mes | string | January, February... |
| nome_dia_semana | string | Monday, Tuesday... |
| mes_ano | string | Jan/2026 |
| is_fim_semana | boolean | |
| estacao_sul | string | Verão/Outono/Inverno/Primavera |

**Cobertura:** 2023-01-01 a 2027-12-31

---

## curvaabc (fato snapshot)

### Bronze (`varejinho.bronze.curvaabc`)

**Extração:** FULL LOAD diário — classificação é recalculada pelo ERP.
**Decisão arquitetural:** reclassificada de dimensão para fato snapshot — acumula estados no tempo para permitir análise de migração de curva.

| Coluna | Tipo Bronze | Observação |
|---|---|---|
| id | string | PK |
| id_loja | string | FK loja |
| id_produto | string | FK produto |
| quantidade | string | Decimal brasileiro — quantidade vendida no período |
| valortotal | string | Decimal brasileiro — valor vendido |
| lucro | string | Decimal brasileiro |
| id_tipocurvaabc_nivel1 | string | Curva geral nível 1 (A/B/C) |
| id_tipocurvaabc_nivel2 | string | Curva geral nível 2 |
| id_tipocurvaabcmercadologico1_nivel1 | string | Curva por seção nível 1 |
| id_tipocurvaabcmercadologico1_nivel2 | string | Curva por seção nível 2 |
| id_tipocurvaabcmercadologico2_nivel1 | string | Curva por grupo nível 1 |
| id_tipocurvaabcmercadologico2_nivel2 | string | Curva por grupo nível 2 |
| id_tipocurvaabcmercadologico3_nivel1 | string | Curva por subgrupo nível 1 |
| id_tipocurvaabcmercadologico3_nivel2 | string | Curva por subgrupo nível 2 |
| id_tipocurvaabcmercadologico4_nivel1 | string | Curva por nível 4 nível 1 |
| id_tipocurvaabcmercadologico4_nivel2 | string | Curva por nível 4 nível 2 |
| id_tipocurvaabcmercadologico5_nivel1 | string | Curva por nível 5 nível 1 |
| id_tipocurvaabcmercadologico5_nivel2 | string | Curva por nível 5 nível 2 |
| ingestion_date | string | Partição |

### Silver (`varejinho.silver.curvaabc`)

| Transformação | Detalhe |
|---|---|
| Cast decimal | `quantidade`, `valortotal`, `lucro` |
| Cast int | Todas as colunas `id_tipocurvaabc*` |
| Coluna derivada | `snapshot_date = ingestion_date::date` |
| Estratégia | MERGE por `(id_produto, id_loja, snapshot_date)` — acumula todos os estados |
| Partição | `snapshot_date` |

### Gold (`varejinho.gold.fato_curva_abc`)

| Decisão | Detalhe |
|---|---|
| Grão | 1 linha por produto/loja/snapshot_date |
| SK | `MD5(id_produto \|\| id_loja \|\| snapshot_date)` |
| Join dim_produto | LEFT JOIN temporal |
| Partição | `snapshot_date` |

---

## Domínios (SCD Tipo 1)

FULL LOAD + overwrite diário. Sem fato correspondente na Gold.

| Tabela | Colunas Bronze | Linhas |
|---|---|---|
| `tipocurvaabc` | id, descricao, ingestion_date | 3 |
| `tipomotivoperda` | id, descricao, id_situacaocadastro, emitenota + 13 cols contábeis, ingestion_date | 22 |
| `tipopedido` | id, descricao, ingestion_date | 2 |
| `tipopromocao` | id, descricao, ingestion_date | 2 |
| `tipoembalagem` | id, descricao, descricaocompleta, ingestion_date | 16 |
| `tipoentrada` | id, descricao, tipo + 47 cols fiscais/contábeis, ingestion_date | 277 |
| `tipofornecedor` | id, descricao, ingestion_date | 4 |
| `tipomercadoria` | id, descricao, referencia, ingestion_date | 504 |
| `tipomovimentacao` | id, descricao, ingestion_date | 36 |
| `tipooferta` | id, descricao, id_situacaocadastro, prioridade, desconsiderarofertavendamedia, scanntech, ingestion_date | 53 |
| `tipopagamento` | id, descricao, banco, cheque, quantidadedias, boleto, docted, debitocc, ingestion_date | 11 |
| `tipoplanoconta` | id, planoconta1, planoconta2, nivel, descricao, ingestion_date | 19 |
| `situacaocadastro` | id, descricao, ingestion_date | 2 |
| `situacaonotaentrada` | id, descricao, ingestion_date | 2 |
| `situacaopagarfornecedorparcela` | id, descricao, ingestion_date | 2 |
| `situacaopagaroutrasdespesas` | id, descricao, ingestion_date | 2 |
| `situacaopedido` | id, descricao, ingestion_date | 3 |

---

## Limitações e decisões de ambiente

| Limitação | Impacto | Resolução |
|---|---|---|
| Pentaho Community sem Parquet Output | Bronze é CSV | `recursiveFileLookup=true` nas external tables |
| `bi_loja` read-only no PostgreSQL | Sem teste de registro retroativo real | Validado logicamente |
| Imports instáveis em serverless | Scripts `.py` não importáveis diretamente | Código inline nos notebooks; `.py` no repo para versionamento |
| SCD2 com `current_timestamp()` como `valid_from` | 100% de `sk_produto NULL` no `fato_vendas` | Reprocessado com `datacadastro` do ERP |
| `precoimediato` com valores 'N' na oferta | `CAST_INVALID_INPUT` | `try_cast` retorna NULL |
| Colunas `text` do PostgreSQL | OutOfMemoryError no Pentaho | Removidas da query de extração |
| MERGE não altera schema de colunas existentes | Colunas continuam com tipo errado após fix | DROP TABLE + recreate |
| Databricks Free Edition serverless only | Sem acesso direto à JVM | External Location via Unity Catalog com IAM Role |
