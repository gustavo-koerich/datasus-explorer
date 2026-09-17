# Explorador de Procedimentos do SUS

Dashboard de análise de procedimentos hospitalares do SUS: filtre por procedimento (parto, joelho, apêndice, catarata...), ano, UF, município, diagnóstico, hospital, material e ato profissional — combináveis entre si — e veja KPIs, distribuição geográfica, diagnósticos associados e evolução temporal.

**[Ver dashboard ao vivo](https://gustavo-koerich.github.io/datasus-explorer/)**

## O que é

Um pipeline em Python baixa e cruza quatro bases públicas do governo brasileiro:

- **SIH/SUS** (DATASUS) — internações hospitalares (grupos RD e SP)
- **SIGTAP** — Tabela Unificada de Procedimentos, Medicamentos e OPM do SUS
- **CID-10** — códigos de diagnóstico
- **CNES** — cadastro nacional de estabelecimentos de saúde

O resultado é uma base tratada, com nomes legíveis (hospital, município, procedimento, diagnóstico) no lugar dos códigos brutos, servida por um dashboard estático com filtros combináveis em cascata (cada campo só mostra opções compatíveis com os demais filtros já aplicados), KPIs, gráficos e tabela — clicar numa barra de gráfico também filtra.

Atualmente cobre 25 procedimentos de 7 especialidades (obstetrícia, cirurgia geral, ortopedia, oftalmologia, otorrinolaringologia, ginecologia, cirurgia bariátrica), amostra de março/2026, todas as 27 UFs. A lista de procedimentos e o período são configuráveis — ver `extrator_sigtap_sus.py`.

## Como funciona o pipeline

`extrator_sigtap_sus.py`:
1. Para cada UF, baixa os grupos RD (internações) e SP (atos profissionais) do SIH/SUS via [PySUS](https://github.com/AlertaDengue/PySUS)
2. Filtra pelos códigos de procedimento configurados em `CODIGOS_PROCEDIMENTO_CIRURGICOS`
3. Cruza com SIGTAP (nome/valor do procedimento), CID-10 (nome do diagnóstico), CNES (nome do hospital) e a tabela de municípios do IBGE
4. Salva checkpoints por UF em `extracao_bruta/` — resumível, seguro interromper a qualquer momento
5. Consolida tudo em `base_procedimentos_sus.csv` e `base_profissionais_sus.csv`

`prepara_dashboard.py` converte essa base num JSON compacto consumido pelo dashboard estático em `docs/`: em vez de repetir texto, cada registro referencia índices em dicionários compartilhados (diagnóstico, município, hospital, procedimento, material, ato profissional) — permite filtros combináveis inteiramente no cliente, sem backend. Números de profissional (CPF) nunca são expostos: viram um id sequencial anônimo, usado só para contar profissionais distintos por filtro.

## Rodando localmente

```bash
pip install -r requirements.txt
```

Baixe o cadastro de estabelecimentos do [Portal de Dados Abertos do CNES](https://cnes.datasus.gov.br/pages/downloads/arquivosBaseDados.jsp) e salve em `dados_referencia/tbEstabelecimento.csv`.

```bash
python extrator_sigtap_sus.py    # gera as duas bases CSV (leva tempo: baixa dado real do DATASUS)
python prepara_dashboard.py      # gera docs/dashboard_data.json
```

Para servir o dashboard localmente:

```bash
cd docs
python -m http.server 8000
```

## Stack

Python (pandas, PySUS, dbfread) para o pipeline · HTML/CSS/JS puro para o dashboard, sem dependências de build.

## Licença

Código disponibilizado publicamente para fins de portfólio e demonstração técnica. Todos os direitos reservados — uso, cópia ou redistribuição requer autorização do autor.
