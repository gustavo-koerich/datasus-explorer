"""
Extrator nacional de dados publicos do DATASUS (SIH/SUS - grupos RD e SP),
filtrado por um conjunto configuravel de procedimentos cirurgicos do SIGTAP.

Este script é uma generalização de um extrator originalmente construído para
um único procedimento (aneurisma/endoprótese aórtica). A lógica de download,
cruzamento com SIGTAP/CID-10/município/CNES e o pipeline resumível por UF
são idênticos; a única mudança é que CODIGOS_PROCEDIMENTO agora cobre várias
especialidades, escolhidas por serem procedimentos amplamente conhecidos e
sem relação com nenhuma linha de produto/dispositivo médico específica --
o objetivo aqui é demonstrar o pipeline como ferramenta genérica de consulta
a procedimentos do SUS, não uma análise de mercado de um segmento.

Pipeline resumível: ao concluir uma UF, os resultados brutos filtrados são
salvos em extracao_bruta/{UF}_procedimentos.parquet e {UF}_profissionais.parquet.
Rodar o script de novo pula UFs já concluídas e retoma de onde parou -- é
seguro interromper (Ctrl+C, fechar o terminal, desligar o PC) a qualquer
momento.

Duas saídas finais:
  - base_procedimentos_sus.csv: uma linha por internação/AIH.
  - base_profissionais_sus.csv: uma linha por ato profissional ligado a
    essas internações (uma AIH pode ter vários profissionais/atos).
As duas se cruzam pela coluna N_AIH.
"""
import io
import os
import zipfile
from datetime import datetime
from ftplib import FTP
from pathlib import Path

import pandas as pd
from dbfread import DBF
from pysus.online_data.SIH import download

FTP_HOST = "ftp.datasus.gov.br"
FTP_DIR_TABELAS = "/dissemin/publicos/SIM/CID10/TABELAS"

# Tabela Unificada (SIGTAP: procedimentos, medicamentos e OPM do SUS),
# publicada mensalmente pelo Ministério da Saúde num FTP à parte, dedicado
# ao SIGTAP (não fica no ftp.datasus.gov.br nem é servida pelo PySUS).
FTP2_HOST = "ftp2.datasus.gov.br"
FTP2_DIR_TUP = "/pub/sistemas/tup/downloads"
SIGTAP_COMPETENCIA = "202606"

# Cadastro de estabelecimentos do CNES, baixado manualmente do portal de
# Dados Abertos do CNES (não vem por FTP nem pelo PySUS):
# https://cnes.datasus.gov.br/pages/downloads/arquivosBaseDados.jsp
# Baixe "Estabelecimentos" da competência desejada e salve neste caminho
# (ou ajuste a constante abaixo).
CNES_ESTABELECIMENTOS_CSV = (
    Path(__file__).parent / "dados_referencia" / "tbEstabelecimento.csv"
)

# Ordem de processamento das UFs -- SP primeiro por ter mais volume e
# validar o pipeline rápido; o resto segue em ordem alfabética.
ESTADOS = [
    "SP", "MG", "RJ", "BA", "PR", "RS", "PE", "CE", "PA", "SC", "GO", "MA",
    "PB", "ES", "AM", "RN", "AL", "PI", "MT", "DF", "MS", "SE", "RO", "TO",
    "AC", "AP", "RR",
]

# Amostra: 1 mês (o mais recente disponível), todas as UFs -- suficiente
# para validar o pipeline e gerar um dashboard de demonstração rapidamente.
# Para a extração completa, troque por algo como:
#   ANOS = list(range(2021, 2027)); MESES = list(range(1, 13))
ANOS = [2026]
MESES = [3]

# Procedimentos cirúrgicos (grupos SIGTAP 03/04), escolhidos por serem
# amplamente conhecidos e cobrirem várias especialidades distintas --
# nenhum tem relação com dispositivo/implante vascular.
CODIGOS_PROCEDIMENTO_CIRURGICOS = [
    # Obstetrícia -- parto e cesárea
    "0310010012",  # Assistência ao parto sem distócia
    "0310010039",  # Parto normal
    "0310010047",  # Parto normal em gestação de alto risco
    "0310010055",  # Parto normal em centro de parto normal (CPN)
    "0411010026",  # Operação cesariana em gestação de alto risco
    "0411010034",  # Operação cesariana
    "0411010042",  # Operação cesariana com laqueadura tubária
    # Cirurgia geral
    "0407020039",  # Apendicectomia
    "0407020047",  # Apendicectomia videolaparoscópica
    "0407030026",  # Colecistectomia
    "0407030034",  # Colecistectomia videolaparoscópica
    "0407040099",  # Hernioplastia inguinal (bilateral)
    "0407040102",  # Hernioplastia inguinal/crural (unilateral)
    "0407040129",  # Hernioplastia umbilical
    "0407010173",  # Gastroplastia com derivação intestinal (bariátrica)
    "0407010181",  # Gastroplastia vertical com banda (bariátrica)
    # Ortopedia -- próteses de quadril e joelho
    "0408040084",  # Artroplastia total primária do quadril cimentada
    "0408040092",  # Artroplastia total primária do quadril não cimentada/híbrida
    "0408040050",  # Artroplastia parcial de quadril
    "0408050063",  # Artroplastia total primária do joelho
    "0408050071",  # Artroplastia unicompartimental primária do joelho
    # Oftalmologia
    "0405050097",  # Facectomia com implante de lente intra-ocular (catarata)
    # Otorrinolaringologia
    "0404010024",  # Amigdalectomia
    "0404010032",  # Amigdalectomia com adenoidectomia
    # Ginecologia
    "0409060100",  # Histerectomia (por via vaginal)
    "0409060119",  # Histerectomia c/ anexectomia (uni/bilateral)
    "0409060135",  # Histerectomia total
]

# Materiais/OPM (grupo SIGTAP 07): só as próteses de quadril têm um código
# de material claramente identificável na Tabela Unificada; os demais
# procedimentos desta lista não usam implante (parto, apendicectomia etc.)
# ou têm o material embutido no próprio código do procedimento (joelho).
CODIGOS_PROCEDIMENTO_MATERIAIS = [
    "0702030139",  # Componente cefálico p/ artroplastia total do quadril (inclui prótese)
    "0702031224",  # Prótese parcial de quadril cimentada monobloco (tipo Thompson)
]

CODIGOS_PROCEDIMENTO = CODIGOS_PROCEDIMENTO_CIRURGICOS + CODIGOS_PROCEDIMENTO_MATERIAIS

# Classificação legível do material/OPM, quando aplicável. Procedimentos
# sem implante (maioria da lista) não entram aqui -- a coluna "Material"
# fica vazia para eles, o que é o resultado clinicamente correto.
MATERIAL_POR_CODIGO = {
    "0702030139": "Prótese de quadril (componente cefálico)",
    "0702031224": "Prótese parcial de quadril cimentada (Thompson)",
}

RENOMEIA_COLUNAS_RD = {
    "DIAG_PRINC": "Diagnóstico_CID",
    "MUNIC_RES": "Código_Município",
    "MUNIC_MOV": "Código_Município_Cirurgia",
    "CNES": "Código_Hospital",
    "PROC_REA": "Tratamento_Cod",
    "VAL_TOT": "Valor_Total",
    "N_AIH": "N_AIH",
}
COLUNAS_RD_BRUTAS = list(RENOMEIA_COLUNAS_RD.values()) + ["UF", "Ano", "Mês", "Dia"]
COLUNAS_SP_BRUTAS = [
    "N_AIH",
    "Profissional_Doc",
    "Profissional_CBO_Cod",
    "Ato_Profissional_Cod",
    "Valor_Ato",
    "UF",
    "Ano",
    "Mês",
]

PASTA_BRUTOS = Path("extracao_bruta")
PASTA_BRUTOS.mkdir(exist_ok=True)

ARQUIVO_SAIDA_PROCEDIMENTOS = "base_procedimentos_sus.csv"
ARQUIVO_SAIDA_PROFISSIONAIS = "base_profissionais_sus.csv"

# Progresso: ARQUIVO_PROGRESSO_ATUAL mostra sempre o estado mais recente
# (sobrescrito a cada mês processado) -- abra no Bloco de Notas a qualquer
# momento para ver onde a extração está. ARQUIVO_PROGRESSO_HISTORICO guarda
# uma linha por UF concluída, com os totais encontrados nela.
ARQUIVO_PROGRESSO_ATUAL = Path("progresso_extracao.txt")
ARQUIVO_PROGRESSO_HISTORICO = Path("progresso_extracao_historico.csv")

DOC_NAO_INFORMADO = "0" * 15


def _agora() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _atualiza_progresso_atual(
    uf: str, ano: int, mes: int, indice_uf: int, total_ufs: int,
    procedimentos_uf: int, atos_uf: int,
) -> None:
    texto = (
        f"Última atualização: {_agora()}\n"
        f"UF atual: {uf} ({indice_uf}/{total_ufs})\n"
        f"Competência sendo processada: {ano}-{mes:02d}\n"
        f"Procedimentos encontrados nesta UF até agora: {procedimentos_uf}\n"
        f"Atos profissionais encontrados nesta UF até agora: {atos_uf}\n"
    )
    ARQUIVO_PROGRESSO_ATUAL.write_text(texto, encoding="utf-8")


def _registra_uf_concluida(uf: str, procedimentos: int, atos: int) -> None:
    novo = not ARQUIVO_PROGRESSO_HISTORICO.exists()
    with open(ARQUIVO_PROGRESSO_HISTORICO, "a", encoding="utf-8-sig", newline="") as f:
        if novo:
            f.write("Timestamp;UF;Procedimentos;Atos_Profissionais\n")
        f.write(f"{_agora()};{uf};{procedimentos};{atos}\n")


def baixa_e_filtra_rd_mes(uf: str, ano: int, mes: int) -> pd.DataFrame:
    parquet_set = download(states=uf, years=ano, months=mes, groups="RD")
    df = parquet_set.to_dataframe()

    df["PROC_REA"] = df["PROC_REA"].astype(str)
    df = df[df["PROC_REA"].isin(CODIGOS_PROCEDIMENTO)].copy()

    if df.empty:
        return pd.DataFrame(columns=COLUNAS_RD_BRUTAS)

    df["VAL_TOT"] = df["VAL_TOT"].astype(str).str.strip().astype(float)
    df["N_AIH"] = df["N_AIH"].astype(str).str.strip()
    df["DT_INTER"] = pd.to_datetime(df["DT_INTER"], format="%Y%m%d")
    df["Ano"] = df["DT_INTER"].dt.year
    df["Mês"] = df["DT_INTER"].dt.month
    df["Dia"] = df["DT_INTER"].dt.day
    df["UF"] = uf

    df = df.rename(columns=RENOMEIA_COLUNAS_RD)
    return df[COLUNAS_RD_BRUTAS]


def baixa_e_filtra_sp_mes(uf: str, ano: int, mes: int, naihs_alvo: set) -> pd.DataFrame:
    """Baixa o grupo SP (Serviços Profissionais) e mantém só os atos ligados
    às AIHs já filtradas no RD (naihs_alvo), evitando carregar as (muitas)
    linhas de atos não relacionados aos procedimentos selecionados."""
    if not naihs_alvo:
        return pd.DataFrame(columns=COLUNAS_SP_BRUTAS)

    parquet_set = download(states=uf, years=ano, months=mes, groups="SP")
    df = parquet_set.to_dataframe()

    df["SP_NAIH"] = df["SP_NAIH"].astype(str).str.strip()
    df = df[df["SP_NAIH"].isin(naihs_alvo)].copy()

    if df.empty:
        return pd.DataFrame(columns=COLUNAS_SP_BRUTAS)

    df["SP_VALATO"] = df["SP_VALATO"].astype(str).str.strip().astype(float)
    df["Profissional_Doc"] = df["SP_PF_DOC"].astype(str).str.strip()
    df["Profissional_CBO_Cod"] = df["SP_PF_CBO"].astype(str).str.strip()
    df["Ato_Profissional_Cod"] = df["SP_ATOPROF"].astype(str).str.strip()
    df["UF"] = uf
    df["Ano"] = ano
    df["Mês"] = mes

    df = df.rename(columns={"SP_NAIH": "N_AIH", "SP_VALATO": "Valor_Ato"})
    return df[COLUNAS_SP_BRUTAS]


def processa_uf(uf: str, indice_uf: int = 1, total_ufs: int = 1) -> None:
    caminho_procedimentos = PASTA_BRUTOS / f"{uf}_procedimentos.parquet"
    caminho_profissionais = PASTA_BRUTOS / f"{uf}_profissionais.parquet"

    if caminho_procedimentos.exists() and caminho_profissionais.exists():
        print(f"[{uf}] já processado anteriormente, pulando.")
        return

    procedimentos_uf = []
    profissionais_uf = []

    for ano in ANOS:
        for mes in MESES:
            _atualiza_progresso_atual(
                uf, ano, mes, indice_uf, total_ufs,
                sum(len(d) for d in procedimentos_uf),
                sum(len(d) for d in profissionais_uf),
            )
            try:
                df_rd = baixa_e_filtra_rd_mes(uf, ano, mes)
            except Exception as exc:
                print(f"[{uf}] {ano}-{mes:02d}: falha ao baixar RD ({exc})")
                continue

            if df_rd.empty:
                continue

            print(f"[{uf}] {ano}-{mes:02d}: {len(df_rd)} procedimentos")
            procedimentos_uf.append(df_rd)

            naihs = set(df_rd["N_AIH"])
            try:
                df_sp = baixa_e_filtra_sp_mes(uf, ano, mes, naihs)
            except Exception as exc:
                print(f"[{uf}] {ano}-{mes:02d}: falha ao baixar SP ({exc})")
                continue

            if not df_sp.empty:
                profissionais_uf.append(df_sp)

    df_procedimentos_uf = (
        pd.concat(procedimentos_uf, ignore_index=True)
        if procedimentos_uf
        else pd.DataFrame(columns=COLUNAS_RD_BRUTAS)
    )
    df_profissionais_uf = (
        pd.concat(profissionais_uf, ignore_index=True)
        if profissionais_uf
        else pd.DataFrame(columns=COLUNAS_SP_BRUTAS)
    )

    df_procedimentos_uf.to_parquet(caminho_procedimentos, index=False)
    df_profissionais_uf.to_parquet(caminho_profissionais, index=False)
    print(
        f"[{uf}] concluído: {len(df_procedimentos_uf)} procedimentos, "
        f"{len(df_profissionais_uf)} atos profissionais"
    )
    _registra_uf_concluida(uf, len(df_procedimentos_uf), len(df_profissionais_uf))


def _baixa_dbf_datasus(nome_arquivo: str) -> pd.DataFrame:
    """
    Baixa uma tabela .DBF de /dissemin/publicos/SIM/CID10/TABELAS no FTP do
    DATASUS e a decodifica como cp850 (DOS Latin US), o encoding real usado
    nesses arquivos legados. As funções equivalentes em pysus.online_data.SIM
    (get_CID10_table/get_municipios) usam iso-8859-1, que corrompe caracteres
    acentuados (ex.: "Ribeirão" vira "RibeirÆo").
    """
    ftp = FTP(FTP_HOST)
    ftp.login()
    ftp.cwd(FTP_DIR_TABELAS)
    with open(nome_arquivo, "wb") as f:
        ftp.retrbinary(f"RETR {nome_arquivo}", f.write)
    ftp.quit()

    dbf = DBF(nome_arquivo, encoding="cp850")
    df = pd.DataFrame(list(dbf))
    os.unlink(nome_arquivo)
    return df


def carrega_tabela_cid10() -> pd.DataFrame:
    """CID-10 -> descrição da doença (tabela oficial DATASUS/SIM)."""
    cid = _baixa_dbf_datasus("CID10.DBF")[["CID10", "DESCR"]].copy()
    cid["CID10"] = cid["CID10"].str.strip()
    cid["DESCR"] = cid["DESCR"].str.strip()
    return cid.rename(columns={"CID10": "Diagnóstico_CID", "DESCR": "Diagnóstico_Nome"})


def carrega_tabela_municipios() -> pd.DataFrame:
    """Código IBGE (6 dígitos, formato DATASUS) -> nome do município."""
    mun = _baixa_dbf_datasus("CADMUN.DBF")[["MUNCOD", "MUNNOME"]].copy()
    mun["MUNCOD"] = mun["MUNCOD"].str.strip()
    mun["MUNNOME"] = mun["MUNNOME"].str.strip()
    return mun.rename(columns={"MUNCOD": "Código_Município", "MUNNOME": "Município_Nome"})


def carrega_tabela_hospitais(codigos_cnes: set) -> pd.DataFrame:
    """
    Código CNES -> nome do estabelecimento, a partir do cadastro completo do
    CNES (tbEstabelecimento, baixado manualmente do portal de Dados Abertos
    do CNES). Usa NO_FANTASIA e cai para NO_RAZAO_SOCIAL quando o nome
    fantasia está vazio. Lido em blocos para não carregar o arquivo
    (centenas de MB) inteiro na memória.
    """
    linhas = []
    leitor = pd.read_csv(
        CNES_ESTABELECIMENTOS_CSV,
        sep=";",
        encoding="cp1252",
        dtype=str,
        usecols=["CO_CNES", "NO_RAZAO_SOCIAL", "NO_FANTASIA"],
        chunksize=200_000,
    )
    for bloco in leitor:
        bloco["CO_CNES"] = bloco["CO_CNES"].str.strip()
        linhas.append(bloco[bloco["CO_CNES"].isin(codigos_cnes)])

    hosp = pd.concat(linhas, ignore_index=True)
    hosp["NO_FANTASIA"] = hosp["NO_FANTASIA"].str.strip()
    hosp["NO_RAZAO_SOCIAL"] = hosp["NO_RAZAO_SOCIAL"].str.strip()
    hosp["Hospital_Nome"] = hosp["NO_FANTASIA"].where(
        hosp["NO_FANTASIA"].notna() & (hosp["NO_FANTASIA"] != ""),
        hosp["NO_RAZAO_SOCIAL"],
    )
    hosp = hosp.rename(columns={"CO_CNES": "Código_Hospital"})
    return hosp[["Código_Hospital", "Hospital_Nome"]].drop_duplicates("Código_Hospital")


def carrega_tabela_sigtap() -> pd.DataFrame:
    """
    Código do procedimento/material/ato profissional (grupo SIGTAP 03/04/07)
    -> nome oficial e valores de referência SUS (VL_SH: serviço
    hospitalar/material, VL_SP: serviço profissional), a partir da Tabela
    Unificada (SIGTAP) publicada mensalmente em ftp2.datasus.gov.br. Layout
    de largura fixa, documentado em tb_procedimento_layout.txt dentro do
    próprio pacote.
    """
    ftp = FTP(FTP2_HOST)
    ftp.login()
    arquivos = ftp.nlst(FTP2_DIR_TUP)
    prefixo = f"{FTP2_DIR_TUP}/TabelaUnificada_{SIGTAP_COMPETENCIA}_"
    [nome_zip] = [f for f in arquivos if f.startswith(prefixo) and f.endswith(".zip")]

    buf = io.BytesIO()
    ftp.retrbinary(f"RETR {nome_zip}", buf.write)
    ftp.quit()
    buf.seek(0)

    with zipfile.ZipFile(buf) as z:
        raw = z.read("tb_procedimento.txt").decode("latin1")

    colspecs = [(0, 10), (10, 260), (282, 294), (306, 318)]
    nomes = ["Tratamento_Cod", "Tratamento_Nome", "VL_SH", "VL_SP"]
    sigtap = pd.read_fwf(io.StringIO(raw), colspecs=colspecs, names=nomes, dtype=str)

    sigtap["Tratamento_Cod"] = sigtap["Tratamento_Cod"].str.strip()
    sigtap["Tratamento_Nome"] = sigtap["Tratamento_Nome"].str.strip()
    sigtap["Valor_Referencia_Hospitalar"] = (
        pd.to_numeric(sigtap["VL_SH"], errors="coerce") / 100.0
    )
    sigtap["Valor_Referencia_Profissional"] = (
        pd.to_numeric(sigtap["VL_SP"], errors="coerce") / 100.0
    )
    return sigtap[
        [
            "Tratamento_Cod",
            "Tratamento_Nome",
            "Valor_Referencia_Hospitalar",
            "Valor_Referencia_Profissional",
        ]
    ]


def extrai_material_real(profissionais_bruto: pd.DataFrame) -> pd.DataFrame:
    """
    O material/OPM (grupo SIGTAP 07) usado numa cirurgia costuma aparecer
    como um "ato profissional" próprio dentro da mesma AIH, no grupo SP --
    com o valor específico que foi pago por aquele item naquela cirurgia.
    Isso é bem mais preciso do que inferir o material apenas a partir do
    procedimento cirúrgico da RD. Quando uma AIH tem mais de um item de
    material faturado, mantém o de maior valor (o principal). Só se aplica
    aos procedimentos com material rastreável (hoje: prótese de quadril) --
    a maioria dos procedimentos desta lista não tem OPM associado.
    """
    colunas = ["N_AIH", "Material_Cod_Real", "Material_Real", "Valor_Material_Real"]
    mat = profissionais_bruto[
        profissionais_bruto["Ato_Profissional_Cod"].isin(CODIGOS_PROCEDIMENTO_MATERIAIS)
    ].copy()
    if mat.empty:
        return pd.DataFrame(columns=colunas)

    mat["Material_Real"] = mat["Ato_Profissional_Cod"].map(MATERIAL_POR_CODIGO)
    mat = mat.sort_values("Valor_Ato", ascending=False).drop_duplicates("N_AIH")
    mat = mat.rename(
        columns={"Ato_Profissional_Cod": "Material_Cod_Real", "Valor_Ato": "Valor_Material_Real"}
    )
    return mat[colunas]


def enriquece_procedimentos(
    df: pd.DataFrame, sigtap: pd.DataFrame, profissionais_bruto: pd.DataFrame
) -> pd.DataFrame:
    """
    Adiciona colunas de nome (Diagnóstico_Nome, Município_Nome, Hospital_Nome,
    Tratamento_Nome) e de valor de referência SUS ao lado dos códigos
    originais, usando as tabelas oficiais do DATASUS/CNES/SIGTAP. O material
    é o real (extrai_material_real, via grupo SP) quando disponível; a
    maioria dos procedimentos simplesmente não tem material associado.
    """
    df["Diagnóstico_CID"] = df["Diagnóstico_CID"].astype(str).str.strip()
    df["Código_Município"] = df["Código_Município"].astype(str).str.strip()
    df["Código_Município_Cirurgia"] = df["Código_Município_Cirurgia"].astype(str).str.strip()
    df["Código_Hospital"] = df["Código_Hospital"].astype(str).str.strip()
    df["Tratamento_Cod"] = df["Tratamento_Cod"].astype(str).str.strip()

    cid = carrega_tabela_cid10()
    mun = carrega_tabela_municipios()
    mun_cirurgia = mun.rename(
        columns={
            "Código_Município": "Código_Município_Cirurgia",
            "Município_Nome": "Município_Cirurgia_Nome",
        }
    )
    hosp = carrega_tabela_hospitais(set(df["Código_Hospital"].unique()))
    material_real = extrai_material_real(profissionais_bruto)

    df = df.merge(cid, on="Diagnóstico_CID", how="left")
    df = df.merge(mun, on="Código_Município", how="left")
    df = df.merge(mun_cirurgia, on="Código_Município_Cirurgia", how="left")
    df = df.merge(hosp, on="Código_Hospital", how="left")
    df = df.merge(sigtap, on="Tratamento_Cod", how="left")
    df = df.merge(material_real, on="N_AIH", how="left")

    df["Material"] = df["Material_Real"].fillna(df["Tratamento_Cod"].map(MATERIAL_POR_CODIGO))

    colunas_finais = [
        "N_AIH",
        "UF",
        "Diagnóstico_CID",
        "Diagnóstico_Nome",
        "Código_Município",
        "Município_Nome",
        "Código_Município_Cirurgia",
        "Município_Cirurgia_Nome",
        "Código_Hospital",
        "Hospital_Nome",
        "Tratamento_Cod",
        "Tratamento_Nome",
        "Material",
        "Material_Cod_Real",
        "Valor_Material_Real",
        "Valor_Referencia_Hospitalar",
        "Valor_Referencia_Profissional",
        "Valor_Total",
        "Ano",
        "Mês",
        "Dia",
    ]
    return df[colunas_finais]


def enriquece_profissionais(df: pd.DataFrame, sigtap: pd.DataFrame) -> pd.DataFrame:
    """Adiciona o nome do ato profissional (mesma tabela SIGTAP) e sinaliza
    quando o documento do profissional não veio preenchido na fonte."""
    df["N_AIH"] = df["N_AIH"].astype(str).str.strip()
    df["Ato_Profissional_Cod"] = df["Ato_Profissional_Cod"].astype(str).str.strip()

    ato = sigtap.rename(
        columns={
            "Tratamento_Cod": "Ato_Profissional_Cod",
            "Tratamento_Nome": "Ato_Profissional_Nome",
        }
    )[["Ato_Profissional_Cod", "Ato_Profissional_Nome"]]
    df = df.merge(ato, on="Ato_Profissional_Cod", how="left")
    df["Profissional_Informado"] = df["Profissional_Doc"] != DOC_NAO_INFORMADO

    colunas_finais = [
        "N_AIH",
        "UF",
        "Ano",
        "Mês",
        "Profissional_Doc",
        "Profissional_Informado",
        "Profissional_CBO_Cod",
        "Ato_Profissional_Cod",
        "Ato_Profissional_Nome",
        "Valor_Ato",
    ]
    return df[colunas_finais]


def consolida_brutos() -> tuple[pd.DataFrame, pd.DataFrame]:
    arquivos_procedimentos = sorted(PASTA_BRUTOS.glob("*_procedimentos.parquet"))
    arquivos_profissionais = sorted(PASTA_BRUTOS.glob("*_profissionais.parquet"))

    procedimentos = pd.concat(
        [pd.read_parquet(f) for f in arquivos_procedimentos], ignore_index=True
    )
    profissionais = pd.concat(
        [pd.read_parquet(f) for f in arquivos_profissionais], ignore_index=True
    )
    return procedimentos, profissionais


def main() -> None:
    total_ufs = len(ESTADOS)
    for indice, uf in enumerate(ESTADOS, start=1):
        processa_uf(uf, indice_uf=indice, total_ufs=total_ufs)

    print("Todas as UFs processadas. Consolidando...")
    procedimentos_bruto, profissionais_bruto = consolida_brutos()

    if procedimentos_bruto.empty:
        print("Nenhum registro encontrado para os filtros configurados.")
        return

    sigtap = carrega_tabela_sigtap()

    procedimentos_final = enriquece_procedimentos(procedimentos_bruto, sigtap, profissionais_bruto)
    procedimentos_final.to_csv(
        ARQUIVO_SAIDA_PROCEDIMENTOS, index=False, encoding="utf-8-sig", sep=";", decimal=","
    )
    print(f"Arquivo salvo: {ARQUIVO_SAIDA_PROCEDIMENTOS} ({len(procedimentos_final)} registros)")

    profissionais_final = enriquece_profissionais(profissionais_bruto, sigtap)
    profissionais_final.to_csv(
        ARQUIVO_SAIDA_PROFISSIONAIS,
        index=False,
        encoding="utf-8-sig",
        sep=";",
        decimal=",",
    )
    print(
        f"Arquivo salvo: {ARQUIVO_SAIDA_PROFISSIONAIS} "
        f"({len(profissionais_final)} registros)"
    )


if __name__ == "__main__":
    main()
