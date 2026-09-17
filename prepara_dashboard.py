"""
Agrega base_procedimentos_sus.csv num JSON compacto para o dashboard estatico
(dashboard/index.html). Diferente do projeto aortico original (que embarcava
linha a linha), aqui agregamos por procedimento -- o volume e maior (232k+
registros) e o caso de uso e "buscar um procedimento e ver o panorama dele",
nao "explorar cada internacao individualmente".
"""
import json
from pathlib import Path

import pandas as pd

ARQUIVO_ENTRADA = "base_procedimentos_sus.csv"
ARQUIVO_SAIDA = Path("docs") / "dashboard_data.json"
TOP_N_HOSPITAIS = 15
TOP_N_MUNICIPIOS = 15

# Categoria por Tratamento_Nome -- so para agrupar visualmente na busca.
CATEGORIA_POR_TRATAMENTO = {
    "PARTO NORMAL": "Obstetrícia",
    "PARTO NORMAL EM GESTAÇÃO DE ALTO RISCO": "Obstetrícia",
    "PARTO NORMAL EM CENTRO DE PARTO NORMAL (CPN)": "Obstetrícia",
    "ASSISTÊNCIA AO PARTO SEM DISTOCIA": "Obstetrícia",
    "OPERAÇÃO CESARIANA": "Obstetrícia",
    "OPERAÇÃO CESARIANA EM GESTAÇÃO DE ALTO RISCO": "Obstetrícia",
    "OPERAÇÃO CESARIANA COM LAQUEADURA TUBARIA": "Obstetrícia",
    "APENDICECTOMIA": "Cirurgia geral",
    "APENDICECTOMIA VIDEOLAPAROSCÓPICA": "Cirurgia geral",
    "COLECISTECTOMIA": "Cirurgia geral",
    "COLECISTECTOMIA VIDEOLAPAROSCÓPICA": "Cirurgia geral",
    "HERNIOPLASTIA INGUINAL (BILATERAL)": "Cirurgia geral",
    "HERNIOPLASTIA INGUINAL / CRURAL (UNILATERAL)": "Cirurgia geral",
    "HERNIOPLASTIA UMBILICAL": "Cirurgia geral",
    "GASTROPLASTIA COM DERIVAÇÃO INTESTINAL": "Cirurgia bariátrica",
    "GASTROPLASTIA VERTICAL COM BANDA": "Cirurgia bariátrica",
    "ARTROPLASTIA TOTAL PRIMÁRIA DO QUADRIL CIMENTADA": "Ortopedia",
    "ARTROPLASTIA TOTAL PRIMARIA DO QUADRIL NÃO CIMENTADA / HÍBRIDA": "Ortopedia",
    "ARTROPLASTIA PARCIAL DE QUADRIL": "Ortopedia",
    "ARTROPLASTIA TOTAL PRIMÁRIA DO JOELHO": "Ortopedia",
    "ARTROPLASTIA UNICOMPARTIMENTAL PRIMÁRIA DO JOELHO": "Ortopedia",
    "FACECTOMIA COM IMPLANTE DE LENTE INTRA-OCULAR": "Oftalmologia",
    "AMIGDALECTOMIA": "Otorrinolaringologia",
    "AMIGDALECTOMIA COM ADENOIDECTOMIA": "Otorrinolaringologia",
    "HISTERECTOMIA (POR VIA VAGINAL)": "Ginecologia",
    "HISTERECTOMIA C/ ANEXECTOMIA (UNI / BILATERAL)": "Ginecologia",
    "HISTERECTOMIA TOTAL": "Ginecologia",
}


def agrega_procedimento(df: pd.DataFrame, nome: str) -> dict:
    sub = df[df["Tratamento_Nome"] == nome]

    por_uf = (
        sub.groupby("UF")["Valor_Total"]
        .agg(qtd="count", valor="sum")
        .sort_values("qtd", ascending=False)
        .reset_index()
    )
    por_hospital = (
        sub.groupby("Hospital_Nome")["Valor_Total"]
        .agg(qtd="count", valor="sum")
        .sort_values("qtd", ascending=False)
        .head(TOP_N_HOSPITAIS)
        .reset_index()
    )
    por_municipio = (
        sub.groupby("Município_Cirurgia_Nome")["Valor_Total"]
        .agg(qtd="count", valor="sum")
        .sort_values("qtd", ascending=False)
        .head(TOP_N_MUNICIPIOS)
        .reset_index()
    )

    return {
        "nome": nome,
        "categoria": CATEGORIA_POR_TRATAMENTO.get(nome, "Outros"),
        "totalRegistros": int(len(sub)),
        "valorTotal": round(float(sub["Valor_Total"].sum()), 2),
        "valorMedio": round(float(sub["Valor_Total"].mean()), 2),
        "porUF": [
            {"uf": r.UF, "qtd": int(r.qtd), "valor": round(float(r.valor), 2)}
            for r in por_uf.itertuples()
        ],
        "porHospital": [
            {
                "hospital": r.Hospital_Nome,
                "qtd": int(r.qtd),
                "valor": round(float(r.valor), 2),
            }
            for r in por_hospital.itertuples()
        ],
        "porMunicipio": [
            {
                "municipio": r.Município_Cirurgia_Nome,
                "qtd": int(r.qtd),
                "valor": round(float(r.valor), 2),
            }
            for r in por_municipio.itertuples()
        ],
    }


def main() -> None:
    df = pd.read_csv(ARQUIVO_ENTRADA, sep=";", decimal=",", encoding="utf-8-sig")

    procedimentos = [
        agrega_procedimento(df, nome) for nome in sorted(df["Tratamento_Nome"].unique())
    ]
    # Ordena por volume, mais comuns primeiro -- facilita achar algo relevante
    # antes mesmo de usar a busca.
    procedimentos.sort(key=lambda p: p["totalRegistros"], reverse=True)

    saida = {
        "competencia": "2026-03",
        "totalRegistros": int(len(df)),
        "totalProcedimentos": len(procedimentos),
        "totalHospitais": int(df["Hospital_Nome"].nunique()),
        "totalMunicipios": int(df["Município_Cirurgia_Nome"].nunique()),
        "procedimentos": procedimentos,
    }

    ARQUIVO_SAIDA.parent.mkdir(exist_ok=True)
    ARQUIVO_SAIDA.write_text(
        json.dumps(saida, ensure_ascii=False, separators=(",", ":")), encoding="utf-8"
    )
    tamanho_kb = ARQUIVO_SAIDA.stat().st_size / 1024
    print(f"Salvo: {ARQUIVO_SAIDA} ({tamanho_kb:.0f} KB, {len(procedimentos)} procedimentos)")


if __name__ == "__main__":
    main()
