"""
Gera docs/dashboard_data.json a partir de base_procedimentos_sus.csv e
base_profissionais_sus.csv, no mesmo esquema (dicionario + linhas indexadas)
usado no protótipo original: cada linha referencia índices em dicionários
compartilhados (CID, município, hospital, tratamento, material, ato
profissional) em vez de repetir texto -- muito mais compacto que embarcar
string por string, e permite filtros combináveis no cliente sem backend.

Profissional_Doc (numero tipo CPF) NUNCA é exposto -- vira um id sequencial
anônimo, usado só para contar profissionais distintos por filtro.
"""
import json
from pathlib import Path

import pandas as pd

ARQUIVO_PROCEDIMENTOS = "base_procedimentos_sus.csv"
ARQUIVO_PROFISSIONAIS = "base_profissionais_sus.csv"
ARQUIVO_SAIDA = Path("docs") / "dashboard_data.json"
COMPETENCIA = "2026-03"

DOC_NAO_INFORMADO = "0" * 15


class Dicionario:
    """Deduplica valores e devolve o índice de cada um (cria se não existir)."""

    def __init__(self):
        self._indice = {}
        self.valores = []

    def idx(self, valor):
        if valor not in self._indice:
            self._indice[valor] = len(self.valores)
            self.valores.append(valor)
        return self._indice[valor]


def main() -> None:
    proc = pd.read_csv(ARQUIVO_PROCEDIMENTOS, sep=";", decimal=",", encoding="utf-8-sig")
    prof = pd.read_csv(ARQUIVO_PROFISSIONAIS, sep=";", decimal=",", encoding="utf-8-sig")

    proc["Material"] = proc["Material"].fillna("Não especificado")
    for col in ["Diagnóstico_Nome", "Município_Nome", "Município_Cirurgia_Nome", "Hospital_Nome"]:
        proc[col] = proc[col].fillna("Não informado")

    # ---- agrega profissionais/atos por AIH ----
    prof["N_AIH"] = prof["N_AIH"].astype(str).str.strip()
    prof["Ato_Profissional_Nome"] = prof["Ato_Profissional_Nome"].fillna("Não especificado")

    dic_ato = Dicionario()
    prof["_atoIdx"] = prof["Ato_Profissional_Nome"].map(dic_ato.idx)

    dic_prof = Dicionario()
    prof_informado = prof[prof["Profissional_Doc"] != DOC_NAO_INFORMADO].copy()
    prof_informado["_profIdx"] = prof_informado["Profissional_Doc"].map(dic_prof.idx)

    atos_por_aih = prof.groupby("N_AIH")["_atoIdx"].apply(lambda s: sorted(set(s))).to_dict()
    profs_por_aih = (
        prof_informado.groupby("N_AIH")["_profIdx"].apply(lambda s: sorted(set(s))).to_dict()
    )

    # ---- dicionarios principais ----
    dic_cid = Dicionario()  # valor = (codigo, nome)
    dic_mun = Dicionario()
    dic_mun_cirurgia = Dicionario()
    dic_hosp = Dicionario()
    dic_trat = Dicionario()  # valor = (codigo, nome)
    dic_material = Dicionario()

    linhas = []
    proc["N_AIH"] = proc["N_AIH"].astype(str).str.strip()
    for r in proc.itertuples(index=False):
        cid_i = dic_cid.idx((r.Diagnóstico_CID, r.Diagnóstico_Nome))
        mun_i = dic_mun.idx(r.Município_Nome)
        mun_cir_i = dic_mun_cirurgia.idx(r.Município_Cirurgia_Nome)
        hosp_i = dic_hosp.idx(r.Hospital_Nome)
        trat_i = dic_trat.idx((r.Tratamento_Cod, r.Tratamento_Nome))
        mat_i = dic_material.idx(r.Material)
        atos = atos_por_aih.get(r.N_AIH, [])
        profs = profs_por_aih.get(r.N_AIH, [])

        linhas.append([
            r.UF, cid_i, mun_i, mun_cir_i, hosp_i, trat_i, mat_i,
            round(float(r.Valor_Total), 2), int(r.Ano), int(r.Mês), int(r.Dia),
            atos, profs,
        ])

    saida = {
        "competencia": COMPETENCIA,
        "totalRegistros": len(linhas),
        "dic": {
            "cid": dic_cid.valores,
            "mun": dic_mun.valores,
            "munCirurgia": dic_mun_cirurgia.valores,
            "hosp": dic_hosp.valores,
            "trat": dic_trat.valores,
            "material": dic_material.valores,
            "ato": dic_ato.valores,
        },
        "procedimentos": linhas,
    }

    ARQUIVO_SAIDA.parent.mkdir(exist_ok=True)
    ARQUIVO_SAIDA.write_text(
        json.dumps(saida, ensure_ascii=False, separators=(",", ":")), encoding="utf-8"
    )
    tamanho_mb = ARQUIVO_SAIDA.stat().st_size / (1024 * 1024)
    print(f"Salvo: {ARQUIVO_SAIDA} ({tamanho_mb:.1f} MB, {len(linhas)} registros)")
    print(f"Profissionais únicos (anonimizados): {len(dic_prof.valores)}")
    print(f"Hospitais: {len(dic_hosp.valores)} | Municípios (cirurgia): {len(dic_mun_cirurgia.valores)}")


if __name__ == "__main__":
    main()
