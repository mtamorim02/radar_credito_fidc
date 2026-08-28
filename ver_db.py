"""
Ver banco do Radar de Crédito
Uso: python ver_db.py
"""
import sqlite3
import os
from datetime import datetime

DB_PATH = os.environ.get("RADAR_DB_PATH", "radar_credito.db")

def main():
    if not os.path.exists(DB_PATH):
        print(f"Banco '{DB_PATH}' não encontrado.")
        print("Rode o radar_credito_fidc.py primeiro para criar o banco.")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    while True:
        print(f"\n{'=' * 60}")
        print(f"Banco: {DB_PATH}")
        print(f"{'=' * 60}")
        print("\n[1] Ver últimos 20 pareceres")
        print("[2] Ver pareceres com violação de covenant")
        print("[3] Ver pareceres por nível de risco")
        print("[4] Buscar parecer por nome")
        print("[5] Ver histórico de indicadores de um fundo (CNPJ)")
        print("[6] Ver todos os fundos com histórico")
        print("[7] Ver estatísticas gerais")
        print("[8] Exportar pareceres para CSV")
        print("[9] Ver estrutura das tabelas")
        print("[10] Sair")

        opcao = input("\nEscolha: ").strip()

        if opcao == "1":
            ver_ultimos_pareceres(conn)
        elif opcao == "2":
            ver_pareceres_violados(conn)
        elif opcao == "3":
            ver_por_risco(conn)
        elif opcao == "4":
            buscar_por_nome(conn)
        elif opcao == "5":
            ver_historico_fundo(conn)
        elif opcao == "6":
            ver_todos_fundos(conn)
        elif opcao == "7":
            ver_estatisticas(conn)
        elif opcao == "8":
            exportar_csv(conn)
        elif opcao == "9":
            ver_estrutura(conn)
        elif opcao == "10":
            print("Até!")
            break
        else:
            print("Opção inválida.")

    conn.close()

def ver_ultimos_pareceres(conn):
    print("\n--- ÚLTIMOS 20 PARECERES ---\n")
    rows = conn.execute(
        """SELECT id, timestamp, nome_operacao, risk_level,
                  covenant_violado, inadimplencia, pdd
           FROM pareceres ORDER BY id DESC LIMIT 20"""
    ).fetchall()
    if not rows:
        print("Nenhum parecer encontrado.")
        return
    for r in rows:
        data = r["timestamp"][:19].replace("T", " ")
        violado = "⚠️ SIM" if r["covenant_violado"] else "NÃO"
        print(f"  [{r['id']:3}] {data} | {r['nome_operacao'][:40]:40} | "
              f"Risco: {r['risk_level']:5} | Violado: {violado} | "
              f"Inad: {r['inadimplencia'] or 'N/A'} | PDD: {r['pdd'] or 'N/A'}")

def ver_pareceres_violados(conn):
    print("\n--- PARECERES COM VIOLAÇÃO DE COVENANT ---\n")
    rows = conn.execute(
        """SELECT id, timestamp, nome_operacao, risk_level,
                  covenant_detalhe, parecer_final
           FROM pareceres WHERE covenant_violado = 1
           ORDER BY id DESC"""
    ).fetchall()
    if not rows:
        print("Nenhuma violação encontrada. ✅")
        return
    for r in rows:
        data = r["timestamp"][:19].replace("T", " ")
        print(f"\n  [{r['id']:3}] {data} | {r['nome_operacao']}")
        print(f"         Risco: {r['risk_level']}")
        detalhe = r["covenant_detalhe"] or ""
        if len(detalhe) > 200:
            detalhe = detalhe[:200] + "..."
        print(f"         Detalhe: {detalhe}")

def ver_por_risco(conn):
    nivel = input("Nível (alto/medio/baixo): ").strip().lower()
    if nivel not in ("alto", "medio", "baixo"):
        print("Nível inválido.")
        return
    print(f"\n--- PARECERES COM RISCO: {nivel.upper()} ---\n")
    rows = conn.execute(
        """SELECT id, timestamp, nome_operacao, covenant_violado,
                  inadimplencia, pdd, parecer_final
           FROM pareceres WHERE risk_level = ?
           ORDER BY id DESC LIMIT 30""",
        (nivel,)
    ).fetchall()
    if not rows:
        print(f"Nenhum parecer com risco '{nivel}'.")
        return
    for r in rows:
        data = r["timestamp"][:19].replace("T", " ")
        violado = "⚠️" if r["covenant_violado"] else ""
        print(f"  [{r['id']:3}] {data} | {r['nome_operacao'][:45]:45} {violado}")
        print(f"         Inad: {r['inadimplencia'] or 'N/A'} | PDD: {r['pdd'] or 'N/A'}")
        parecer = r["parecer_final"] or ""
        if len(parecer) > 150:
            parecer = parecer[:150] + "..."
        print(f"         Parecer: {parecer}")
        print()

def buscar_por_nome(conn):
    termo = input("Nome (ou parte): ").strip()
    if not termo:
        print("Termo vazio.")
        return
    print(f"\n--- BUSCA: '{termo}' ---\n")
    rows = conn.execute(
        """SELECT id, timestamp, nome_operacao, risk_level,
                  covenant_violado, parecer_final
           FROM pareceres WHERE nome_operacao LIKE ?
           ORDER BY id DESC""",
        (f"%{termo}%",)
    ).fetchall()
    if not rows:
        print(f"Nenhum parecer encontrado para '{termo}'.")
        return
    for r in rows:
        data = r["timestamp"][:19].replace("T", " ")
        violado = "⚠️ SIM" if r["covenant_violado"] else "NÃO"
        print(f"  [{r['id']:3}] {data} | {r['nome_operacao'][:50]:50} | "
              f"Risco: {r['risk_level']:5} | Violado: {violado}")

def ver_historico_fundo(conn):
    cnpj_input = input("CNPJ do fundo: ").strip()
    cnpj_limpo = "".join(c for c in cnpj_input if c.isdigit())
    if len(cnpj_limpo) != 14:
        print("CNPJ inválido.")
        return
    print(f"\n--- HISTÓRICO: {cnpj_limpo} ---\n")
    rows = conn.execute(
        """SELECT timestamp, nome_fundo, patrimonio_liquido,
                  patrimonio_liquido_anterior, variacao_pl,
                  inadimplencia, inadimplencia_anterior,
                  pdd, pdd_anterior, subordinacao, subordinacao_anterior,
                  alerta_deterioracao
           FROM historico_indicadores WHERE cnpj = ?
           ORDER BY id""",
        (cnpj_limpo,)
    ).fetchall()
    if not rows:
        print(f"Nenhum histórico para o CNPJ {cnpj_limpo}.")
        return
    print(f"Fundo: {rows[0]['nome_fundo']}")
    print(f"Total de análises: {len(rows)}\n")
    for r in rows:
        data = r["timestamp"][:19].replace("T", " ")
        pl = f"R$ {r['patrimonio_liquido']:,.2f}" if r["patrimonio_liquido"] else "N/A"
        var = f"{r['variacao_pl']:+.1f}%" if r["variacao_pl"] is not None else "—"
        print(f"  {data}")
        print(f"    PL: {pl} (var: {var})")
        print(f"    Inadimplência: {r['inadimplencia'] or 'N/A'} (ant: {r['inadimplencia_anterior'] or 'N/A'})")
        print(f"    PDD: {r['pdd'] or 'N/A'} (ant: {r['pdd_anterior'] or 'N/A'})")
        print(f"    Subordinação: {r['subordinacao'] or 'N/A'} (ant: {r['subordinacao_anterior'] or 'N/A'})")
        if r["alerta_deterioracao"]:
            print(f"    ⚠️ {r['alerta_deterioracao']}")
        print()

def ver_todos_fundos(conn):
    print("\n--- FUNDOS COM HISTÓRICO ---\n")
    rows = conn.execute(
        """SELECT cnpj, nome_fundo,
                  COUNT(*) as total,
                  MAX(timestamp) as ultima,
                  MIN(patrimonio_liquido) as pl_min,
                  MAX(patrimonio_liquido) as pl_max,
                  SUM(CASE WHEN alerta_deterioracao IS NOT NULL THEN 1 ELSE 0 END) as alertas
           FROM historico_indicadores
           GROUP BY cnpj, nome_fundo
           ORDER BY ultima DESC"""
    ).fetchall()
    if not rows:
        print("Nenhum fundo com histórico.")
        return
    for r in rows:
        print(f"  CNPJ: {r['cnpj']}")
        print(f"    Nome: {r['nome_fundo']}")
        print(f"    Análises: {r['total']} | Última: {r['ultima'][:19]}")
        pl_min = f"R$ {r['pl_min']:,.2f}" if r["pl_min"] else "N/A"
        pl_max = f"R$ {r['pl_max']:,.2f}" if r["pl_max"] else "N/A"
        print(f"    PL: {pl_min} → {pl_max}")
        if r["alertas"]:
            print(f"    ⚠️ {r['alertas']} alerta(s) de deterioração")
        print()

def ver_estatisticas(conn):
    print("\n--- ESTATÍSTICAS GERAIS ---\n")
    total = conn.execute("SELECT COUNT(*) FROM pareceres").fetchone()[0]
    print(f"Total de pareceres: {total}")

    por_risco = conn.execute(
        "SELECT risk_level, COUNT(*) FROM pareceres GROUP BY risk_level"
    ).fetchall()
    for r in por_risco:
        print(f"  Risco {r[0] or 'N/A'}: {r[1]}")

    violados = conn.execute(
        "SELECT COUNT(*) FROM pareceres WHERE covenant_violado = 1"
    ).fetchone()[0]
    print(f"Violações de covenant: {violados}")

    total_hist = conn.execute("SELECT COUNT(*) FROM historico_indicadores").fetchone()[0]
    print(f"\nSnapshots de histórico: {total_hist}")

    fundos_unicos = conn.execute(
        "SELECT COUNT(DISTINCT cnpj) FROM historico_indicadores"
    ).fetchone()[0]
    print(f"Fundos únicos com histórico: {fundos_unicos}")

    alertas = conn.execute(
        "SELECT COUNT(*) FROM historico_indicadores WHERE alerta_deterioracao IS NOT NULL"
    ).fetchone()[0]
    print(f"Alertas de deterioração: {alertas}")

def exportar_csv(conn):
    import csv
    filename = f"pareceres_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    rows = conn.execute(
        """SELECT id, timestamp, nome_operacao, tipo_operacao, risk_level,
                  risk_justificativa, is_fidc, covenant_violado, covenant_detalhe,
                  inadimplencia, pdd, parecer_final
           FROM pareceres ORDER BY id DESC"""
    ).fetchall()
    if not rows:
        print("Nenhum parecer para exportar.")
        return
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow([d[0] for d in conn.execute(
            "SELECT * FROM pareceres LIMIT 0").description])
        for row in rows:
            writer.writerow(row)
    print(f"✅ Exportado: {filename} ({len(rows)} registros)")

def ver_estrutura(conn):
    print("\n--- ESTRUTURA DAS TABELAS ---\n")
    for tabela in ["pareceres", "historico_indicadores"]:
        print(f"Tabela: {tabela}")
        cols = conn.execute(f"PRAGMA table_info({tabela})").fetchall()
        for c in cols:
            print(f"  {c['name']:35} {c['type'] or 'TEXT':15} "
                  f"{'PK' if c['pk'] else ''} {'NOT NULL' if c['notnull'] else ''}")
        print()

if __name__ == "__main__":
    main()