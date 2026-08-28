"""
Radar de Crédito Estruturado — Versão com fidcs.com.br + tratamento completo de erros
-------------------------------------------------------------------------------------
Agente com LangGraph que faz triagem de risco de crédito e, quando a
operação é um FIDC, analisa covenants do regulamento para detectar
violações de cláusulas estruturais.

Fontes de dados (ordem de prioridade):
    1. fidcs.com.br (dados da CVM já processados — mais rápido)
    2. CVM CSV (cad_fi.csv + registro_fundo_classe.zip)
    3. DuckDuckGo (fallback web)
    4. Google News RSS (fatos relevantes)

Tratamento de erros abrangente:
    - Timeout, rate limiting (429), conexão recusada
    - Respostas vazias, malformadas, encoding incorreto
    - CAPTCHA do DuckDuckGo, feed XML quebrado
    - CSV corrompido, ZIP inválido, arquivo travado
    - JSON inválido do LLM, resposta vazia do Groq
    - SQLite locked, disco cheio, permissão negada
    - CNPJ inválido, campos ausentes, delimitador incorreto
"""

import os
import sys
import json
import sqlite3
import hashlib
import re
import csv as csv_module
import xml.etree.ElementTree as ET
import tempfile
import zipfile
import shutil
import time
from datetime import datetime, timedelta
from typing import Literal, Optional
from urllib.parse import quote_plus
from email.utils import parsedate_to_datetime

import requests
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from langchain_groq import ChatGroq
from langgraph.graph import StateGraph, END
from typing_extensions import TypedDict

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
load_dotenv(r"D:\VSCode\projeto_FIDC\.env", override=True)

GROQ_KEY = os.environ.get("GROQ_API_KEY")
if not GROQ_KEY:
    print("[ERRO CRÍTICO] GROQ_API_KEY não encontrada no .env")
    sys.exit(1)

try:
    llm = ChatGroq(
        model="openai/gpt-oss-120b",
        temperature=0,
        api_key=GROQ_KEY,
        max_retries=3,
        timeout=60,
    )
except Exception as e:
    print(f"[ERRO CRÍTICO] Falha ao inicializar Groq: {e}")
    sys.exit(1)

HTTP_TIMEOUT = 30
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
}
MAX_RETRIES = 3
RETRY_DELAY = 2

# ---------------------------------------------------------------------------
# Helpers de erro
# ---------------------------------------------------------------------------

def _request_com_retry(url: str, metodo: str = "GET", **kwargs) -> Optional[requests.Response]:
    """Faz requisição HTTP com retry. Trata timeout, 429, 5xx, conexão."""
    kwargs.setdefault("timeout", HTTP_TIMEOUT)
    kwargs.setdefault("headers", HTTP_HEADERS)
    for tentativa in range(1, MAX_RETRIES + 1):
        try:
            if metodo == "GET":
                resp = requests.get(url, **kwargs)
            elif metodo == "POST":
                resp = requests.post(url, **kwargs)
            else:
                return None
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", RETRY_DELAY * tentativa))
                print(f"  [HTTP 429] Rate limited. Aguardando {wait}s (tentativa {tentativa}/{MAX_RETRIES})")
                time.sleep(wait)
                continue
            if 500 <= resp.status_code < 600:
                print(f"  [HTTP {resp.status_code}] Erro do servidor (tentativa {tentativa}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY * tentativa)
                continue
            return resp
        except requests.exceptions.Timeout:
            print(f"  [TIMEOUT] {url} (tentativa {tentativa}/{MAX_RETRIES})")
            time.sleep(RETRY_DELAY * tentativa)
        except requests.exceptions.ConnectionError as e:
            print(f"  [CONEXÃO] {url}: {e} (tentativa {tentativa}/{MAX_RETRIES})")
            time.sleep(RETRY_DELAY * tentativa)
        except requests.exceptions.RequestException as e:
            print(f"  [ERRO] {url}: {e} (tentativa {tentativa}/{MAX_RETRIES})")
            time.sleep(RETRY_DELAY * tentativa)
    print(f"  [FALHA] Todas as {MAX_RETRIES} tentativas falharam para {url}")
    return None

def _limpar_html(texto: str) -> str:
    """Remove tags HTML e entidades comuns."""
    if not texto:
        return ""
    texto = re.sub(r"<[^>]+>", "", texto)
    texto = texto.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    texto = texto.replace("&quot;", '"').replace("&#39;", "'").replace("&nbsp;", " ")
    return texto.strip()

def _extrair_json(resposta: str) -> Optional[dict]:
    """Extrai JSON da resposta do LLM. Trata markdown, texto extra, aspas simples."""
    if not resposta:
        return None
    resposta = resposta.strip()
    if resposta.startswith("```"):
        linhas = resposta.split("\n")
        linhas = linhas[1:]
        if linhas and linhas[-1].strip().startswith("```"):
            linhas = linhas[:-1]
        resposta = "\n".join(linhas).strip()
    try:
        return json.loads(resposta)
    except json.JSONDecodeError:
        pass
    tentativa = resposta.replace("'", '"')
    try:
        return json.loads(tentativa)
    except json.JSONDecodeError:
        pass
    match = re.search(r"\{.*\}", resposta, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            pass
    return None

def _invocar_llm_com_retry(prompt: str) -> Optional[str]:
    """Invoca o LLM com retry. Trata timeout, rate limiting, resposta vazia."""
    for tentativa in range(1, MAX_RETRIES + 1):
        try:
            resposta = llm.invoke(prompt)
            if resposta and resposta.content:
                return resposta.content.strip()
            print(f"  [LLM] Resposta vazia (tentativa {tentativa}/{MAX_RETRIES})")
        except Exception as e:
            erro_str = str(e).lower()
            if "rate" in erro_str or "429" in erro_str:
                wait = RETRY_DELAY * tentativa * 2
                print(f"  [LLM 429] Rate limited. Aguardando {wait}s (tentativa {tentativa}/{MAX_RETRIES})")
                time.sleep(wait)
            elif "timeout" in erro_str or "timed out" in erro_str:
                print(f"  [LLM TIMEOUT] (tentativa {tentativa}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY * tentativa)
            else:
                print(f"  [LLM ERRO] {e} (tentativa {tentativa}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY * tentativa)
    print(f"  [LLM FALHA] Todas as {MAX_RETRIES} tentativas falharam")
    return None

def _validar_cnpj(cnpj: str) -> tuple:
    """Valida formato do CNPJ. Retorna (cnpj_limpo, cnpj_formatado) ou (None, None)."""
    if not cnpj:
        return None, None
    cnpj_limpo = "".join(c for c in cnpj if c.isdigit())
    if len(cnpj_limpo) != 14:
        return None, None
    cnpj_formatado = f"{cnpj_limpo[:2]}.{cnpj_limpo[2:5]}.{cnpj_limpo[5:8]}/{cnpj_limpo[8:12]}-{cnpj_limpo[12:14]}"
    return cnpj_limpo, cnpj_formatado

# ---------------------------------------------------------------------------
# Estado do grafo
# ---------------------------------------------------------------------------
class CreditState(TypedDict, total=False):
    operacao: dict
    is_fidc: bool
    risk_level: str
    risk_justificativa: str
    contexto_externo: str
    regulamento_texto: str
    covenants_extraidos: list
    covenant_violado: bool
    covenant_detalhe: str
    violacoes_lista: list
    informe_cvm: dict
    parecer_final: str
    registro_id: int
    alertas_deterioracao: list 

# ---------------------------------------------------------------------------
# Persistência (SQLite)
# ---------------------------------------------------------------------------
DB_PATH = os.environ.get("RADAR_DB_PATH", "radar_credito.db")

def init_db():
    """Cria as tabelas de pareceres e histórico de indicadores."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pareceres (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                nome_operacao TEXT,
                tipo_operacao TEXT,
                risk_level TEXT,
                risk_justificativa TEXT,
                is_fidc INTEGER,
                covenants_extraidos TEXT,
                covenant_violado INTEGER,
                covenant_detalhe TEXT,
                contexto_externo TEXT,
                parecer_final TEXT,
                hash TEXT UNIQUE,
                inadimplencia TEXT,
                pdd TEXT
            )
        """)
        try:
            conn.execute("ALTER TABLE pareceres ADD COLUMN inadimplencia TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE pareceres ADD COLUMN pdd TEXT")
        except sqlite3.OperationalError:
            pass
        # NOVA TABELA: evolução dos indicadores ao longo do tempo
        conn.execute("""
            CREATE TABLE IF NOT EXISTS historico_indicadores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                cnpj TEXT,
                nome_fundo TEXT,
                patrimonio_liquido REAL,
                patrimonio_liquido_anterior REAL,
                inadimplencia TEXT,
                inadimplencia_anterior TEXT,
                pdd TEXT,
                pdd_anterior TEXT,
                subordinacao TEXT,
                subordinacao_anterior TEXT,
                variacao_pl REAL,
                alerta_deterioracao TEXT
            )
        """)
        conn.commit()
        conn.close()
    except sqlite3.OperationalError as e:
        print(f"[DB] Erro ao criar tabela: {e}")
    except Exception as e:
        print(f"[DB] Erro inesperado: {e}")

def salvar_parecer(state: dict) -> Optional[int]:
    """Salva parecer no SQLite, incluindo inadimplência e PDD."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        content_hash = hashlib.md5(
            f"{state.get('operacao', {}).get('nome', '')}{datetime.now().isoformat()}".encode()
        ).hexdigest()

        # Extrai inadimplência e PDD do informe_cvm
        informe = state.get("informe_cvm", {})
        if not isinstance(informe, dict):
            informe = {}
        inadimplencia = informe.get("inadimplencia_90d", "")
        pdd = informe.get("pdd", "")

        cursor = conn.execute(
            """INSERT OR REPLACE INTO pareceres
               (timestamp, nome_operacao, tipo_operacao, risk_level,
                risk_justificativa, is_fidc, covenants_extraidos,
                covenant_violado, covenant_detalhe, contexto_externo,
                parecer_final, hash, inadimplencia, pdd)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                datetime.now().isoformat(),
                state.get("operacao", {}).get("nome", "N/A"),
                state.get("operacao", {}).get("tipo", "N/A"),
                state.get("risk_level", "N/A"),
                state.get("risk_justificativa", ""),
                int(state.get("is_fidc", False)),
                json.dumps(state.get("covenants_extraidos", []), ensure_ascii=False),
                int(state.get("covenant_violado", False)),
                state.get("covenant_detalhe", ""),
                state.get("contexto_externo", ""),
                state.get("parecer_final", ""),
                content_hash,
                inadimplencia,
                pdd,
            ),
        )
        conn.commit()
        registro_id = cursor.lastrowid
        conn.close()
        return registro_id
    except sqlite3.OperationalError as e:
        print(f"[DB] Erro ao salvar (locked/disco): {e}")
        return None
    except Exception as e:
        print(f"[DB] Erro inesperado: {e}")
        return None

def _parsear_valor(texto):
    """Converte '9,5%', '0.095' etc. para float. Retorna None se não conseguir."""
    if texto is None:
        return None
    texto = str(texto).strip()
    if texto in ("", "N/A", "—", "não informado", "nao informado", "verificar informe mensal"):
        return None
    texto = texto.replace("%", "").replace("R$", "").strip()
    if "," in texto:
        texto = texto.replace(".", "").replace(",", ".")
    try:
        return float(texto)
    except ValueError:
        return None

def _buscar_ultimo_historico(cnpj_limpo):
    """Retorna o último snapshot de indicadores do CNPJ, ou None."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        cursor = conn.execute(
            """SELECT timestamp, patrimonio_liquido, inadimplencia, pdd, subordinacao
               FROM historico_indicadores WHERE cnpj = ?
               ORDER BY id DESC LIMIT 1""",
            (cnpj_limpo,),
        )
        row = cursor.fetchone()
        conn.close()
        if row:
            return {
                "timestamp": row[0],
                "patrimonio_liquido": row[1],
                "inadimplencia": row[2],
                "pdd": row[3],
                "subordinacao": row[4],
            }
        return None
    except sqlite3.Error as e:
        print(f"[DB] Erro ao buscar histórico: {e}")
        return None

def detectar_deterioracao(informe_atual, ultimo):
    """Compara indicadores atuais com o último snapshot. Retorna lista de alertas."""
    alertas = []
    if not ultimo:
        return alertas  # Primeira análise — sem base de comparação

    # --- PL ---
    pl_atual = informe_atual.get("patrimonio_liquido")
    pl_anterior = ultimo.get("patrimonio_liquido")
    if pl_atual and pl_anterior:
        variacao = (pl_atual - pl_anterior) / pl_anterior * 100
        if variacao <= -20:
            alertas.append(f"🔴 PL caiu {abs(variacao):.1f}% desde a última análise (R$ {pl_anterior:,.2f} → R$ {pl_atual:,.2f})")
        elif variacao <= -10:
            alertas.append(f"🟡 PL caiu {abs(variacao):.1f}% desde a última análise (R$ {pl_anterior:,.2f} → R$ {pl_atual:,.2f})")

    # --- Inadimplência (variação em pontos percentuais) ---
    inad_atual = _parsear_valor(informe_atual.get("inadimplencia_90d"))
    inad_anterior = _parsear_valor(ultimo.get("inadimplencia"))
    if inad_atual is not None and inad_anterior is not None:
        delta = inad_atual - inad_anterior
        if delta >= 5:
            alertas.append(f"🔴 Inadimplência subiu {delta:.1f} p.p. (de {inad_anterior:.1f}% para {inad_atual:.1f}%)")
        elif delta >= 2:
            alertas.append(f"🟡 Inadimplência subiu {delta:.1f} p.p. (de {inad_anterior:.1f}% para {inad_atual:.1f}%)")

    # --- PDD ---
    pdd_atual = _parsear_valor(informe_atual.get("pdd"))
    pdd_anterior = _parsear_valor(ultimo.get("pdd"))
    if pdd_atual is not None and pdd_anterior is not None:
        delta = pdd_atual - pdd_anterior
        if delta > 0:
            alertas.append(f"🟡 PDD subiu de {pdd_anterior:.1f} para {pdd_atual:.1f}")

    return alertas

def salvar_historico_indicadores(cnpj_fundo, informe, alertas):
    """Salva snapshot dos indicadores no histórico. Retorna o ID ou None."""
    cnpj_limpo, _ = _validar_cnpj(cnpj_fundo)
    if not cnpj_limpo:
        return None
    try:
        ultimo = _buscar_ultimo_historico(cnpj_limpo)
        pl_atual = informe.get("patrimonio_liquido")
        pl_anterior = ultimo.get("patrimonio_liquido") if ultimo else None
        variacao_pl = None
        if pl_atual and pl_anterior:
            variacao_pl = (pl_atual - pl_anterior) / pl_anterior * 100
        conn = sqlite3.connect(DB_PATH, timeout=10)
        cursor = conn.execute(
            """INSERT INTO historico_indicadores
               (timestamp, cnpj, nome_fundo, patrimonio_liquido, patrimonio_liquido_anterior,
                inadimplencia, inadimplencia_anterior, pdd, pdd_anterior,
                subordinacao, subordinacao_anterior, variacao_pl, alerta_deterioracao)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                datetime.now().isoformat(),
                cnpj_limpo,
                informe.get("nome_fundo_cvm", "N/A"),
                pl_atual,
                pl_anterior,
                str(informe.get("inadimplencia_90d", "")),
                (str(ultimo.get("inadimplencia")) if ultimo else None),
                str(informe.get("pdd", "")),
                (str(ultimo.get("pdd")) if ultimo else None),
                str(informe.get("subordinacao_atual", "")),
                (str(ultimo.get("subordinacao")) if ultimo else None),
                variacao_pl,
                "\n".join(alertas) if alertas else None,
            ),
        )
        conn.commit()
        registro_id = cursor.lastrowid
        conn.close()
        return registro_id
    except sqlite3.Error as e:
        print(f"[DB] Erro ao salvar histórico: {e}")
        return None

# ---------------------------------------------------------------------------
# Funções de download e busca CVM
# ---------------------------------------------------------------------------

def _baixar_csv_cvm(url: str, cache_path: str) -> bool:
    """Baixa CSV da CVM para cache. Trata timeout, incompleto, corrompido, disco."""
    try:
        resp = requests.get(url, timeout=120, stream=True, headers=HTTP_HEADERS)
        if resp.status_code != 200:
            print(f"  [CVM] HTTP {resp.status_code} ao baixar {url}")
            return False
        tmp_path = cache_path + ".tmp"
        try:
            with open(tmp_path, "wb") as f:
                bytes_baixados = 0
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        bytes_baixados += len(chunk)
            if bytes_baixados < 100:
                print(f"  [CVM] Arquivo muito pequeno ({bytes_baixados} bytes)")
                os.remove(tmp_path)
                return False
            shutil.move(tmp_path, cache_path)
            tamanho_mb = os.path.getsize(cache_path) / (1024 * 1024)
            print(f"  [CVM] Arquivo baixado: {tamanho_mb:.1f} MB")
            return True
        except IOError as e:
            print(f"  [CVM] Erro de I/O ao salvar: {e}")
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
            return False
    except requests.exceptions.Timeout:
        print(f"  [CVM] Timeout ao baixar {url}")
        return False
    except requests.exceptions.ConnectionError:
        print(f"  [CVM] Erro de conexão ao baixar {url}")
        return False
    except Exception as e:
        print(f"  [CVM] Erro inesperado no download: {e}")
        return False

def _baixar_e_extrair_zip_cvm(url: str, cache_csv_path: str) -> bool:
    """Baixa ZIP da CVM, extrai o CSV do FUNDO (não da classe)."""
    zip_path = cache_csv_path.replace(".csv", ".zip")
    try:
        resp = requests.get(url, timeout=120, stream=True, headers=HTTP_HEADERS)
        if resp.status_code != 200:
            print(f"  [CVM] HTTP {resp.status_code} ao baixar ZIP")
            return False
        with open(zip_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        tamanho_mb = os.path.getsize(zip_path) / (1024 * 1024)
        print(f"  [CVM] ZIP baixado: {tamanho_mb:.1f} MB")
    except Exception as e:
        print(f"  [CVM] Erro ao baixar ZIP: {e}")
        return False
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            nomes = zf.namelist()
            print(f"  [CVM] Arquivos no ZIP: {nomes}")
            csv_encontrado = None
            # PRIORIZA registro_fundo.csv sobre registro_classe.csv
            for nome in nomes:
                if nome.endswith('.csv') and 'fundo' in nome.lower():
                    csv_encontrado = nome
                    break
            if not csv_encontrado:
                for nome in nomes:
                    if nome.endswith('.csv'):
                        csv_encontrado = nome
                        break
            if not csv_encontrado:
                print(f"  [CVM] Nenhum CSV encontrado dentro do ZIP")
                return False
            zf.extract(csv_encontrado, tempfile.gettempdir())
            caminho_extraido = os.path.join(tempfile.gettempdir(), csv_encontrado)
            if caminho_extraido != cache_csv_path:
                shutil.move(caminho_extraido, cache_csv_path)
            print(f"  [CVM] CSV extraído: {csv_encontrado}")
            return True
    except zipfile.BadZipFile:
        print(f"  [CVM] ZIP corrompido ou inválido")
        return False
    except Exception as e:
        print(f"  [CVM] Erro ao extrair ZIP: {e}")
        return False

def _buscar_cnpj_no_csv(csv_path: str, cnpj_limpo: str) -> Optional[dict]:
    """Busca CNPJ em CSV da CVM. Trata encoding, delimitador, colunas ausentes."""
    encodings = ["latin-1", "utf-8", "cp1252", "iso-8859-1"]
    for encoding in encodings:
        try:
            with open(csv_path, "r", encoding=encoding) as f:
                primeira_linha = f.readline()
                f.seek(0)
                delimitador = ";"
                if primeira_linha.count(",") > primeira_linha.count(";"):
                    delimitador = ","
                reader = csv_module.DictReader(f, delimiter=delimitador)
                if reader.fieldnames is None:
                    continue
                coluna_cnpj = None
                for candidato in ["CNPJ_FUNDO", "cnpj_fundo", "Cnpj", "CNPJ"]:
                    if candidato in reader.fieldnames:
                        coluna_cnpj = candidato
                        break
                if not coluna_cnpj:
                    continue
                for row in reader:
                    cnpj_row = "".join(c for c in (row.get(coluna_cnpj) or "") if c.isdigit())
                    if cnpj_row == cnpj_limpo:
                        return dict(row)
            return None
        except UnicodeDecodeError:
            continue
        except csv_module.Error:
            continue
        except IOError as e:
            print(f"  [CVM] Erro de I/O ao ler CSV: {e}")
            return None
        except Exception:
            continue
    return None

# ---------------------------------------------------------------------------
# Buscador PRIMÁRIO — fidcs.com.br
# ---------------------------------------------------------------------------

def buscar_medidas_fie_cvm(cnpj_fundo: str) -> dict:
    """
    Busca inadimplência e PDD do FIDC no CSV de medidas FIE da CVM.
    URL: https://dados.cvm.gov.br/dados/FIE/MEDIDAS/DADOS/medidas_mes_fie_AAAAMM.csv
    Tenta os últimos 3 meses até encontrar dados.
    """
    cnpj_limpo, cnpj_formatado = _validar_cnpj(cnpj_fundo)
    if not cnpj_limpo:
        return {"erro": "CNPJ inválido"}

    agora = datetime.now()
    meses_para_tentar = []

    for i in range(6):  # Tenta os últimos 6 meses
        data = agora - timedelta(days=30 * (i + 1))
        meses_para_tentar.append(data.strftime("%Y%m"))

    for mes_ano in meses_para_tentar:
        cache_path = os.path.join(tempfile.gettempdir(), f"cvm_medidas_fie_{mes_ano}.csv")

        if not os.path.exists(cache_path):
            url = f"https://dados.cvm.gov.br/dados/FIE/MEDIDAS/DADOS/medidas_mes_fie_{mes_ano}.csv"
            print(f"[CVM-MEDIDAS] Tentando: {url}")
            baixou = _baixar_csv_cvm(url, cache_path)
            if not baixou:
                continue
        else:
            print(f"[CVM-MEDIDAS] Usando cache: medidas_fie_{mes_ano}.csv")

        resultado = _buscar_cnpj_no_csv(cache_path, cnpj_limpo)
        if resultado:
            print(f"[CVM-MEDIDAS] ✓ Encontrado para {mes_ano}")
            # Extrai inadimplência e PDD — nomes de colunas podem variar
            inad = None
            pdd = None

            # Procura por colunas de inadimplência
            for chave, valor in resultado.items():
                if not valor:
                    continue
                chave_lower = (chave or "").lower()
                if "inadimpl" in chave_lower and not inad:
                    inad = valor
                if "pdd" in chave_lower or "provis" in chave_lower.lower():
                    if not pdd:
                        pdd = valor

            return {
                "inadimplencia_90d": inad or "não informado",
                "pdd": pdd or "não informado",
                "data_referencia_medidas": mes_ano,
                "fonte_medidas": f"CVM FIE Medidas ({mes_ano})",
            }

    print(f"[CVM-MEDIDAS] CNPJ não encontrado nos meses disponíveis")
    return {"erro": "Não encontrado nas medidas FIE da CVM"}

def buscar_dados_fidcs_com_br(cnpj_fundo):
    """
    Busca dados do FIDC no fidcs.com.br usando Playwright (renderiza JS).
    Extrai texto visível da página em vez de HTML — mais robusto.
    """
    cnpj_limpo, cnpj_formatado = _validar_cnpj(cnpj_fundo)
    if not cnpj_limpo:
        return {"erro": f"CNPJ inválido: '{cnpj_fundo}'"}

    url = f"https://fidcs.com.br/fundo/{cnpj_limpo}"
    print(f"[FIDCS] Buscando dados em: {url}")

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("[FIDCS] Playwright não instalado. Usando requests (sem dados dinâmicos).")
        resp = _request_com_retry(url)
        if resp is None or resp.status_code != 200:
            return {"erro": "Fundo não encontrado no fidcs.com.br"}
        dados = _extrair_dados_html_estatico(resp.text)
        if dados:
            dados["fonte"] = "fidcs.com.br (HTML estático — sem JS)"
            dados["url_fonte"] = url
            return dados
        return {"erro": "Fundo não encontrado no fidcs.com.br"}

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, wait_until="networkidle", timeout=30000)
            page.wait_for_timeout(3000)

            # Extrai o TEXTO VISÍVEL da página (não HTML) — muito mais robusto
            texto_visivel = page.inner_text("body")
            browser.close()

        # Salva o texto em arquivo para debug
        debug_path = os.path.join(tempfile.gettempdir(), f"fidcs_debug_{cnpj_limpo}.txt")
        with open(debug_path, "w", encoding="utf-8") as f:
            f.write(texto_visivel)
        print(f"[FIDCS] Texto da página salvo para debug: {debug_path}")

    except Exception as e:
        print(f"[FIDCS] Erro no Playwright: {e}")
        resp = _request_com_retry(url)
        if resp is None or resp.status_code != 200:
            return {"erro": "Fundo não encontrado no fidcs.com.br"}
        dados = _extrair_dados_html_estatico(resp.text)
        if dados:
            dados["fonte"] = "fidcs.com.br (HTML estático — sem JS)"
            dados["url_fonte"] = url
            return dados
        return {"erro": "Fundo não encontrado no fidcs.com.br"}

    dados = _extrair_dados_texto(texto_visivel, cnpj_fundo)

    if not dados.get("nome_fundo_cvm") and not dados.get("patrimonio_liquido"):
        print(f"[FIDCS] Página não contém dados estruturados do fundo")
        return {"erro": "Fundo não encontrado ou dados indisponíveis no fidcs.com.br"}

    dados["fonte"] = "fidcs.com.br (dados da CVM processados)"
    dados["url_fonte"] = url

    print(f"[FIDCS] ✓ Dados encontrados: {dados.get('nome_fundo_cvm', 'N/A')}")
    if dados.get("patrimonio_liquido"):
        print(f"[FIDCS] PL: R$ {dados['patrimonio_liquido']:,.2f}")
    if dados.get("inadimplencia_90d"):
        print(f"[FIDCS] Inadimplência: {dados['inadimplencia_90d']}")
    if dados.get("pdd"):
        print(f"[FIDCS] PDD: {dados['pdd']}")
    if dados.get("gestor"):
        print(f"[FIDCS] Gestor: {dados['gestor']}")

    return dados

def _extrair_dados_texto(texto, cnpj_fundo=""):
    """
    Extrai dados do TEXTO VISÍVEL da página (pós-renderização JS).
    Prioriza o parágrafo descritivo "Perfil, histórico e informações cadastrais"
    que contém todos os dados em linguagem natural — muito mais robusto.
    """
    dados = {}
    linhas = texto.split("\n")
    linhas_limpas = [l.strip() for l in linhas if l.strip()]

    # --- Extração via parágrafo descritivo (PRIORIDADE) ---
    # O site gera um texto com todas as informações. Ex:
    # "gerido pela OURO PRETO GESTÃO DE RECURSOS S.A. e administrado por QI..."
    # "patrimônio líquido de R$ 14,25 milhões"
    # "inadimplência da carteira está em 1,99%"
    # "PDD) corresponde a 0,90% da carteira"
    # "subordinação de 100,00%"

    # Junta o texto todo em uma string para regex
    texto_completo = "\n".join(linhas_limpas)

    # Gestora: "gerido pela X e" ou "gerido pela X."
    gestor_match = re.search(
        r'gerid[oa] pel[oa]\s+(.+?)\s+(?:e\s+administrad|\.|$)',
        texto_completo, re.IGNORECASE
    )
    if gestor_match:
        dados["gestor"] = gestor_match.group(1).strip().rstrip('.')

    # Administradora: "administrado por X." ou "administrado por X,"
    admin_match = re.search(
        r'administrad[oa] por\s+(.+?)\s*(?:\.|,|\s+Atualmente)',
        texto_completo, re.IGNORECASE
    )
    if admin_match:
        dados["administrador"] = admin_match.group(1).strip().rstrip('.')

    # Patrimônio Líquido: "patrimônio líquido de R$ 14,25 milhões"
    pl_match = re.search(
        r'patrim[ôo]nio l[íi]quido de\s*R\$\s*([\d.,]+)\s*(milh[õo]es|mil|bilh[õo]es|mi|bi|milhão)',
        texto_completo, re.IGNORECASE
    )
    if pl_match:
        valor_str = pl_match.group(1)
        unidade = pl_match.group(2).lower()
        if ',' in valor_str and '.' in valor_str:
            valor_str = valor_str.replace('.', '').replace(',', '.')
        elif ',' in valor_str:
            valor_str = valor_str.replace(',', '.')
        try:
            valor = float(valor_str)
            if "bilh" in unidade:
                valor *= 1_000_000_000
            elif "milh" in unidade:
                valor *= 1_000_000
            elif "mil" == unidade or unidade == "mil":
                valor *= 1_000
            dados["patrimonio_liquido"] = valor
        except (ValueError, TypeError):
            pass

    # Inadimplência: "inadimplência da carteira está em 1,99%" ou "em 1,99%"
    inad_match = re.search(
        r'inadimpl[êe]ncia\s*(?:da carteira\s*)?est[áa] em\s*([\d.,]+)\s*%',
        texto_completo, re.IGNORECASE
    )
    if inad_match:
        dados["inadimplencia_90d"] = inad_match.group(1) + "%"

    # PDD: "PDD) corresponde a 0,90%" ou "PDD ... 0,90%"
    pdd_match = re.search(
        r'PDD\)\s*corresponde a\s*([\d.,]+)\s*%',
        texto_completo, re.IGNORECASE
    )
    if not pdd_match:
        pdd_match = re.search(
            r'provis[ãa]o para devedores duvidosos\s*\(PDD\)\s*corresponde a\s*([\d.,]+)\s*%',
            texto_completo, re.IGNORECASE
        )
    if pdd_match:
        dados["pdd"] = pdd_match.group(1) + "%"

    # Subordinação: "subordinação de 100,00%"
    sub_match = re.search(
        r'[íi]ndice de subordina[çc][ãa]o de\s*([\d.,]+)\s*%',
        texto_completo, re.IGNORECASE
    )
    if sub_match:
        dados["subordinacao_atual"] = sub_match.group(1) + "%"

    # Situação: "em funcionamento normal" ou "está ativo"
    if re.search(r'funcionamento normal', texto_completo, re.IGNORECASE):
        dados["situacao"] = "Em Funcionamento Normal"
    elif re.search(r'est[áa] ativo', texto_completo, re.IGNORECASE):
        dados["situacao"] = "Ativo"
    elif re.search(r'cancelado', texto_completo, re.IGNORECASE):
        dados["situacao"] = "Cancelado"
    elif re.search(r'em liquida[çc][ãa]o', texto_completo, re.IGNORECASE):
        dados["situacao"] = "Em Liquidação"

    # Nome do fundo: "O BELSINOS (CNPJ ...) é um Fundo de Investimento"
    nome_match = re.search(
        r'(?:^|\n)\s*(.+?)\s*\(CNPJ\s*[\d./-]+\)\s*é um Fundo',
        texto_completo
    )
    if nome_match:
        nome = nome_match.group(1).strip()
        # Remove "O " / "A " inicial se houver
        if nome.startswith("O ") or nome.startswith("A "):
            nome = nome[2:]
        dados["nome_fundo_cvm"] = nome

    # Cotistas: "conta com X cotistas" ou "conta atualmente com X cotistas"
    cotistas_match = re.search(
        r'conta\s*(?:atualmente\s*)?com\s*(\d+)\s*cotistas',
        texto_completo, re.IGNORECASE
    )
    if cotistas_match:
        dados["numero_cotistas"] = int(cotistas_match.group(1))

    # --- Fallback: se algum campo não foi encontrado no texto descritivo,
    # tenta buscar pelo label ALL CAPS na estrutura ---
    def _buscar_valor_exato(label, linhas):
        import unicodedata
        def norm(t):
            t = unicodedata.normalize('NFD', t)
            t = ''.join(c for c in t if unicodedata.category(c) != 'Mn')
            return t.upper().strip()
        norm_label = norm(label)
        for i, linha in enumerate(linhas):
            if norm(linha) == norm_label:
                for j in range(i + 1, min(i + 3, len(linhas))):
                    valor = linhas[j].strip()
                    if valor and valor != "—" and valor != "-" and len(valor) < 200:
                        return valor
        return None

    if not dados.get("gestor"):
        v = _buscar_valor_exato("GESTORA", linhas_limpas)
        if v: dados["gestor"] = v
    if not dados.get("administrador"):
        v = _buscar_valor_exato("ADMINISTRADORA", linhas_limpas)
        if v: dados["administrador"] = v
    if not dados.get("inadimplencia_90d"):
        v = _buscar_valor_exato("INADIMPLÊNCIA", linhas_limpas)
        if v: dados["inadimplencia_90d"] = v
    if not dados.get("pdd"):
        v = _buscar_valor_exato("PDD", linhas_limpas)
        if v: dados["pdd"] = v
    if not dados.get("subordinacao_atual"):
        v = _buscar_valor_exato("SUBORDINAÇÃO", linhas_limpas)
        if v: dados["subordinacao_atual"] = v
    if not dados.get("patrimonio_liquido"):
        v = _buscar_valor_exato("PATRIMÔNIO LÍQUIDO", linhas_limpas)
        if v:
            pl_fb = re.search(r'R\$\s*([\d.,]+)\s*(mi|bi|mil|bilhões|milhões)', v, re.IGNORECASE)
            if pl_fb:
                vs = pl_fb.group(1)
                un = pl_fb.group(2).lower()
                if ',' in vs and '.' in vs:
                    vs = vs.replace('.', '').replace(',', '.')
                elif ',' in vs:
                    vs = vs.replace(',', '.')
                try:
                    val = float(vs)
                    if "bilh" in un: val *= 1_000_000_000
                    elif "milh" in un or un == "mi": val *= 1_000_000
                    elif un == "mil": val *= 1_000
                    dados["patrimonio_liquido"] = val
                except (ValueError, TypeError):
                    pass

    return dados

    def _buscar_valor(label, linhas, variacoes=None):
        """Busca o valor que aparece após o label no texto."""
        labels = [label] + (variacoes or [])
        for i, linha in enumerate(linhas):
            for lbl in labels:
                if lbl.lower() in linha.lower():
                    # Caso 1: valor na mesma linha (ex: "Gestora: XYZ Ltda")
                    if ":" in linha:
                        valor = linha.split(":", 1)[1].strip()
                        if valor and valor != "—" and valor != "-":
                            return valor
                    # Caso 2: valor na próxima linha
                    if i + 1 < len(linhas):
                        proxima = linhas[i + 1].strip()
                        if proxima and proxima != "—" and proxima != "-" and len(proxima) < 200:
                            if not any(lbl2.lower() in proxima.lower() for lbl2 in ["gestor", "administr", "inadimpl", "subordin", "PDD", "provis", "patrimônio", "variação", "cotistas", "situação", "classe", "tipo"]):
                                return proxima
                    # Caso 3: valor 2 linhas depois
                    if i + 2 < len(linhas):
                        proxima = linhas[i + 2].strip()
                        if proxima and proxima != "—" and proxima != "-" and len(proxima) < 200:
                            if not any(lbl2.lower() in proxima.lower() for lbl2 in ["gestor", "administr", "inadimpl", "subordin", "PDD", "provis", "patrimônio", "variação", "cotistas", "situação", "classe", "tipo"]):
                                return proxima
        return None

    # Gestora/Gestor
    gestor = _buscar_valor("GESTORA", linhas_limpas, ["Gestor"])
    if gestor:
        dados["gestor"] = gestor

    # Administradora
    admin = _buscar_valor("ADMINISTRADORA", linhas_limpas, ["Administrador"])
    if admin:
        dados["administrador"] = admin

    # Inadimplência
    inad = _buscar_valor("INADIMPLÊNCIA", linhas_limpas, ["Inadimplencia"])
    if inad:
        dados["inadimplencia_90d"] = inad

    # PDD
    pdd = _buscar_valor("PDD", linhas_limpas, ["Provisão para Devedores Duvidosos", "Provisao para Devedores"])
    if pdd:
        dados["pdd"] = pdd

    # Subordinação
    sub = _buscar_valor("SUBORDINAÇÃO", linhas_limpas, ["Subordinacao"])
    if sub:
        dados["subordinacao_atual"] = sub

    # Patrimônio Líquido
    for linha in linhas_limpas:
        pl_match = re.search(r'R\$\s*([\d.,]+)\s*(mi|bi|mil|bilhões|milhões)', linha)
        if pl_match:
            valor_str = pl_match.group(1)
            unidade = pl_match.group(2).lower()
            if ',' in valor_str and '.' in valor_str:
                valor_str = valor_str.replace('.', '').replace(',', '.')
            elif ',' in valor_str:
                valor_str = valor_str.replace(',', '.')
            try:
                valor = float(valor_str)
                if unidade in ("bi", "bilhões"):
                    valor *= 1_000_000_000
                elif unidade in ("mi", "milhões"):
                    valor *= 1_000_000
                elif unidade == "mil":
                    valor *= 1_000
                dados["patrimonio_liquido"] = valor
            except (ValueError, TypeError):
                pass
            break

    # Situação
    for linha in linhas_limpas:
        if "Em Funcionamento Normal" in linha:
            dados["situacao"] = "Em Funcionamento Normal"
            break
        if "Cancelado" in linha and "Funcionamento" not in linha:
            dados["situacao"] = "Cancelado"
            break
        if "Em Liquidação" in linha:
            dados["situacao"] = "Em Liquidação"
            break
        if "Fase Pré-Operacional" in linha:
            dados["situacao"] = "Fase Pré-Operacional"
            break

    # Variação mensal
    var = _buscar_valor("VARIAÇÃO MENSAL", linhas_limpas, ["Variacao mensal"])
    if var:
        dados["variacao_pl_mensal"] = var

    # Número de cotistas
    cotistas = _buscar_valor("COTISTAS", linhas_limpas)
    if cotistas:
        try:
            dados["numero_cotistas"] = int(cotistas)
        except ValueError:
            pass

    return dados

def _extrair_dados_html_estatico(html):
    """Extrai dados do HTML estático (fallback quando Playwright não está disponível)."""
    dados = {}

    nome_match = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL)
    if nome_match:
        nome = _limpar_html(nome_match.group(1))
        if nome and "next_f" not in nome:
            dados["nome_fundo_cvm"] = nome

    pl_match = re.search(r'R\$\s*([\d.,]+)\s*(mi|bi|mil|bilhões|milhões)', html)
    if pl_match:
        valor_str = pl_match.group(1)
        unidade = pl_match.group(2).lower()
        if ',' in valor_str and '.' in valor_str:
            valor_str = valor_str.replace('.', '').replace(',', '.')
        elif ',' in valor_str:
            valor_str = valor_str.replace(',', '.')
        try:
            valor = float(valor_str)
            if unidade in ("bi", "bilhões"):
                valor *= 1_000_000_000
            elif unidade in ("mi", "milhões"):
                valor *= 1_000_000
            elif unidade == "mil":
                valor *= 1_000
            dados["patrimonio_liquido"] = valor
        except (ValueError, TypeError):
            pass

    sit_match = re.search(r'(Em Funcionamento Normal|Cancelado|Em Liquidação|Fase Pré-Operacional)', html, re.IGNORECASE)
    if sit_match:
        dados["situacao"] = sit_match.group(1)

    return dados

def _extrair_dados_html_estatico(html):
    """Extrai dados do HTML estático (fallback quando Playwright não está disponível)."""
    dados = {}

    nome_match = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL)
    if nome_match:
        nome = _limpar_html(nome_match.group(1))
        if nome and "next_f" not in nome:
            dados["nome_fundo_cvm"] = nome

    pl_match = re.search(r'R\$\s*([\d.,]+)\s*(mi|bi|mil|bilhões|milhões)', html)
    if pl_match:
        valor_str = pl_match.group(1)
        unidade = pl_match.group(2).lower()
        if ',' in valor_str and '.' in valor_str:
            valor_str = valor_str.replace('.', '').replace(',', '.')
        elif ',' in valor_str:
            valor_str = valor_str.replace(',', '.')
        try:
            valor = float(valor_str)
            if unidade in ("bi", "bilhões"):
                valor *= 1_000_000_000
            elif unidade in ("mi", "milhões"):
                valor *= 1_000_000
            elif unidade == "mil":
                valor *= 1_000
            dados["patrimonio_liquido"] = valor
        except (ValueError, TypeError):
            pass

    sit_match = re.search(r'(Em Funcionamento Normal|Cancelado|Em Liquidação|Fase Pré-Operacional)', html, re.IGNORECASE)
    if sit_match:
        dados["situacao"] = sit_match.group(1)

    return dados

# ---------------------------------------------------------------------------
# Buscadores externos
# ---------------------------------------------------------------------------

def buscar_fatos_relevantes_web(nome: str, dias: int = 60) -> str:
    """Busca fatos relevantes via Google News RSS com tratamento completo de erros."""
    if not nome or not nome.strip():
        return "Nome não fornecido — busca de fatos relevantes pulada."
    nome_curto = nome[:100].strip()
    query = quote_plus(f"{nome_curto} FIDC OR crédito OR inadimplência OR rating")
    url = f"https://news.google.com/rss/search?q={query}&hl=pt-BR&gl=BR&ceid=BR:pt-BR"
    resp = _request_com_retry(url)
    if resp is None:
        return f"Não foi possível buscar fatos relevantes (rede indisponível após {MAX_RETRIES} tentativas)."
    if resp.status_code != 200:
        return f"Falha na busca de notícias (HTTP {resp.status_code})."
    if not resp.content or len(resp.content) < 50:
        return "Resposta vazia do Google News."
    try:
        root = ET.fromstring(resp.content)
    except ET.ParseError as e:
        return f"Erro ao processar feed XML do Google News: {e}"
    items = root.findall(".//item")
    if not items:
        return f"Nenhum fato relevante encontrado sobre '{nome_curto}'."
    trechos = []
    data_limite = datetime.now() - timedelta(days=dias)
    itens_fora_periodo = 0
    itens_sem_data = 0
    for item in items[:15]:
        titulo = item.findtext("title", "") or "(sem título)"
        link = item.findtext("link", "") or ""
        pub_date_str = item.findtext("pubDate", "") or ""
        source = item.findtext("source", "N/A") or "N/A"
        if pub_date_str:
            try:
                pub_date = parsedate_to_datetime(pub_date_str)
                if pub_date and pub_date < data_limite:
                    itens_fora_periodo += 1
                    continue
            except (ValueError, TypeError):
                itens_sem_data += 1
        else:
            itens_sem_data += 1
        trechos.append(
            f"- {titulo}\n"
            f"  Fonte: {source}\n"
            f"  Data: {pub_date_str or 'data não disponível'}\n"
            f"  URL: {link or 'URL não disponível'}"
        )
    if not trechos:
        msg = f"Nenhum fato relevante nos últimos {dias} dias sobre '{nome_curto}'."
        if itens_fora_periodo > 0:
            msg += f" ({itens_fora_periodo} itens fora do período)"
        return msg
    resultado = f"Fatos relevantes (últimos {dias} dias):\n" + "\n\n".join(trechos)
    if itens_sem_data > 0:
        resultado += f"\n\n(Nota: {itens_sem_data} item(ns) sem data foram incluídos)"
    return resultado

def buscar_regulamento_web(cnpj_fundo: str, nome_fundo: str) -> str:
    """Busca regulamento via fidcs.com.br + CVM CSV + DuckDuckGo com tratamento completo."""
    trechos_coletados = []
    cnpj_limpo, _ = _validar_cnpj(cnpj_fundo)

    # 1. fidcs.com.br (dados cadastrais já processados)
    if cnpj_limpo:
        dados_fidcs = buscar_dados_fidcs_com_br(cnpj_fundo)
        if "erro" not in dados_fidcs:
            info_fidcs = (
                f"=== DADOS CADASTRAIS (fidcs.com.br) ===\n"
                f"Denominação: {dados_fidcs.get('nome_fundo_cvm', 'N/A')}\n"
                f"CNPJ: {cnpj_fundo}\n"
                f"Situação: {dados_fidcs.get('situacao', 'N/A')}\n"
                f"PL: R$ {dados_fidcs.get('patrimonio_liquido', 'N/A'):,.2f}\n"
                f"Gestor: {dados_fidcs.get('gestor', 'N/A')}\n"
                f"Administrador: {dados_fidcs.get('administrador', 'N/A')}\n"
                f"Subordinação: {dados_fidcs.get('subordinacao_atual', 'N/A')}\n"
                f"Inadimplência: {dados_fidcs.get('inadimplencia_90d', 'N/A')}\n"
                f"Fonte: {dados_fidcs.get('url_fonte', 'N/A')}\n"
            )
            trechos_coletados.append(info_fidcs)

    # 2. CVM — cad_fi.csv (não adaptados)
    if cnpj_limpo and not trechos_coletados:
        csv_cache_path = os.path.join(tempfile.gettempdir(), "cvm_fundos_cad.csv")
        cache_expirou = True
        if os.path.exists(csv_cache_path):
            try:
                idade = datetime.now().timestamp() - os.path.getmtime(csv_cache_path)
                if idade < 86400:
                    cache_expirou = False
            except OSError:
                pass
        if cache_expirou:
            print("[CVM] Baixando cad_fi.csv...")
            _baixar_csv_cvm(
                "https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv",
                csv_cache_path
            )
        if os.path.exists(csv_cache_path):
            resultado = _buscar_cnpj_no_csv(csv_cache_path, cnpj_limpo)
            if resultado:
                trechos_coletados.append(
                    f"=== DADOS CADASTRAIS CVM ===\n"
                    f"Denominação: {resultado.get('DENOM_SOCIAL', 'N/A')}\n"
                    f"CNPJ: {resultado.get('CNPJ_FUNDO', 'N/A')}\n"
                    f"Classe: {resultado.get('CLASSE', 'N/A')}\n"
                    f"Situação: {resultado.get('SIT', 'N/A')}\n"
                    f"Data de constituição: {resultado.get('DT_CONST', 'N/A')}\n"
                    f"Administrador: {resultado.get('ADMIN', 'N/A')}\n"
                    f"Gestor: {resultado.get('GESTOR', 'N/A')}\n"
                    f"Custodiante: {resultado.get('CUSTODIANTE', 'N/A')}\n"
                    f"Tipo: {resultado.get('TP_FUNDO', 'N/A')}\n"
                )
            else:
                # 3. CVM — registro_fundo_classe.zip (adaptados RCVM 175)
                csv_rcvm_path = os.path.join(tempfile.gettempdir(), "cvm_registro_fundo.csv")
                if not os.path.exists(csv_rcvm_path) or cache_expirou:
                    print("[CVM] Tentando registro_fundo_classe.zip (adaptados RCVM 175)...")
                    _baixar_e_extrair_zip_cvm(
                        "https://dados.cvm.gov.br/dados/FI/CAD/DADOS/registro_fundo_classe.zip",
                        csv_rcvm_path
                    )
                if os.path.exists(csv_rcvm_path):
                    resultado = _buscar_cnpj_no_csv(csv_rcvm_path, cnpj_limpo)
                    if resultado:
                        trechos_coletados.append(
                            f"=== DADOS CADASTRAIS CVM (RCVM 175) ===\n"
                            f"Denominação: {resultado.get('DENOM_SOCIAL', 'N/A')}\n"
                            f"CNPJ: {resultado.get('CNPJ_FUNDO', 'N/A')}\n"
                            f"Classe: {resultado.get('CLASSE', 'N/A')}\n"
                            f"Situação: {resultado.get('SIT', 'N/A')}\n"
                            f"Data de constituição: {resultado.get('DT_CONST', 'N/A')}\n"
                            f"Administrador: {resultado.get('ADMIN', 'N/A')}\n"
                            f"Gestor: {resultado.get('GESTOR', 'N/A')}\n"
                            f"Custodiante: {resultado.get('CUSTODIANTE', 'N/A')}\n"
                            f"Tipo: {resultado.get('TP_FUNDO', 'N/A')}\n"
                        )

    # 4. DuckDuckGo HTML
    if nome_fundo and nome_fundo.strip():
        nome_curto = nome_fundo[:80].strip()
        queries = [
            f'"{nome_curto}" regulamento subordinação mínima cláusula',
            f'"{nome_curto}" FIDC regulamento inadimplência máxima limite',
            f'"{nome_curto}" FIDC regulamento amortização antecipada evento',
        ]
        for query in queries:
            ddg_url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
            resp = _request_com_retry(ddg_url)
            if resp is None or resp.status_code != 200:
                continue
            html = resp.text
            if "captcha" in html.lower() or "blocked" in html.lower() or "anomaly" in html.lower():
                print("  [DDG] CAPTCHA/bloqueio detectado — pulando")
                continue
            titulos = re.findall(r'class="result__a"[^>]*>(.*?)</a>', html, re.DOTALL)
            snippets = re.findall(r'class="result__snippet"[^>]*>(.*?)</a>', html, re.DOTALL)
            for titulo, snippet in zip(titulos[:3], snippets[:3]):
                titulo_limpo = _limpar_html(titulo)
                snippet_limpo = _limpar_html(snippet)
                if titulo_limpo or snippet_limpo:
                    trechos_coletados.append(f"- {titulo_limpo}\n  {snippet_limpo[:600]}")
            if len(trechos_coletados) >= 6:
                break

    if not trechos_coletados:
        return ("Não foi possível obter o regulamento automaticamente. "
                "Forneça o texto do regulamento manualmente.")
    return "\n\n".join(trechos_coletados)

def buscar_informe_mensal_cvm(cnpj_fundo: str) -> dict:
    """
    Consulta dados do FIDC. Prioridade:
    1. fidcs.com.br (dados já processados, inclui inadimplência/PDD do Next.js)
    2. CVM CSV (cadastro + registro_fundo)
    3. CVM Medidas FIE (inadimplência e PDD mensais)
    4. DuckDuckGo (fallback)
    """
    cnpj_limpo, cnpj_formatado = _validar_cnpj(cnpj_fundo)
    if not cnpj_limpo:
        return {"erro": f"CNPJ inválido ou não fornecido: '{cnpj_fundo}'"}

    # --- PRIORIDADE 1: fidcs.com.br ---
    print(f"[CVM] Buscando no fidcs.com.br...")
    resultado = buscar_dados_fidcs_com_br(cnpj_fundo)
    if "erro" not in resultado:
        # Se fidcs.com.br não trouxe inadimplência/PDD, busca no CVM Medidas
        if not resultado.get("inadimplencia_90d") or resultado.get("inadimplencia_90d") == "N/A":
            print(f"[CVM] Inadimplência não encontrada no fidcs.com.br. Buscando no CVM Medidas FIE...")
            medidas = buscar_medidas_fie_cvm(cnpj_fundo)
            if "erro" not in medidas:
                if medidas.get("inadimplencia_90d") and medidas["inadimplencia_90d"] != "não informado":
                    resultado["inadimplencia_90d"] = medidas["inadimplencia_90d"]
                    print(f"[CVM-MEDIDAS] Inadimplência: {medidas['inadimplencia_90d']}")
                if medidas.get("pdd") and medidas["pdd"] != "não informado":
                    resultado["pdd"] = medidas["pdd"]
                    print(f"[CVM-MEDIDAS] PDD: {medidas['pdd']}")
        return resultado

    print(f"[CVM] fidcs.com.br falhou. Tentando CVM CSV...")

    # --- PRIORIDADE 2: CVM CSV ---
    csv_cache_path = os.path.join(tempfile.gettempdir(), "cvm_fundos_cad.csv")
    cache_expirou = True
    if os.path.exists(csv_cache_path):
        try:
            idade = datetime.now().timestamp() - os.path.getmtime(csv_cache_path)
            if idade < 86400:
                cache_expirou = False
        except OSError:
            pass
    if cache_expirou:
        print("[CVM] Baixando cad_fi.csv...")
        _baixar_csv_cvm("https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv", csv_cache_path)

    resultado_csv = None
    if os.path.exists(csv_cache_path):
        resultado_csv = _buscar_cnpj_no_csv(csv_cache_path, cnpj_limpo)

    # ARQUIVO 2: registro_fundo_classe.zip
    if not resultado_csv:
        csv_rcvm_path = os.path.join(tempfile.gettempdir(), "cvm_registro_fundo.csv")
        precisa_baixar = True
        if os.path.exists(csv_rcvm_path):
            try:
                idade = datetime.now().timestamp() - os.path.getmtime(csv_rcvm_path)
                if idade < 86400:
                    precisa_baixar = False
            except OSError:
                pass
        if precisa_baixar:
            print("[CVM] Baixando registro_fundo_classe.zip...")
            _baixar_e_extrair_zip_cvm(
                "https://dados.cvm.gov.br/dados/FI/CAD/DADOS/registro_fundo_classe.zip",
                csv_rcvm_path
            )
        if os.path.exists(csv_rcvm_path):
            resultado_csv = _buscar_cnpj_no_csv(csv_rcvm_path, cnpj_limpo)

    # --- PRIORIDADE 3: CVM Medidas FIE (inadimplência e PDD) ---
    medidas = buscar_medidas_fie_cvm(cnpj_fundo)

    # --- Monta resultado final ---
    if resultado_csv:
        pl_raw = resultado_csv.get("VL_PATRIM_LIQ")
        pl = None
        if pl_raw:
            try:
                pl = float(pl_raw)
            except (ValueError, TypeError):
                pl = None
        resultado_final = {
            "nome_fundo_cvm": resultado_csv.get("DENOM_SOCIAL", "N/A"),
            "patrimonio_liquido": pl,
            "data_referencia": resultado_csv.get("DT_PATRIM_LIQ", "N/A"),
            "fonte": "CVM (dados.cvm.gov.br)",
            "situacao": resultado_csv.get("SIT", "N/A"),
            "classe": resultado_csv.get("CLASSE", "N/A"),
            "gestor": resultado_csv.get("GESTOR", "N/A"),
            "administrador": resultado_csv.get("ADMIN", "N/A"),
            "tipo_fundo": resultado_csv.get("TP_FUNDO", "N/A"),
            "inadimplencia_90d": "verificar informe mensal",
            "subordinacao_atual": "verificar informe mensal",
        }
        # Adiciona inadimplência e PDD das medidas FIE
        if "erro" not in medidas:
            if medidas.get("inadimplencia_90d") and medidas["inadimplencia_90d"] != "não informado":
                resultado_final["inadimplencia_90d"] = medidas["inadimplencia_90d"]
            if medidas.get("pdd") and medidas["pdd"] != "não informado":
                resultado_final["pdd"] = medidas["pdd"]
        return resultado_final

    # --- PRIORIDADE 4: DuckDuckGo ---
    print(f"[CVM] CNPJ não encontrado. Buscando via DuckDuckGo...")
    queries = [
        f'"{cnpj_formatado}" CVM fundo cadastro',
        f'"{cnpj_limpo}" CVM FIDC informe mensal inadimplência',
        f'"{cnpj_formatado}" FIDC subordinação patrimônio',
    ]
    resultados = []
    for query in queries:
        ddg_url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
        resp = _request_com_retry(ddg_url)
        if resp is None or resp.status_code != 200:
            continue
        html = resp.text
        if "captcha" in html.lower() or "blocked" in html.lower():
            continue
        titulos = re.findall(r'class="result__a"[^>]*>(.*?)</a>', html, re.DOTALL)
        snippets = re.findall(r'class="result__snippet"[^>]*>(.*?)</a>', html, re.DOTALL)
        for titulo, snippet in zip(titulos[:3], snippets[:3]):
            t = _limpar_html(titulo)
            s = _limpar_html(snippet)
            if t or s:
                resultados.append(f"- {t}\n  {s[:500]}")
        if len(resultados) >= 3:
            break
    if resultados:
        return {
            "nome_fundo_cvm": "N/A (encontrado via busca web)",
            "inadimplencia_90d": "não informado",
            "subordinacao_atual": "não informado",
            "patrimonio_liquido": None,
            "data_referencia": "N/A",
            "fonte": "DuckDuckGo (fallback)",
            "resultados_web": "\n".join(resultados),
        }
    return {"erro": f"Não foi possível obter dados da CVM para CNPJ {cnpj_formatado}."}

# ---------------------------------------------------------------------------
# Nós do grafo
# ---------------------------------------------------------------------------

def input_node(state: CreditState) -> CreditState:
    operacao = state["operacao"]
    is_fidc = operacao.get("tipo", "").lower() in ("fidc", "fundo_de_investimento_em_direitos_creditorios")
    return {"is_fidc": is_fidc}

def risk_scoring(state: CreditState) -> CreditState:
    operacao = state["operacao"]
    nome = operacao.get("nome", "N/A")
    try:
        fatos_relevantes = buscar_fatos_relevantes_web(nome, dias=60)
    except Exception as e:
        fatos_relevantes = f"Erro inesperado ao buscar fatos relevantes: {e}"
    prompt = f"""
Você é um analista de crédito sênior. Classifique o risco da seguinte
operação em "baixo", "medio" ou "alto", com base nos dados fornecidos.

Dados da operação:
{json.dumps(operacao, ensure_ascii=False, indent=2, default=str)}

FATOS RELEVANTES DOS ÚLTIMOS 60 DIAS:
{fatos_relevantes}

Considere: endividamento em relação ao faturamento, histórico de
inadimplência (se houver), rating (se houver) e os fatos relevantes
acima. Se faltar informação, seja conservador na classificação.

RESPONDA APENAS com um JSON válido no seguinte formato, sem markdown,
sem texto adicional, sem comentários:
{{
  "nivel": "baixo|medio|alto",
  "justificativa": "sua justificativa em 1-2 frases"
}}
"""
    resposta = _invocar_llm_com_retry(prompt)
    if resposta is None:
        return {
            "risk_level": "alto",
            "risk_justificativa": "LLM indisponível após múltiplas tentativas. Classificação conservadora.",
            "contexto_externo": fatos_relevantes,
        }
    dados = _extrair_json(resposta)
    if dados:
        nivel = dados.get("nivel", "medio").lower()
        if nivel not in ("baixo", "medio", "alto"):
            nivel = "medio"
        justificativa = dados.get("justificativa", "Sem justificativa.")
    else:
        nivel = "medio"
        justificativa = f"Não foi possível extrair JSON. Resposta bruta: {resposta[:200]}"
    return {
        "risk_level": nivel,
        "risk_justificativa": justificativa,
        "contexto_externo": fatos_relevantes,
    }

def busca_contexto(state: CreditState) -> CreditState:
    if state.get("contexto_externo"):
        return {}
    tomador = state["operacao"].get("nome", "tomador desconhecido")
    try:
        contexto = buscar_fatos_relevantes_web(tomador, dias=60)
    except Exception as e:
        contexto = f"Erro ao buscar contexto: {e}"
    return {"contexto_externo": contexto}

def buscar_regulamento_node(state: CreditState) -> CreditState:
    operacao = state["operacao"]
    nome_fundo = operacao.get("nome", "N/A")
    cnpj_fundo = operacao.get("cnpj", "")
    regulamento_existente = operacao.get("regulamento_texto", "")
    if regulamento_existente and len(regulamento_existente) > 200:
        try:
            informe = buscar_informe_mensal_cvm(cnpj_fundo) if cnpj_fundo else {"erro": "CNPJ não fornecido"}
        except Exception as e:
            informe = {"erro": f"Erro ao buscar informe CVM: {e}"}
        return {"regulamento_texto": regulamento_existente, "informe_cvm": informe}
    try:
        regulamento = buscar_regulamento_web(cnpj_fundo, nome_fundo)
    except Exception as e:
        regulamento = f"Erro ao buscar regulamento: {e}"
    try:
        informe = buscar_informe_mensal_cvm(cnpj_fundo) if cnpj_fundo else {"erro": "CNPJ não fornecido"}
    except Exception as e:
        informe = {"erro": f"Erro ao buscar informe CVM: {e}"}
    return {"regulamento_texto": regulamento, "informe_cvm": informe}

def analyze_covenants(state: CreditState) -> CreditState:
    operacao = state["operacao"]
    regulamento = state.get("regulamento_texto", operacao.get("regulamento_texto", ""))
    informe_cvm = state.get("informe_cvm", {})
    if not isinstance(informe_cvm, dict):
        informe_cvm = {}
    indicadores_usuario = operacao.get("indicadores_atuais", {})
    if not isinstance(indicadores_usuario, dict):
        indicadores_usuario = {}
    indicadores = {
        "subordinacao_atual": informe_cvm.get("subordinacao_atual") or indicadores_usuario.get("subordinacao_atual", "não informado"),
        "inadimplencia_90d": informe_cvm.get("inadimplencia_90d") or indicadores_usuario.get("inadimplencia_90d", "não informado"),
        "patrimonio_liquido": informe_cvm.get("patrimonio_liquido"),
        "data_referencia_cvm": informe_cvm.get("data_referencia", "N/A"),
        "fonte_cvm": informe_cvm.get("fonte", "não disponível"),
    }
    prompt = f"""
Você é um analista de estruturação de crédito sênior. Abaixo estão trechos
do regulamento de um FIDC e os números atuais da carteira. Identifique os
principais covenants (subordinação mínima, índice de inadimplência máximo,
eventos de amortização antecipada) e diga se algum foi violado. Liste cada
violação individualmente com severidade.

Regulamento (trechos obtidos automaticamente):
{regulamento}

Indicadores atuais da carteira:
{json.dumps(indicadores, ensure_ascii=False, indent=2, default=str)}

RESPONDA APENAS com um JSON válido no seguinte formato, sem markdown,
sem texto adicional, sem comentários:
{{
  "violado": true,
  "detalhe": "descrição resumida da violação ou 'nenhuma violação identificada'",
  "covenants_identificados": ["cláusula 1", "cláusula 2", "cláusula 3"],
  "violacoes": [
    {{
      "clausula": "nome/numero da cláusula",
      "detalhe": "esperado X, atual Y",
      "severidade": "alta|media|baixa"
    }}
  ]
}}

Se não houver violações, use "violado": false e "violacoes": [].
"""
    resposta = _invocar_llm_com_retry(prompt)
    if resposta is None:
        return {
            "covenant_violado": False,
            "covenant_detalhe": "Análise de covenants não realizada — LLM indisponível.",
            "covenants_extraidos": [],
            "violacoes_lista": [],
        }
    dados = _extrair_json(resposta)
    if dados:
        violado = bool(dados.get("violado", False))
        detalhe = dados.get("detalhe", "nenhuma violação identificada")
        covenants = dados.get("covenants_identificados", [])
        violacoes = dados.get("violacoes", [])
        if not isinstance(covenants, list):
            covenants = []
        if not isinstance(violacoes, list):
            violacoes = []
    else:
        violado = False
        detalhe = f"Não foi possível extrair JSON. Resposta bruta: {resposta[:300]}"
        covenants = []
        violacoes = []
    return {
        "covenant_violado": violado,
        "covenant_detalhe": detalhe,
        "covenants_extraidos": covenants,
        "violacoes_lista": violacoes,
    }

def comparar_historico(state):
    """Nó: compara indicadores atuais com o histórico e detecta deterioração."""
    operacao = state["operacao"]
    cnpj_fundo = operacao.get("cnpj", "")
    informe = state.get("informe_cvm", {})
    if not isinstance(informe, dict) or "erro" in informe or not cnpj_fundo:
        return {"alertas_deterioracao": []}

    cnpj_limpo, _ = _validar_cnpj(cnpj_fundo)
    ultimo = _buscar_ultimo_historico(cnpj_limpo) if cnpj_limpo else None
    alertas = detectar_deterioracao(informe, ultimo)
    salvar_historico_indicadores(cnpj_fundo, informe, alertas)

    if alertas:
        print("\n[ALERTA DE DETERIORAÇÃO]")
        for a in alertas:
            print(f"  {a}")
    elif ultimo:
        print("[HISTÓRICO] Nenhuma deterioração em relação à última análise.")
    else:
        print("[HISTÓRICO] Primeira análise deste fundo — snapshot salvo como base.")

    return {"alertas_deterioracao": alertas}

def gerar_alerta(state):
    violacoes = state.get("violacoes_lista", [])
    if not isinstance(violacoes, list):
        violacoes = []
    alertas_det = state.get("alertas_deterioracao", [])
    if not isinstance(alertas_det, list):
        alertas_det = []
    detalhe = state.get("covenant_detalhe", "covenant não especificado")

    linhas_alerta = []
    if state.get("covenant_violado"):
        linhas_alerta.append(f"⚠️ ALERTA DE COVENANT: {detalhe}")
        if violacoes:
            linhas_alerta.append("\nViolações detalhadas:")
            for v in violacoes:
                if not isinstance(v, dict):
                    continue
                severidade = v.get("severidade", "")
                severidade_emoji = {"alta": "🔴", "media": "🟡", "baixa": "🟢"}.get(severidade, "⚪")
                linhas_alerta.append(
                    f"  {severidade_emoji} {v.get('clausula', 'N/A')}: {v.get('detalhe', 'N/A')} "
                    f"(severidade: {severidade or 'N/A'})"
                )
                
    if alertas_det:
        linhas_alerta.append("\n⚠️ ALERTA DE DETERIORAÇÃO (comparação histórica):")
        for a in alertas_det:
            linhas_alerta.append(f"  {a}")

    alerta = "\n".join(linhas_alerta)
    print(alerta)

    webhook_url = os.environ.get("RADAR_WEBHOOK_URL")
    if webhook_url:
        try:
            requests.post(
                webhook_url,
                json={
                    "alert_type": "covenant_violation",
                    "message": alerta,
                    "violacoes": violacoes,
                    "deterioracao": alertas_det,
                },
                timeout=10,
            )
        except requests.RequestException:
            pass
    return {"covenant_detalhe": alerta}

def parecer_final(state: CreditState) -> CreditState:
    partes = [
        f"Operação: {state['operacao'].get('nome', 'N/A')}",
        f"Risco: {state.get('risk_level', 'N/A')} — {state.get('risk_justificativa', '')}",
    ]
    if state.get("contexto_externo"):
        contexto = state["contexto_externo"]
        if len(contexto) > 1000:
            contexto = contexto[:1000] + "..."
        partes.append(f"Contexto externo (60d): {contexto}")
    if state.get("is_fidc"):
        covenants = state.get("covenants_extraidos", [])
        partes.append(f"Covenants analisados: {covenants}")
        informe = state.get("informe_cvm", {})
        if isinstance(informe, dict) and "erro" not in informe:
            pl = informe.get("patrimonio_liquido")
            if pl:
                partes.append(f"Informe: Inadimplência: {informe.get('inadimplencia_90d', 'N/A')}, Subordinação: {informe.get('subordinacao_atual', 'N/A')}, PL: R$ {pl:,.2f}")
            else:
                partes.append(f"Informe: Inadimplência: {informe.get('inadimplencia_90d', 'N/A')}, Subordinação: {informe.get('subordinacao_atual', 'N/A')}")
        if state.get("covenant_violado"):
            violacoes = state.get("violacoes_lista", [])
            partes.append(f"⚠️ Violação detectada: {state.get('covenant_detalhe')}")
            if violacoes:
                partes.append(f"Total de violações: {len(violacoes)}")
        else:
            partes.append("Covenants: nenhuma violação identificada.")

        if state.get("alertas_deterioracao"):
            alertas_det = state.get("alertas_deterioracao", [])
            partes.append(f"⚠️ Deterioração vs. análise anterior: {'; '.join(alertas_det)}")
    resumo = "\n".join(partes)
    prompt = f"""
Com base nas informações abaixo, escreva um parecer final curto (3-5
frases) para um comitê de crédito, em tom profissional e objetivo.
Se houver alerta de covenant, deixe isso claro logo no início.
Mencione a fonte dos dados (fidcs.com.br, CVM, Google News) quando relevante.

Informações apuradas:
{resumo}
"""
    parecer = _invocar_llm_com_retry(prompt)
    if parecer is None:
        parecer = "Não foi possível gerar o parecer automático. Dados apurados:\n" + resumo
    state["parecer_final"] = parecer
    registro_id = salvar_parecer(state)
    return {"parecer_final": parecer, "registro_id": registro_id}

# ---------------------------------------------------------------------------
# Roteadores
# ---------------------------------------------------------------------------
def roteador_1(state: CreditState) -> str:
    if state.get("is_fidc"):
        return "buscar_regulamento"
    if state.get("risk_level") == "baixo":
        return "parecer_final"
    return "busca_contexto"

def roteador_2(state):
    if state.get("covenant_violado") or state.get("alertas_deterioracao"):
        return "gerar_alerta"
    return "parecer_final"

# ---------------------------------------------------------------------------
# Grafo
# ---------------------------------------------------------------------------
def build_graph():
    graph = StateGraph(CreditState)
    graph.add_node("input_node", input_node)
    graph.add_node("risk_scoring", risk_scoring)
    graph.add_node("busca_contexto", busca_contexto)
    graph.add_node("buscar_regulamento", buscar_regulamento_node)
    graph.add_node("analyze_covenants", analyze_covenants)
    graph.add_node("comparar_historico", comparar_historico)  # NOVO
    graph.add_node("gerar_alerta", gerar_alerta)
    graph.add_node("parecer_final", parecer_final)
    graph.set_entry_point("input_node")
    graph.add_edge("input_node", "risk_scoring")
    graph.add_conditional_edges("risk_scoring", roteador_1, {
        "parecer_final": "parecer_final",
        "busca_contexto": "busca_contexto",
        "buscar_regulamento": "buscar_regulamento",
    })
    graph.add_edge("busca_contexto", "parecer_final")
    graph.add_edge("buscar_regulamento", "analyze_covenants")
    graph.add_edge("analyze_covenants", "comparar_historico")  # NOVO: covenants → histórico
    graph.add_conditional_edges("comparar_historico", roteador_2, {  # roteador sai do histórico agora
        "gerar_alerta": "gerar_alerta",
        "parecer_final": "parecer_final",
    })
    graph.add_edge("gerar_alerta", "parecer_final")
    graph.add_edge("parecer_final", END)
    return graph.compile()

# ---------------------------------------------------------------------------
# Coleta manual
# ---------------------------------------------------------------------------
def _perguntar(mensagem: str, obrigatorio: bool = True) -> str:
    while True:
        valor = input(mensagem).strip()
        if valor or not obrigatorio:
            return valor
        print("Esse campo é obrigatório, tente de novo.")

def coletar_operacao_manual() -> dict:
    print("\n--- Nova operação ---")
    nome = _perguntar("Nome da empresa/tomador/FIDC: ")
    tipo_input = _perguntar("É um FIDC? (s/n): ").lower()
    is_fidc = tipo_input.startswith("s")
    operacao: dict = {"nome": nome, "tipo": "fidc" if is_fidc else "tomador_direto"}
    if is_fidc:
        cnpj = _perguntar("CNPJ do fundo (ex: 12.345.678/0001-90): ", obrigatorio=False)
        operacao["cnpj"] = cnpj
        print("\nO regulamento será buscado automaticamente via fidcs.com.br + CVM + DuckDuckGo.")
        print("Se quiser fornecer trechos manualmente, cole abaixo (ou Enter para pular):")
        regulamento_manual = _perguntar("> ", obrigatorio=False)
        if regulamento_manual:
            operacao["regulamento_texto"] = regulamento_manual
        print("\nIndicadores atuais (opcionais):")
        subordinacao = _perguntar("Subordinação atual (ex: 11%): ", obrigatorio=False)
        inadimplencia = _perguntar("Inadimplência 90d atual (ex: 9.5%): ", obrigatorio=False)
        operacao["indicadores_atuais"] = {
            "subordinacao_atual": subordinacao or "não informado",
            "inadimplencia_90d": inadimplencia or "não informado",
        }
    else:
        faturamento = _perguntar("Faturamento anual (R$, só número): ", obrigatorio=False)
        divida = _perguntar("Dívida total (R$, só número): ", obrigatorio=False)
        rating = _perguntar("Rating de crédito (ou deixe em branco): ", obrigatorio=False)
        operacao["faturamento_anual"] = int(faturamento) if faturamento.isdigit() else None
        operacao["divida_total"] = int(divida) if divida.isdigit() else None
        operacao["rating"] = rating or None
    return operacao

# ---------------------------------------------------------------------------
# Execução
# ---------------------------------------------------------------------------
def rodar_operacao(app, operacao: dict) -> None:
    print(f"\n{'=' * 60}\n{operacao['nome']}\n{'=' * 60}")
    try:
        resultado = app.invoke({"operacao": operacao})
    except Exception as e:
        print(f"\n[ERRO FATAL] Falha ao executar o grafo: {e}")
        return
    print("\n--- PARECER FINAL ---")
    print(resultado.get("parecer_final", "Parecer não disponível."))
    if resultado.get("violacoes_lista"):
        print("\n--- VIOLAÇÕES DETALHADAS ---")
        for v in resultado["violacoes_lista"]:
            if isinstance(v, dict):
                print(f"  • {v.get('clausula', 'N/A')}: {v.get('detalhe', 'N/A')} (severidade: {v.get('severidade', 'N/A')})")
    informe = resultado.get("informe_cvm", {})
    if isinstance(informe, dict) and "erro" not in informe:
        print("\n--- DADOS DO FUNDO ---")
        print(f"  Fundo: {informe.get('nome_fundo_cvm', 'N/A')}")
        print(f"  Fonte: {informe.get('fonte', 'N/A')}")
        print(f"  Inadimplência: {informe.get('inadimplencia_90d', 'N/A')}")
        print(f"  PDD: {informe.get('pdd', 'N/A')}")
        print(f"  Subordinação: {informe.get('subordinacao_atual', 'N/A')}")
        if informe.get("patrimonio_liquido"):
            print(f"  PL: R$ {informe['patrimonio_liquido']:,.2f}")
        print(f"  Situação: {informe.get('situacao', 'N/A')}")
        print(f"  Gestor: {informe.get('gestor', 'N/A')}")
        print(f"  Administrador: {informe.get('administrador', 'N/A')}")

    elif isinstance(informe, dict) and "erro" in informe:
        print(f"\n[CVM] {informe['erro']}")
    if resultado.get("registro_id"):
        print(f"\n[Registrado no banco — ID: {resultado['registro_id']}]")
    else:
        print("\n[Aviso: parecer não foi salvo no banco de dados]")

def rodar_batch(app, arquivo_lista):
    """Processa vários FIDCs a partir de um arquivo .txt (um CNPJ por linha)."""
    try:
        with open(arquivo_lista, "r", encoding="utf-8") as f:
            linhas = [linha.strip() for linha in f if linha.strip() and not linha.strip().startswith("#")]
    except IOError as e:
        print(f"[LOTE] Erro ao ler arquivo: {e}")
        return

    if not linhas:
        print("[LOTE] Arquivo vazio — nada para processar.")
        return

    print(f"\n[LOTE] {len(linhas)} fundo(s) para analisar.")
    resultados = []

    for i, linha in enumerate(linhas, 1):
        # Aceita "CNPJ" ou "CNPJ;Nome do fundo"
        if ";" in linha:
            cnpj, nome = linha.split(";", 1)
            cnpj, nome = cnpj.strip(), nome.strip()
        else:
            cnpj, nome = linha, f"FIDC {linha}"

        cnpj_limpo, _ = _validar_cnpj(cnpj)
        if not cnpj_limpo:
            print(f"\n[LOTE] {i}/{len(linhas)} — CNPJ inválido: '{cnpj}' — pulando")
            continue

        print(f"\n[LOTE] {i}/{len(linhas)} — {cnpj}")
        operacao = {"nome": nome, "tipo": "fidc", "cnpj": cnpj}
        try:
            resultado = app.invoke({"operacao": operacao})
            resultados.append(resultado)
        except Exception as e:
            print(f"[LOTE] Falha ao processar {cnpj}: {e}")

    gerar_relatorio_consolidado(resultados)

def gerar_relatorio_consolidado(resultados):
    """Gera resumo executivo de todos os FIDCs analisados no lote."""
    if not resultados:
        print("\n[LOTE] Nenhum fundo foi processado com sucesso.")
        return

    total = len(resultados)
    violacoes = [r for r in resultados if r.get("covenant_violado")]
    deterioracoes = [r for r in resultados if r.get("alertas_deterioracao")]
    risco_alto = [r for r in resultados if r.get("risk_level") == "alto"]
    risco_medio = [r for r in resultados if r.get("risk_level") == "medio"]
    risco_baixo = [r for r in resultados if r.get("risk_level") == "baixo"]

    print("\n" + "=" * 60)
    print("RELATÓRIO CONSOLIDADO DO LOTE")
    print("=" * 60)
    print(f"\nTotal analisado:            {total}")
    print(f"Risco alto:                 {len(risco_alto)}")
    print(f"Risco médio:                {len(risco_medio)}")
    print(f"Risco baixo:                {len(risco_baixo)}")
    print(f"Violação de covenant:       {len(violacoes)}")
    print(f"Deterioração de indicadores: {len(deterioracoes)}")

    print("\n--- RESUMO POR FUNDO ---")
    for r in resultados:
        nome = r.get("operacao", {}).get("nome", "N/A")
        risco = r.get("risk_level", "N/A")
        informe = r.get("informe_cvm", {}) if isinstance(r.get("informe_cvm"), dict) else {}
        pl = informe.get("patrimonio_liquido")
        pl_str = f"R$ {pl:,.2f}" if pl else "N/A"
        flags = []
        if r.get("covenant_violado"):
            flags.append("VIOLAÇÃO")
        if r.get("alertas_deterioracao"):
            flags.append("DETERIORAÇÃO")
        flag_str = f"  ⚠️ {' + '.join(flags)}" if flags else ""
        print(f"  [{risco.upper():5}] {nome[:60]:60} PL: {pl_str}{flag_str}")

    criticos = [r for r in resultados if r.get("covenant_violado") or r.get("alertas_deterioracao")]
    if criticos:
        print("\n--- FUNDOS QUE EXIGEM AÇÃO IMEDIATA ---")
        for r in criticos:
            nome = r.get("operacao", {}).get("nome", "N/A")
            motivos = []
            if r.get("covenant_violado"):
                motivos.append("violação de covenant")
            if r.get("alertas_deterioracao"):
                motivos.append("deterioração de indicadores")
            print(f"  • {nome} — {'; '.join(motivos)}")

def ver_historico(cnpj):
    """Mostra a evolução dos indicadores de um fundo ao longo do tempo."""
    cnpj_limpo, _ = _validar_cnpj(cnpj)
    if not cnpj_limpo:
        print("CNPJ inválido.")
        return
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        cursor = conn.execute(
            """SELECT timestamp, patrimonio_liquido, variacao_pl, inadimplencia, pdd, subordinacao, alerta_deterioracao
               FROM historico_indicadores WHERE cnpj = ? ORDER BY id""",
            (cnpj_limpo,),
        )
        rows = cursor.fetchall()
        conn.close()
    except sqlite3.Error as e:
        print(f"[DB] Erro ao consultar histórico: {e}")
        return

    if not rows:
        print(f"\nNenhum histórico encontrado para o CNPJ {cnpj_limpo}.")
        print("Analise o fundo primeiro para criar a base de comparação.")
        return

    print(f"\n{'=' * 60}")
    print(f"HISTÓRICO DE INDICADORES — {cnpj_limpo}")
    print("=" * 60)
    for row in rows:
        ts, pl, var_pl, inad, pdd, sub, alerta = row
        data = ts[:19].replace("T", " ")
        pl_str = f"R$ {pl:,.2f}" if pl else "N/A"
        var_str = f"{var_pl:+.1f}%" if var_pl is not None else "—"
        print(f"\n  Data: {data}")
        print(f"    PL: {pl_str} (variação: {var_str})")
        print(f"    Inadimplência: {inad or 'N/A'} | PDD: {pdd or 'N/A'} | Subordinação: {sub or 'N/A'}")
        if alerta:
            print(f"    ⚠️ {alerta}")

if __name__ == "__main__":
    init_db()
    app = build_graph()
    print("Radar de Crédito Estruturado")
    print("=" * 60)

    while True:
        print("\n[1] Analisar uma operação (individual)")
        print("[2] Analisar lote de FIDCs (arquivo .txt com CNPJs)")
        print("[3] Ver histórico de indicadores de um fundo")
        print("[4] Sair")

        opcao = _perguntar("Escolha uma opção: ", obrigatorio=True).strip()

        if opcao == "1":
            operacao = coletar_operacao_manual()
            rodar_operacao(app, operacao)
        elif opcao == "2":
            arquivo = _perguntar("Caminho do arquivo .txt: ")
            rodar_batch(app, arquivo)
        elif opcao == "3":
            cnpj = _perguntar("CNPJ do fundo: ")
            ver_historico(cnpj)
        elif opcao == "4":
            print("Encerrando. Até!")
            break
        else:
            print("Opção inválida — digite 1, 2, 3 ou 4.")