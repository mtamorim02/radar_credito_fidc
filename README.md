# 📊 Radar de Crédito Estruturado — FIDC

Agente de IA com LangGraph que automatiza a análise de risco de crédito de FIDCs (Fundos de Investimento em Direitos Creditórios), buscando dados na CVM e no fidcs.com.br, detectando violações de covenants e gerando pareceres automáticos para comitê de crédito.

## 🚀 Funcionalidades

- **Busca automática em 4 fontes** (ordem de prioridade):
  1. fidcs.com.br (dados da CVM processados, renderizados via Playwright)
  2. CVM CSV (cad_fi.csv + registro_fundo_classe.zip para fundos adaptados à RCVM 175)
  3. CVM Medidas FIE (inadimplência e PDD mensais)
  4. DuckDuckGo (fallback web)

- **Análise de covenants**: identifica subordinação mínima, inadimplência máxima e eventos de amortização antecipada no regulamento, comparando com os indicadores atuais do fundo

- **Comparação histórica**: salva snapshots dos indicadores (PL, inadimplência, PDD, subordinação) e alerta automaticamente se houver deterioração entre análises

- **Batch mode**: processa vários FIDCs de uma vez a partir de um arquivo `.txt` e gera relatório consolidado

- **Persistência em SQLite**: todos os pareceres e histórico de indicadores ficam salvos localmente

- **Fatos relevantes**: busca notícias dos últimos 60 dias via Google News RSS

- **Tratamento de erros abrangente**: timeout, rate limiting (429), CAPTCHA, encoding, ZIP corrompido, SQLite locked, JSON inválido do LLM, e mais

## 📋 Pré-requisitos

- Python 3.10+
- Chave de API do Groq (gratuita em [console.groq.com](https://console.groq.com))
- Playwright + Chromium (para renderização JavaScript do fidcs.com.br)

## 🔧 Instalação
```bash
# Clone o repositório
git clone https://github.com/mtamorim02/radar_credito_fidc.git
cd radar-credito-fidc

# Crie e ative o ambiente virtual
python -m venv venv
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Instale as dependências
pip install -r requirements.txt

# Instale o Chromium para o Playwright
python -m playwright install chromium

# Configure as variáveis de ambiente
cp .env.example .env
# Edite o .env e coloque sua GROQ_API_KEY
