-- Radar de Crédito Estruturado — Schema SQLite
-- Execute: sqlite3 radar_credito.db < schema.sql

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
);

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
);

CREATE INDEX IF NOT EXISTS idx_pareceres_nome ON pareceres(nome_operacao);
CREATE INDEX IF NOT EXISTS idx_pareceres_risk ON pareceres(risk_level);
CREATE INDEX IF NOT EXISTS idx_pareceres_violado ON pareceres(covenant_violado);
CREATE INDEX IF NOT EXISTS idx_historico_cnpj ON historico_indicadores(cnpj);
CREATE INDEX IF NOT EXISTS idx_historico_ts ON historico_indicadores(timestamp);

CREATE VIEW IF NOT EXISTS vw_pareceres_alertas AS
SELECT id, timestamp, nome_operacao, risk_level,
       CASE WHEN covenant_violado = 1 THEN 'SIM' ELSE 'NÃO' END AS violado,
       inadimplencia, pdd, parecer_final
FROM pareceres
WHERE covenant_violado = 1
ORDER BY timestamp DESC;

CREATE VIEW IF NOT EXISTS vw_evolucao_fundo AS
SELECT cnpj, nome_fundo,
       COUNT(*) AS total_analises,
       MIN(timestamp) AS primeira_analise,
       MAX(timestamp) AS ultima_analise,
       MIN(patrimonio_liquido) AS pl_minimo,
       MAX(patrimonio_liquido) AS pl_maximo,
       COUNT(CASE WHEN alerta_deterioracao IS NOT NULL THEN 1 END) AS total_alertas
FROM historico_indicadores
GROUP BY cnpj, nome_fundo
ORDER BY ultima_analise DESC;