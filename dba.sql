CREATE TABLE clientes_tokens (
    id SERIAL PRIMARY KEY,
    cliente_id INTEGER UNIQUE NOT NULL,
    token TEXT NOT NULL,
    ativo BOOLEAN DEFAULT TRUE,
    criado_em TIMESTAMP DEFAULT NOW(),
    atualizado_em TIMESTAMP DEFAULT NOW()
);

CREATE
OR REPLACE FUNCTION atualiza_timestamp_clientes_tokens() RETURNS TRIGGER AS $ $ BEGIN NEW.atualizado_em = NOW();

RETURN NEW;

END;

$ $ LANGUAGE plpgsql;

CREATE TRIGGER trg_atualiza_clientes_tokens BEFORE
UPDATE
    ON clientes_tokens FOR EACH ROW EXECUTE FUNCTION atualiza_timestamp_clientes_tokens();

CREATE TABLE dados_raw (
    id BIGSERIAL PRIMARY KEY,
    cliente_id INTEGER NOT NULL REFERENCES clientes_tokens(cliente_id),
    data_coleta TIMESTAMP DEFAULT NOW(),
    origem TEXT NOT NULL,
    payload JSONB NOT NULL,
    hash_conteudo TEXT UNIQUE
);

CREATE INDEX idx_dadosraw_cliente_data ON dados_raw (cliente_id, data_coleta DESC);

CREATE INDEX idx_dadosraw_payload_gin ON dados_raw USING GIN (payload jsonb_path_ops);

CREATE UNIQUE INDEX idx_dadosraw_cliente_hash ON dados_raw (cliente_id, hash_conteudo);