-- Ids come from a sequence in blocks of 50 (Hibernate's pooled optimizer), not IDENTITY: with IDENTITY
-- Hibernate must round-trip every insert to learn the id, and cannot batch inserts at all.
CREATE SEQUENCE trades_id_seq INCREMENT BY 50;

CREATE TABLE trades (
    id               BIGINT PRIMARY KEY,
    dedupe_key       VARCHAR(64)  NOT NULL,
    client_order_id  VARCHAR(64)  NOT NULL,
    client_id        VARCHAR(64)  NOT NULL,
    quote_id         VARCHAR(64)  NOT NULL,
    pair             VARCHAR(6),
    side             VARCHAR(4),
    qty              BIGINT,
    price            BIGINT,          -- 1e-5 units, see Px
    status           VARCHAR(16)  NOT NULL,
    reason           VARCHAR(32),
    received_at      TIMESTAMPTZ  NOT NULL,
    quote_expires_at TIMESTAMPTZ,
    CONSTRAINT trades_dedupe_key_uq UNIQUE (dedupe_key)
);

-- the control run (idempotency off) can have several rows per client_order_id; this finds them
CREATE INDEX trades_client_order_id_idx ON trades (client_order_id);
