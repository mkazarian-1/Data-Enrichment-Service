CREATE TABLE result
(
    id         BIGSERIAL PRIMARY KEY,
    message_id UUID        NOT NULL UNIQUE,
    user_id    BIGINT      NOT NULL,
    action     VARCHAR(64) NOT NULL,
    result     BOOLEAN     NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE outbox
(
    id          BIGSERIAL PRIMARY KEY,
    message_id  UUID         NOT NULL,
    exchange    VARCHAR(128) NOT NULL,
    routing_key VARCHAR(128) NOT NULL,
    payload     JSONB        NOT NULL,
    status      VARCHAR(16)  NOT NULL DEFAULT 'PENDING',
    attempts    INT          NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    sent_at     TIMESTAMPTZ  NULL
);

CREATE INDEX idx_outbox_pending ON outbox (created_at) WHERE status = 'PENDING';
