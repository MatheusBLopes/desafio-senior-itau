-- Lances aceitos pelo Valkey que não couberam em bids porque colidiram com outro lance já gravado
-- (idempotency key reutilizada depois de expirar, ou Valkey que voltou atrás num failover). Antes eles
-- sumiam no ON CONFLICT DO NOTHING; agora ficam aqui, com métrica e alarme, para reconciliação.
create table bid_conflicts (
    auction_id       uuid         not null references auctions (id),
    seq              bigint       not null,
    user_id          varchar(64)  not null,
    amount           bigint       not null,
    idempotency_key  varchar(128) not null,
    accepted_at      timestamptz  not null,
    detected_at      timestamptz  not null default now(),
    -- O mesmo evento pode ser reentregue (XCLAIM): fica uma linha só.
    primary key (auction_id, seq, user_id, idempotency_key)
);
