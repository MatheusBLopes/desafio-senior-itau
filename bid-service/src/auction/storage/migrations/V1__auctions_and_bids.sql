-- Valores monetários em centavos (bigint): sem arredondamento e igual ao que o Lua manipula.

create table auctions (
    id                      uuid primary key,
    title                   varchar(200) not null,
    starting_price          bigint       not null check (starting_price > 0),
    min_increment           bigint       not null check (min_increment > 0),
    anti_sniping_window_ms  bigint       not null check (anti_sniping_window_ms >= 0),
    anti_sniping_extend_ms  bigint       not null check (anti_sniping_extend_ms >= 0),
    status                  varchar(16)  not null check (status in ('OPEN', 'CLOSED')),
    created_at              timestamptz  not null,
    -- Prazo vigente: começa no prazo original e acompanha as extensões do anti-sniping.
    ends_at                 timestamptz  not null,
    -- Projeção do estado vivo, atualizada pelo consumer do stream.
    current_price           bigint,
    leader_id               varchar(64),
    last_seq                bigint       not null default 0,
    closed_at               timestamptz,
    winner_id               varchar(64),
    final_price             bigint
);

create index idx_auctions_open on auctions (ends_at) where status = 'OPEN';

-- Só lances aceitos. As constraints são a segunda barreira contra duplicidade,
-- caso o consumer reprocesse um evento (ACK perdido, XCLAIM).
create table bids (
    auction_id       uuid         not null references auctions (id),
    seq              bigint       not null,
    user_id          varchar(64)  not null,
    amount           bigint       not null check (amount > 0),
    idempotency_key  varchar(128) not null,
    accepted_at      timestamptz  not null,
    primary key (auction_id, seq),
    constraint uq_bids_idempotency unique (auction_id, user_id, idempotency_key)
);
