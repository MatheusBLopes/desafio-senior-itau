-- Aceita ou rejeita um lance de forma atômica. O Valkey executa um script por vez,
-- então todos os lances de um leilão são serializados aqui, sem lock distribuído.
--
-- KEYS[1] auction:{id}               hash com o estado vivo do leilão
-- KEYS[2] idem:{id}:<user>:<key>     resultado memorizado da idempotency key
-- KEYS[3] bids:{id}                  stream de eventos (write-behind para o Postgres)
-- ARGV[1] userId  ARGV[2] amount (centavos)  ARGV[3] fingerprint do payload
-- ARGV[4] TTL da idempotency key (s): de uma rejeição, a partir de agora; de um aceite, a partir do fim
--         do leilão (a key não pode expirar com o leilão aberto, ou o mesmo lance seria aceito de novo)
-- ARGV[5] MAXLEN aproximado do stream
-- ARGV[6] auctionId  ARGV[7] canal Pub/Sub  ARGV[8] idempotency key
--
-- Retorno: { kind, json }, onde kind é NEW, REPLAY, MISMATCH ou NOT_LOADED.

local auctionKey, idemKey, streamKey = KEYS[1], KEYS[2], KEYS[3]
local userId = ARGV[1]
local amount = tonumber(ARGV[2])
local fingerprint = ARGV[3]

local function int(n) return string.format('%d', n) end

-- 1. Idempotência: um retry devolve exatamente a resposta original.
local previous = redis.call('HMGET', idemKey, 'h', 'r')
if previous[1] then
    if previous[1] ~= fingerprint then
        return { 'MISMATCH', previous[2] }
    end
    return { 'REPLAY', previous[2] }
end

-- Estado ausente (restart do Valkey ou leilão expirado da memória): a app reidrata do Postgres.
if redis.call('EXISTS', auctionKey) == 0 then
    return { 'NOT_LOADED', '' }
end

-- Relógio único: o do servidor Valkey, nunca o do cliente ou da task.
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local f = redis.call('HMGET', auctionKey, 'status', 'endsAt', 'currentPrice', 'startingPrice',
    'minIncrement', 'seq', 'extendWindowMs', 'extendByMs')
local status = f[1]
local endsAt = tonumber(f[2])
local currentPrice = tonumber(f[3])
local startingPrice = tonumber(f[4])
local minIncrement = tonumber(f[5])
local seq = tonumber(f[6])
local extendWindowMs = tonumber(f[7])
local extendByMs = tonumber(f[8])

local function minimumNext()
    if seq == 0 then return startingPrice end
    return currentPrice + minIncrement
end

local function remember(result, expireAtMs)
    redis.call('HSET', idemKey, 'h', fingerprint, 'r', result)
    if expireAtMs then
        redis.call('PEXPIREAT', idemKey, int(expireAtMs))
    else
        redis.call('EXPIRE', idemKey, ARGV[4])
    end
    return { 'NEW', result }
end

-- Números saem como string nos JSONs: o cjson serializa com %.14g e arredondaria valores grandes.
local function reject(reason)
    return remember(cjson.encode({
        outcome = 'REJECTED', reason = reason, seq = int(seq),
        currentPrice = int(currentPrice), minimumNext = int(minimumNext()),
        endsAt = int(endsAt), serverTime = int(now)
    }))
end

-- 2. Status e prazo.
if status ~= 'OPEN' or now >= endsAt then
    return reject('AUCTION_CLOSED')
end

-- 3. Valor: o primeiro lance precisa cobrir o preço inicial; os demais, preço atual + incremento.
-- Lance igual ao atual fica abaixo do mínimo, então no empate vence quem chegou primeiro.
if amount < minimumNext() then
    return reject('BID_TOO_LOW')
end

-- 4. Aceite.
seq = redis.call('HINCRBY', auctionKey, 'seq', 1)
currentPrice = amount

-- 5. Anti-sniping: lance perto do fim empurra o encerramento.
local extended = false
if endsAt - now <= extendWindowMs then
    local newEndsAt = now + extendByMs
    if newEndsAt > endsAt then
        endsAt = newEndsAt
        extended = true
    end
end

redis.call('HSET', auctionKey, 'currentPrice', int(amount), 'leaderId', userId, 'endsAt', int(endsAt))

-- O ID do stream é 0-<seq>: crescente, sem colisão e permite XRANGE direto por seq.
redis.call('XADD', streamKey, 'MAXLEN', '~', ARGV[5], '0-' .. int(seq),
    'type', 'BID', 'seq', int(seq), 'userId', userId, 'amount', int(amount),
    'idemKey', ARGV[8], 'ts', int(now), 'endsAt', int(endsAt))

redis.call('PUBLISH', ARGV[7], cjson.encode({
    type = 'BID_ACCEPTED', auctionId = ARGV[6], seq = int(seq), userId = userId,
    amount = int(amount), minimumNext = int(minimumNext()), endsAt = int(endsAt),
    extended = extended, serverTime = int(now)
}))

return remember(cjson.encode({
    outcome = 'ACCEPTED', seq = int(seq), currentPrice = int(amount), minimumNext = int(minimumNext()),
    endsAt = int(endsAt), extended = extended, serverTime = int(now)
}), endsAt + tonumber(ARGV[4]) * 1000)
