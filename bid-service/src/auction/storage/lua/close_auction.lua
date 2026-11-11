-- Encerra o leilão se o prazo (possivelmente estendido pelo anti-sniping) já passou.
-- Idempotente: várias tasks podem chamar ao mesmo tempo sem efeito duplicado.
--
-- KEYS[1] auction:{id}  KEYS[2] bids:{id}
-- ARGV[1] auctionId  ARGV[2] canal Pub/Sub  ARGV[3] MAXLEN aproximado do stream
--
-- Retorno: { kind, endsAt }, onde kind é CLOSED, ALREADY_CLOSED, PENDING ou NOT_LOADED.

local auctionKey, streamKey = KEYS[1], KEYS[2]

-- Inteiro como string sem notação científica (o tostring do Lua usaria 1e+15).
local function int(n) return string.format('%d', n) end

if redis.call('EXISTS', auctionKey) == 0 then
    return { 'NOT_LOADED', 0 }
end

local redisTime = redis.call('TIME')
local now = tonumber(redisTime[1]) * 1000 + math.floor(tonumber(redisTime[2]) / 1000)

local state = redis.call('HMGET', auctionKey, 'status', 'endsAt', 'currentPrice', 'leaderId', 'seq')
local status = state[1]
local endsAt = tonumber(state[2])
local seq = tonumber(state[5])

if status == 'CLOSED' then
    return { 'ALREADY_CLOSED', endsAt }
end
if now < endsAt then
    return { 'PENDING', endsAt }
end

local winnerId = ''
local finalPrice = 0
if seq > 0 then
    winnerId = state[4]
    finalPrice = tonumber(state[3])
end

redis.call('HSET', auctionKey, 'status', 'CLOSED', 'closedAt', int(now))

-- Vai no mesmo stream dos lances, logo depois do último seq: o consumer persiste na ordem certa.
redis.call('XADD', streamKey, 'MAXLEN', '~', ARGV[3], '0-' .. int(seq + 1),
    'type', 'CLOSED', 'seq', int(seq), 'winnerId', winnerId, 'finalPrice', int(finalPrice),
    'ts', int(now))

redis.call('PUBLISH', ARGV[2], cjson.encode({
    type = 'AUCTION_CLOSED', auctionId = ARGV[1], seq = int(seq), winnerId = winnerId,
    finalPrice = int(finalPrice), endsAt = int(endsAt), serverTime = int(now)
}))

return { 'CLOSED', endsAt }
