-- Carrega o estado de um leilão no Valkey a partir do Postgres (criação ou reidratação após restart).
-- Só grava se o hash ainda não existe: duas tasks reidratando ao mesmo tempo não se sobrescrevem,
-- e um estado vivo mais novo nunca é trocado por um mais antigo do banco.
--
-- Exceção: se o hash existe mas tem seq menor que o do banco, o Valkey perdeu lances já persistidos
-- (failover para uma réplica atrasada). Aceitar lances assim reusaria seqs que já estão no banco,
-- então o estado avança para o do Postgres.
--
-- KEYS[1] auction:{id}  KEYS[2] bids:{id}
-- ARGV: status, startingPrice, minIncrement, currentPrice, leaderId, seq, endsAt,
--       extendWindowMs, extendByMs, consumer group, auctionId, canal Pub/Sub
--
-- Retorno: 1 se carregou, 2 se alcançou o banco, 0 se já estava carregado e em dia.

local auctionKey, streamKey = KEYS[1], KEYS[2]
local seq = tonumber(ARGV[6])
local endsAt = tonumber(ARGV[7])
local status = ARGV[1]

-- Inteiro como string sem notação científica (o tostring do Lua usaria 1e+15).
local function int(n) return string.format('%d', n) end

-- Publica AUCTION_LOADED com o estado carregado, para as tasks trocarem o cache local.
local function publishLoaded()
    -- Avisa as tasks: o cache local do pré-filtro deve ser substituído (o preço pode ter voltado).
    local minimumNext = tonumber(ARGV[2])
    if seq > 0 then minimumNext = tonumber(ARGV[4]) + tonumber(ARGV[3]) end
    redis.call('PUBLISH', ARGV[12], cjson.encode({
        type = 'AUCTION_LOADED', auctionId = ARGV[11], seq = int(seq), userId = ARGV[5],
        amount = ARGV[4], minimumNext = int(minimumNext), endsAt = int(endsAt),
        status = status
    }))
end

if redis.call('EXISTS', auctionKey) == 1 then
    local live = redis.call('HMGET', auctionKey, 'seq', 'endsAt', 'status')
    if tonumber(live[1]) >= seq then
        return 0
    end
    endsAt = math.max(endsAt, tonumber(live[2]))
    status = live[3]
    redis.call('HSET', auctionKey, 'seq', ARGV[6], 'currentPrice', ARGV[4], 'leaderId', ARGV[5],
        'endsAt', int(endsAt))
    publishLoaded()
    return 2
end

redis.call('HSET', auctionKey,
    'status', ARGV[1], 'startingPrice', ARGV[2], 'minIncrement', ARGV[3],
    'currentPrice', ARGV[4], 'leaderId', ARGV[5], 'seq', ARGV[6], 'endsAt', ARGV[7],
    'extendWindowMs', ARGV[8], 'extendByMs', ARGV[9])

-- O grupo começa em 0 para que nenhum evento novo escape do consumer.
local created = redis.pcall('XGROUP', 'CREATE', streamKey, ARGV[10], '0', 'MKSTREAM')
if type(created) == 'table' and created.err and not string.find(created.err, 'BUSYGROUP') then
    return redis.error_reply(created.err)
end

publishLoaded()
return 1
