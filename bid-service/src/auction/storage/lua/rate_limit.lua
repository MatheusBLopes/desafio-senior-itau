-- Token bucket por usuário. Fica num script separado do lance porque a chave do usuário
-- cai em outro slot do cluster (não compartilha o hash tag do leilão).
--
-- KEYS[1] rl:{userId}
-- ARGV[1] capacidade (tokens)  ARGV[2] reposição (tokens/s)
--
-- Retorno: 1 se liberado, 0 se limitado.

local capacity = tonumber(ARGV[1])
local refillPerSec = tonumber(ARGV[2])

local redisTime = redis.call('TIME')
local now = tonumber(redisTime[1]) * 1000 + math.floor(tonumber(redisTime[2]) / 1000)

local bucket = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(bucket[1]) or capacity
local lastRefillMs = tonumber(bucket[2]) or now

tokens = math.min(capacity, tokens + (now - lastRefillMs) * refillPerSec / 1000)

local allowed = 0
if tokens >= 1 then
    tokens = tokens - 1
    allowed = 1
end

redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', string.format('%d', now))
redis.call('PEXPIRE', KEYS[1], math.ceil(capacity / refillPerSec * 1000) + 1000)
return allowed
