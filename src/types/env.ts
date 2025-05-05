export interface Env{
    UPSTASH_REDIS_URL:string;
    UPSTASH_REDIS_TOKEN:string;
    UPSTASH_REDIS_STREAM_NAME:string;
    STREAMCONSUMER:DurableObjectNamespace
    NATS_CRED:string
    NATS_CRED_KEY_SEED:string
    NATS_CRED_JWT:string
    DRAGONFLY_REDIS_URL:string
    DRAGONFLY_REDIS_TOKEN:string
}