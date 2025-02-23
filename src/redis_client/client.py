import redis
import redis.asyncio

async_redis_client = redis.asyncio.Redis()
redis_client = redis.Redis()


def redis_factory():
    return redis.Redis()
