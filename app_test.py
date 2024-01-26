from fastapi import FastAPI
from starlette.requests import Request
from starlette.responses import Response

from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from fastapi_cache.decorator import cache

from redis import asyncio as aioredis

app = FastAPI()


@cache()
async def get_cache():
    return 1


@app.get("/")
@cache(expire=60)
async def index():
    return dict(hello="world")


@app.on_event("startup")
async def startup():
    redis = aioredis.from_url("redis://default:zZ6zRNfGhP9FRzwuk9wT6DHHhN3KhISc@redis-12726.c309.us-east-2-1.ec2.cloud.redislabs.com:12726")
    FastAPICache.init(RedisBackend(redis), prefix="fastapi-cache")
