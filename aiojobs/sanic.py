from functools import wraps

from . import create_scheduler

__all__ = ('setup', 'spawn', 'get_scheduler', 'get_scheduler_from_app',
           'atomic')


def get_scheduler(request):
    scheduler = get_scheduler_from_app(request.app)
    if scheduler is None:
        raise RuntimeError(
            "Call aiojobs.aiohttp.setup() on application initialization")
    return scheduler


def get_scheduler_from_app(app):
    return getattr(app, 'AIOJOBS_SCHEDULER', None)


async def spawn(request, coro):
    return await get_scheduler(request).spawn(coro)


def atomic(coro):
    @wraps(coro)
    async def wrapper(request):
        job = await spawn(request, coro(request))
        return await job.wait()
    return wrapper


def setup(app, **kwargs):
    async def on_startup(app, loop):
        app.AIOJOBS_SCHEDULER = await create_scheduler(**kwargs)

    async def on_cleanup(app, loop):
        await app.AIOJOBS_SCHEDULER.close()

    app.register_listener(on_startup, 'before_server_start')
    app.register_listener(on_cleanup, 'before_server_stop')
