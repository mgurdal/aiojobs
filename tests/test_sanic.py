import asyncio

import pytest
from sanic import Sanic
from sanic import response
from sanic.views import HTTPMethodView

from aiojobs.sanic import atomic, get_scheduler, get_scheduler_from_app
from aiojobs.sanic import setup as aiojobs_setup
from aiojobs.sanic import spawn


async def test_plugin(test_client):
    job = None

    async def coro():
        await asyncio.sleep(10)

    async def handler(request):
        nonlocal job

        job = await spawn(request, coro())
        assert not job.closed
        return response.json({})

    app = Sanic(__name__)
    app.add_route(handler, '/')
    aiojobs_setup(app)

    client = await test_client(app)
    resp = await client.get('/')
    assert resp.status == 200

    assert job.active
    await client.close()
    assert job.closed


async def test_no_setup(test_client):
    async def handler(request):
        with pytest.raises(RuntimeError):
            get_scheduler(request)
        return response.json({})

    app = Sanic(__name__)
    app.add_route(handler, '/')

    client = await test_client(app)
    resp = await client.get('/')
    assert resp.status == 200


async def test_atomic(test_client):
    @atomic
    async def handler(request):
        await asyncio.sleep(0)
        return response.json({})

    app = Sanic(__name__)
    app.add_route(handler, '/')
    aiojobs_setup(app)

    client = await test_client(app)
    resp = await client.get('/')
    assert resp.status == 200

    scheduler = get_scheduler_from_app(app)

    assert scheduler.active_count == 0
    assert scheduler.pending_count == 0


async def test_atomic_from_view(test_client):
    app = Sanic(__name__)

    class MyView(HTTPMethodView):
        decorators = [atomic]

        async def get(self, request):
            return response.json({})

    app.add_route(MyView.as_view(), '/')
    aiojobs_setup(app)

    client = await test_client(app)
    resp = await client.get('/')
    assert resp.status == 200

    scheduler = get_scheduler_from_app(app)

    assert scheduler.active_count == 0
    assert scheduler.pending_count == 0
