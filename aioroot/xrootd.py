import asyncio
from functools import partial
from pyxrootd.client import File


class XRootDFile:
    def __init__(self, url):
        self._url = url
        self._file = File()
        self._timeout = 60
        self._servers = []
        self._loop = None

    def _handle(self, future, status, content, servers):
        if future.cancelled():
            return
        try:
            if not status['ok']:
                raise IOError(status['message'].strip())
            self._servers.append(servers)
            self._loop.call_soon_threadsafe(future.set_result, content)
        except Exception as exc:
            self._loop.call_soon_threadsafe(future.set_exception, exc)

    async def open(self):
        self._loop = asyncio.get_event_loop()
        future = self._loop.create_future()
        res = self._file.open(self._url, timeout=self._timeout, callback=partial(self._handle, future))
        if not res['ok']:
            raise IOError(res['message'].strip())
        await future
        return self

    async def close(self):
        if not self._file.is_open():
            self._file = None
            return
        future = asyncio.get_event_loop().create_future()
        res = self._file.close(timeout=self._timeout, callback=partial(self._handle, future))
        if not res['ok']:
            raise IOError(res['message'].strip())
        await future
        self._file = None
        self._loop = None

    async def stat(self):
        future = self._loop.create_future()
        res = self._file.stat(timeout=self._timeout, callback=partial(self._handle, future))
        if not res['ok']:
            raise IOError(res['message'].strip())
        return await future

    async def read(self, offset, size):
        future = self._loop.create_future()
        res = self._file.read(offset=offset, size=size, timeout=self._timeout, callback=partial(self._handle, future))
        if not res['ok']:
            raise IOError(res['message'].strip())
        return await future

    async def __aenter__(self):
        return await self.open()

    async def __aexit__(self, exc_type, exc, tb):
        # could pass lost connection exception for example
        return await self.close()
