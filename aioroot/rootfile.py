import asyncio
import warnings
from functools import partial
from concurrent.futures import ThreadPoolExecutor
from .xrootd import XRootDFile
from .structure import (
    ROOTObject,
    TFile,
    TKey,
    TDirectory,
    TKeyList,
    CompressionHeader,
    ClassMap,
)


class ROOTFile:
    def __init__(self, url, threadpool=None):
        self._file = XRootDFile(url)
        self._open_readstep = 512  # ROOT uses 300
        self._ownpool = None
        self._threadpool = threadpool
        if threadpool is None:
            self._ownpool = partial(ThreadPoolExecutor, max_workers=1)
        self.fileheader = TFile()
        self.rootkey = TKey()
        self.rootdir = TDirectory()
        self.keyslist = TKeyList()

    async def _run_in_pool(self, fun, *args):
        return await asyncio.get_event_loop().run_in_executor(self._threadpool, fun, *args)

    async def open(self):
        if self._ownpool is not None:
            self._threadpool = self._ownpool()
        await self._file.open()

        headbytes = await self._file.read(0, self._open_readstep)

        offset = self.fileheader.read(headbytes)
        if offset + self.rootkey.minsize() > len(headbytes):
            warnings.warn("Readahead too small in file open to reach root key", RuntimeWarning)
            more = max(offset + self.rootkey.minsize() - len(headbytes), self._open_readstep)
            headbytes = headbytes + (await self._file.read(len(headbytes), more))

        offset = self.rootkey.read(headbytes, offset)
        if self.rootkey.data['fSeekKey'] != self.fileheader.data['fBEGIN']:
            raise RuntimeError("Root directory object not immediately after key")
        if offset + self.rootkey.data['fObjlen'] > len(headbytes):
            warnings.warn("Readahead too small in file open to reach end of root directory header", RuntimeWarning)
            more = offset + self.rootkey.data['fObjlen'] - len(headbytes)
            headbytes = headbytes + (await self._file.read(len(headbytes), more))

        offset = self.rootdir.read(headbytes, offset)
        keysbytes = await self._file.read(self.rootdir.data['fSeekKeys'], self.rootdir.data['fNbytesKeys'])

        offset = self.keyslist.read(keysbytes)
        return self

    async def close(self):
        if self._ownpool is not None:
            self._threadpool = None
        return await self._file.close()

    async def __aenter__(self):
        return await self.open()

    async def __aexit__(self, exc_type, exc, tb):
        return await self.close()

    def keys(self):
        return self.keyslist.keys()

    @property
    def data(self):
        def dig(item):
            if isinstance(item, ROOTObject):
                return {k: dig(v) for k, v in item.data.items()}
            elif isinstance(item, dict):
                return {k: dig(v) for k, v in item.items()}
            return item

        data = {
            'fileheader': self.fileheader,
            'rootkey': self.rootkey,
            'rootdir': self.rootdir,
            'keyslist': self.keyslist
        }
        return dig(data)

    async def __getitem__(self, key):
        key = self.keyslist[key]
        obj = ClassMap[key.data['fClassName']]()
        # worth reading key at object? not doing now
        objbytes = await self._file.read(key.data['fSeekKey'] + key.data['fKeylen'], key.data['fNbytes'] - key.data['fKeylen'])

        if key.compressed:
            cheader = CompressionHeader()
            offset = cheader.read(objbytes)
            objbytes = await self._run_in_pool(cheader.decompressor(), memoryview(objbytes)[offset:])

        obj.read(objbytes)
        key.data['object'] = obj
        return obj
