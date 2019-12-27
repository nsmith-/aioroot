import time
import asyncio
import struct
import warnings
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pyxrootd.client import File
from pprint import pprint
import uproot


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


class Buffer:
    '''Possibly useless'''
    def __init__(self, bytes, offset):
        '''From bytestring and global offset'''
        self._bytes = bytes
        self._globaloffset = offset

    @property
    def bytes(self):
        return self._bytes

    def offset(self, globaloffset):
        return globaloffset - self._globaloffset


class NamedStruct:
    def __init__(self, sformat, sfields):
        self._struct = struct.Struct(sformat)
        if not isinstance(sfields, (list, str)):
            raise ValueError
        self._fields = sfields
        self._single = not isinstance(sfields, list)

    @property
    def size(self):
        return self._struct.size

    @property
    def fields(self):
        return self._fields

    def unpack_from(self, buffer, offset):
        vals = self._struct.unpack_from(buffer, offset)
        offset = offset + self._struct.size
        if self._single:
            return vals[0], offset
        return dict(zip(self._fields, vals)), offset


class ROOTObject:
    def __init__(self):
        self.data = {}

    def __repr__(self):
        return "<%s instance at 0x%x with data: %r>" % (type(self).__name__, id(self), self.data)


class TStreamed(ROOTObject):
    header1 = NamedStruct(">IH", ['fSize', 'fVersion'])

    def read(self, buffer, offset=0):
        fields, offset = TStreamed.header1.unpack_from(buffer, offset)
        if not (fields['fSize'] & 0x40000000):
            raise RuntimeError("Unsupported streamer version")
        fields['fSize'] -= 0x40000000
        self._end = offset + fields['fSize'] - 2  # fVersion
        self.data.update(fields)
        if type(self) is TStreamed:
            offset = self._end
        return offset

    def check(self, offset):
        if offset < self._end:
            raise RuntimeError("Did not read enough from a streamer")
        if offset > self._end:
            raise RuntimeError("Read too much from a streamer")


class TString(ROOTObject):
    nBytes = NamedStruct(">B", 'nBytes')
    nBytesLong = NamedStruct(">i", 'nBytes')

    @classmethod
    def readstring(cls, buffer, offset):
        length, offset = cls.nBytes.unpack_from(buffer, offset)
        if length == 255:
            length, offset = cls.nBytesLong.unpack_from(buffer, offset)
        string, offset = bytes(buffer[offset:offset + length]), offset + length
        return string, offset

    def read(self, buffer, offset):
        # TODO probably need to read streamer
        self.data['string'], offset = self.readstring(buffer, offset)
        return offset


class NameTitle(ROOTObject):
    def read(self, buffer, offset=0):
        self.data['fName'], offset = TString.readstring(buffer, offset)
        self.data['fTitle'], offset = TString.readstring(buffer, offset)
        return offset


class TFile(ROOTObject):
    header1 = NamedStruct(">4sii", ['magic', 'fVersion', 'fBEGIN'])
    header2_small = NamedStruct(">iiiiiBiii18s", ['fEND', 'fSeekFree', 'fNbytesFree', 'nfree', 'fNbytesName', 'fUnits', 'fCompress', 'fSeekInfo', 'fNbytesInfo', 'fUUID'])
    header2_big = NamedStruct(">qqiiiBiqi18s", ['fEND', 'fSeekFree', 'fNbytesFree', 'nfree', 'fNbytesName', 'fUnits', 'fCompress', 'fSeekInfo', 'fNbytesInfo', 'fUUID'])

    @classmethod
    def minsize(cls):
        '''If this much can be read, we know how much the whole thing needs'''
        return cls.header1.size

    def read(self, buffer, offset=0):
        fields, offset = TFile.header1.unpack_from(buffer, offset)
        self.data.update(fields)
        if self.data['magic'] != b'root':
            raise RuntimeError("Wrong magic")
        if self.data['fVersion'] < 1000000:
            fields, offset = TFile.header2_small.unpack_from(buffer, offset)
            self.data.update(fields)
        else:
            fields, offset = TFile.header2_big.unpack_from(buffer, offset)
            self.data.update(fields)
        # all zeros unti fBEGIN in theory
        offset = self.data['fBEGIN']
        return offset

    @property
    def size(self):
        return self.data['fBEGIN']

    def decompressor(self):
        return Compression.decompressor(self.data['fCompress'])


class TKey(ROOTObject):
    header1 = NamedStruct(">ihiIhh", ['fNbytes', 'fVersion', 'fObjlen', 'fDatime', 'fKeylen', 'fCycle'])
    header2_small = NamedStruct(">ii", ['fSeekKey', 'fSeekPdir'])
    header2_big = NamedStruct(">qq", ['fSeekKey', 'fSeekPdir'])

    @classmethod
    def minsize(cls):
        return cls.header1.size

    def read(self, buffer, offset=0):
        start = offset
        fields, offset = TKey.header1.unpack_from(buffer, offset)
        self.data.update(fields)
        if self.data['fVersion'] < 1000:
            fields, offset = TKey.header2_small.unpack_from(buffer, offset)
            self.data.update(fields)
        else:
            fields, offset = TKey.header2_big.unpack_from(buffer, offset)
            self.data.update(fields)
        self.data['fClassName'], offset = TString.readstring(buffer, offset)
        self.data['fName'], offset = TString.readstring(buffer, offset)
        self.data['fTitle'], offset = TString.readstring(buffer, offset)
        if start + self.data['fKeylen'] != offset:
            raise RuntimeError("Read fewer bytes from TKey (%d) than expected (%d)" % (offset - start, self.data['fKeylen']))
        return offset

    @property
    def size(self):
        return self.data['fKeylen']

    @property
    def compressed(self):
        return self.data['fNbytes'] < (self.data['fKeylen'] + self.data['fObjlen'])

    @property
    def namecycle(self):
        return b'%s;%d' % (self.data['fName'], self.data['fCycle'])


class TKeyList(ROOTObject, Mapping):
    nKeys = NamedStruct(">i", 'nKeys')

    def __init__(self):
        super().__init__()
        self.headkey = TKey()
        self.data['headkey'] = self.headkey
        self._keys = {}
        self.data['keys'] = self._keys
        self._lastcycle = {}

    def read(self, buffer, offset=0):
        offset = self.headkey.read(buffer, offset)
        end = offset + self.headkey.data['fObjlen']
        nkeys, offset = TKeyList.nKeys.unpack_from(buffer, offset)
        self.data['nKeys'] = nkeys
        while offset < end and len(self._keys) < nkeys:
            key = TKey()
            offset = key.read(buffer, offset)
            self._keys[key.namecycle] = key
            if self._lastcycle.get(key.data['fName'], -1) < key.data['fCycle']:
                self._lastcycle[key.data['fName']] = key.data['fCycle']
        if len(self._keys) != nkeys:
            raise RuntimeError("Expected to read %d keys but got %d in %r" % (nkeys, len(self._keys), self))
        if offset < end:
            self.data['trailings'] = bytes(buffer[offset:end])
            offset = end
        return offset

    def __iter__(self):
        return iter(self._keys)

    def __len__(self):
        return len(self._keys)

    def __getitem__(self, keyname):
        if keyname not in self._keys:
            keyname = b'%s;%d' % (keyname, self._lastcycle[keyname])
        return self._keys.__getitem__(keyname)


class TDirectory(ROOTObject):
    header1 = NamedStruct(">hIIii", ['fVersion', 'fDatimeC', 'fDatimeM', 'fNbytesKeys', 'fNbytesName'])
    header2_small = NamedStruct(">iii", ['fSeekDir', 'fSeekParent', 'fSeekKeys'])
    header2_big = NamedStruct(">qqq", ['fSeekDir', 'fSeekParent', 'fSeekKeys'])

    def read(self, buffer, offset=0):
        offset = NameTitle.read(self, buffer, offset)
        fields, offset = TDirectory.header1.unpack_from(buffer, offset)
        self.data.update(fields)
        if self.data['fVersion'] < 1000:
            fields, offset = TDirectory.header2_small.unpack_from(buffer, offset)
            self.data.update(fields)
        else:
            fields, offset = TDirectory.header2_big.unpack_from(buffer, offset)
            self.data.update(fields)
        return offset

    def __getitem__(self, key):
        return self.data[key]


class Compression:
    _, kZLIB, kLZMA, kOldCompressionAlgo, kLZ4, kZSTD = range(6)
    magicmap = {b'ZL': kZLIB, b'XZ': kLZMA, b'L4': kLZ4, b'ZS': kZSTD}

    @classmethod
    def decompressor(cls, fCompress):
        if isinstance(fCompress, bytes):
            fCompress = cls.magicmap[fCompress]
        if fCompress == cls.kZLIB:
            def decompress(bytes, uncompressed_size):
                from zlib import decompress as zlib_decompress
                return zlib_decompress(bytes)

            return decompress
        elif fCompress == cls.kLZMA:
            def decompress(bytes, uncompressed_size):
                from lzma import decompress as lzma_decompress
                return lzma_decompress(bytes)

            return decompress
        elif fCompress == cls.kOldCompressionAlgo:
            raise NotImplementedError("kOldCompressionAlgo")
        elif fCompress == cls.kLZ4:
            def decompress(bytes, uncompressed_size):
                from lz4.block import decompress as lz4_decompress
                return lz4_decompress(bytes, uncompressed_size=uncompressed_size)

            return decompress
        elif fCompress == cls.kZSTD:
            raise NotImplementedError("kZSTD")
        raise RuntimeError("Compression not supported")


class CompressionHeader(ROOTObject):
    header = NamedStruct(">2sB3s3s", ['magic', 'version', 'compressed_size', 'uncompressed_size'])
    l4header = NamedStruct(">Q", 'checksum')
    l4dochecksum = True

    def decode_size(self, field):
        decoder = struct.Struct("<I")  # big-endian everywhere else but here
        return decoder.unpack(field + b'\x00')[0]

    def read(self, buffer, offset=0):
        fields, offset = CompressionHeader.header.unpack_from(buffer, offset)
        # 3 byte fields is absolutely positively disgusting and you should feel bad
        fields['compressed_size'] = self.decode_size(fields['compressed_size'])
        fields['uncompressed_size'] = self.decode_size(fields['uncompressed_size'])
        self.data.update(fields)
        if self.data['magic'] == 'L4':
            self.data['checksum'], offset = CompressionHeader.l4header.unpack_from(buffer, offset)
        return offset

    def decompressor(self):
        decompressor = Compression.decompressor(self.data['magic'])
        if self.l4dochecksum and self.data['magic'] == 'L4':
            def decompress(bytes):
                import xxhash
                out = decompressor(bytes, uncompressed_size=self.data['uncompressed_size'])
                if xxhash.xxh64(out).intdigest() != self.data['checksum']:
                    raise RuntimeError("Checksum mismatch while decompressing")
                return out

            return decompress
        return partial(decompressor, uncompressed_size=self.data['uncompressed_size'])


class TObject(TStreamed):
    header1 = NamedStruct(">HII", ['whatsthis', 'fBits', 'fUniqueID'])

    def read(self, buffer, offset=0):
        offset = super().read(buffer, offset)
        fields, offset = TObject.header1.unpack_from(buffer, offset)
        self.data.update(fields)
        if type(self) is TObject:
            self.check(offset)
        return offset


class TNamed(TObject):
    def read(self, buffer, offset=0):
        offset = super().read(buffer, offset)
        offset = NameTitle.read(self, buffer, offset)
        if type(self) is TNamed:
            self.check(offset)
        return offset


class TAttLine(TStreamed):
    header1 = NamedStruct(">hhh", ['fLineColor', 'fLineStyle', 'fLineWidth'])

    def read(self, buffer, offset=0):
        offset = super().read(buffer, offset)
        if self.data['fVersion'] != 2:
            raise RuntimeError("Unrecognized TAttLine version")
        fields, offset = TAttLine.header1.unpack_from(buffer, offset)
        self.data.update(fields)
        if type(self) is TAttLine:
            self.check(offset)
        return offset


class TTree(TStreamed):
    supers = [TNamed, TAttLine, TStreamed, TStreamed]  # TAttFill, TAttMarker
    header_v20 = NamedStruct(
        ">qqqqqdiiiiIqqqqqq",
        [
            'fEntries', 'fTotBytes', 'fZipBytes', 'fSavedBytes', 'fFlushedBytes', 'fWeight',
            'fTimerInterval', 'fScanField', 'fUpdate', 'fDefaultEntryOffsetLen', 'fNClusterRange',
            'fMaxEntries', 'fMaxEntryLoop', 'fMaxVirtualSize', 'fAutoSave', 'fAutoFlush', 'fEstimate',
            'fClusterRangeEnd', 'fClusterSize', 'fIOFeatures', 'fBranches', 'fLeaves', 'fAliases',
            'fIndexValues', 'fIndex', 'fTreeIndex', 'fFriends'
        ]
    )

    def read(self, buffer, offset=0):
        offset = super().read(buffer, offset)
        for cls in TTree.supers:
            superinfo = cls()
            offset = superinfo.read(buffer, offset)
            self.data[cls.__name__] = superinfo

        if self.data['fVersion'] == 20:
            fields, offset = TTree.header_v20.unpack_from(buffer, offset)
            self.data.update(fields)
        else:
            raise RuntimeError("Unknown TTree class version")
        # print(buffer[offset:offset + 60])
        # from uproot:
        # fBasketSeek_dtype = cls._dtype1
        # if getattr(context, "speedbump", True):
        #     cursor.skip(1)
        # self._fClusterRangeEnd = cursor.array(source, self._fNClusterRange, fBasketSeek_dtype)
        # fBasketSeek_dtype = cls._dtype2
        # if getattr(context, "speedbump", True):
        #     cursor.skip(1)
        # self._fClusterSize = cursor.array(source, self._fNClusterRange, fBasketSeek_dtype)
        # self._fIOFeatures = ROOT_3a3a_TIOFeatures.read(source, cursor, context, self)
        # self._fBranches = TObjArray.read(source, cursor, context, self)
        # self._fLeaves = TObjArray.read(source, cursor, context, self)
        # self._fAliases = _readobjany(source, cursor, context, parent)
        # self._fIndexValues = TArrayD.read(source, cursor, context, self)
        # self._fIndex = TArrayI.read(source, cursor, context, self)
        # self._fTreeIndex = _readobjany(source, cursor, context, parent)
        # self._fFriends = _readobjany(source, cursor, context, parent)
        # _readobjany(source, cursor, context, parent, asclass=Undefined)
        # _readobjany(source, cursor, context, parent, asclass=Undefined)
        return offset


class ROOTFile:
    classmap = {
        b'TTree': TTree,
    }

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
        obj = ROOTFile.classmap[key.data['fClassName']]()
        # worth reading key at object? not doing now
        objbytes = await self._file.read(key.data['fSeekKey'] + key.data['fKeylen'], key.data['fNbytes'] - key.data['fKeylen'])

        if key.compressed:
            cheader = CompressionHeader()
            offset = cheader.read(objbytes)
            objbytes = await self._run_in_pool(cheader.decompressor(), memoryview(objbytes)[offset:])

        obj.read(objbytes)
        key.data['object'] = obj
        return obj


async def getentries(url, threadpool=None):
    async with ROOTFile(url, threadpool=threadpool) as file:
        tree = await file[b'Events']
        # pprint(file.data)
    return url, tree.data['fEntries']


async def main():
    urls = [
        "root://eospublic.cern.ch//eos/opendata/cms/derived-data/AOD2NanoAODOutreachTool/DYJetsToLL.root",
        "root://eospublic.cern.ch//eos/opendata/cms/derived-data/AOD2NanoAODOutreachTool/GluGluToHToTauTau.root",
        "root://eospublic.cern.ch//eos/opendata/cms/derived-data/AOD2NanoAODOutreachTool/Run2012BC_DoubleMuParked_Muons.root",
        "root://eospublic.cern.ch//eos/opendata/cms/derived-data/AOD2NanoAODOutreachTool/Run2012B_TauPlusX.root",
        "root://eospublic.cern.ch//eos/opendata/cms/derived-data/AOD2NanoAODOutreachTool/Run2012C_TauPlusX.root",
        "root://eospublic.cern.ch//eos/opendata/cms/derived-data/AOD2NanoAODOutreachTool/TTbar.root",
        "root://eospublic.cern.ch//eos/opendata/cms/derived-data/AOD2NanoAODOutreachTool/VBF_HToTauTau.root",
        "root://eospublic.cern.ch//eos/opendata/cms/derived-data/AOD2NanoAODOutreachTool/W1JetsToLNu.root",
        "root://eospublic.cern.ch//eos/opendata/cms/derived-data/AOD2NanoAODOutreachTool/W2JetsToLNu.root",
        "root://eospublic.cern.ch//eos/opendata/cms/derived-data/AOD2NanoAODOutreachTool/W3JetsToLNu.root",
    ]

    with ThreadPoolExecutor(max_workers=2) as threadpool:
        # prime the plumbing
        await getentries(urls[0], threadpool)

        entries_async = {}
        tic = time.time()
        for result in asyncio.as_completed(map(partial(getentries, threadpool=threadpool), urls)):
            url, entries = await result
            entries_async[url] = entries
        toc = time.time()
        print("Elapsed (async):", toc - tic)

        # prime the plumbing
        await asyncio.get_event_loop().run_in_executor(threadpool, uproot.numentries, urls[0], b'Events')

        entries_uproot = {}
        tic = time.time()
        for url in urls:
            entries = await asyncio.get_event_loop().run_in_executor(threadpool, uproot.numentries, url, b'Events')
            entries_uproot[url] = entries
        toc = time.time()
        print("Elapsed (uproot):", toc - tic)

        print("All entries agree?", all(entries_async[url] == entries_uproot[url] for url in urls))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.set_debug(False)
    loop.run_until_complete(main())
