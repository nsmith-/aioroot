import time
import asyncio
import struct
import warnings
from collections.abc import Mapping
from functools import partial
from pyxrootd.client import File


class XRootDFile:
    def __init__(self, url):
        self._url = url
        self._file = File()
        self._timeout = 60
        self._servers = []

    def _handle(self, future, status, content, servers):
        try:
            if not status['ok']:
                raise IOError(status['message'].strip())
            self._servers.append(servers)
            future.get_loop().call_soon_threadsafe(future.set_result, content)
        except Exception as exc:
            future.get_loop().call_soon_threadsafe(future.set_exception, exc)

    async def open(self):
        future = asyncio.get_event_loop().create_future()
        res = self._file.open(self._url, timeout=self._timeout, callback=partial(self._handle, future))
        if not res['ok']:
            raise IOError(res['message'].strip())
        await future
        return self

    async def close(self):
        future = asyncio.get_event_loop().create_future()
        res = self._file.close(timeout=self._timeout, callback=partial(self._handle, future))
        if not res['ok']:
            raise IOError(res['message'].strip())
        await future
        self._file = None

    async def stat(self):
        future = asyncio.get_event_loop().create_future()
        res = self._file.stat(timeout=self._timeout, callback=partial(self._handle, future))
        if not res['ok']:
            raise IOError(res['message'].strip())
        return await future

    async def read(self, offset, size):
        future = asyncio.get_event_loop().create_future()
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
        self._fields = {}

    def __repr__(self):
        return "<%s instance at 0x%x with fields: %r>" % (type(self).__name__, id(self), self._fields)

    @property
    def fields(self):
        return self._fields

    def __getitem__(self, key):
        return self._fields[key]


class TStreamed(ROOTObject):
    header1 = NamedStruct(">IH", ['fSize', 'fVersion'])

    def read(self, buffer, offset=0):
        fields, offset = TStreamed.header1.unpack_from(buffer, offset)
        if not (fields['fSize'] & 0x40000000):
            raise RuntimeError("Unsupported streamer version")
        fields['fSize'] -= 0x40000000
        self._end = offset + fields['fSize'] - 2  # fVersion
        self._fields.update(fields)
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
        self._fields['string'], offset = self.readstring(buffer, offset)
        return offset


class NameTitle(ROOTObject):
    def read(self, buffer, offset=0):
        self._fields['fName'], offset = TString.readstring(buffer, offset)
        self._fields['fTitle'], offset = TString.readstring(buffer, offset)
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
        self._fields.update(fields)
        if self._fields['magic'] != b'root':
            raise RuntimeError("Wrong magic")
        if self._fields['fVersion'] < 1000000:
            fields, offset = TFile.header2_small.unpack_from(buffer, offset)
            self._fields.update(fields)
        else:
            fields, offset = TFile.header2_big.unpack_from(buffer, offset)
            self._fields.update(fields)
        # all zeros unti fBEGIN in theory
        offset = self._fields['fBEGIN']
        return offset

    @property
    def size(self):
        return self._fields['fBEGIN']

    def decompressor(self):
        return Compression.decompressor(self._fields['fCompress'])


class TKey(ROOTObject):
    header1 = NamedStruct(">ihiIhh", ['fNbytes', 'fVersion', 'fObjlen', 'fDatime', 'fKeylen', 'fCycle'])
    header2_small = NamedStruct(">ii", ['fSeekKey', 'fSeekPdir'])
    header2_big = NamedStruct(">qq", ['fSeekKey', 'fSeekPdir'])

    @classmethod
    def minsize(cls):
        return cls.header1.size

    def read(self, buffer, offset=0):
        fields, offset = TKey.header1.unpack_from(buffer, offset)
        self._fields.update(fields)
        if self._fields['fVersion'] < 1000:
            fields, offset = TKey.header2_small.unpack_from(buffer, offset)
            self._fields.update(fields)
        else:
            fields, offset = TKey.header2_big.unpack_from(buffer, offset)
            self._fields.update(fields)
        self._fields['fClassName'], offset = TString.readstring(buffer, offset)
        self._fields['fName'], offset = TString.readstring(buffer, offset)
        self._fields['fTitle'], offset = TString.readstring(buffer, offset)
        return offset

    @property
    def size(self):
        return self._fields['fKeylen']

    @property
    def compressed(self):
        return self._fields['fNbytes'] < (self._fields['fKeylen'] + self._fields['fObjlen'])


class TKeyList(ROOTObject, Mapping):
    nKeys = NamedStruct(">i", 'nKeys')

    def __init__(self):
        super().__init__()
        self.headkey = TKey()
        self._keys = {}

    def read(self, buffer, offset=0):
        offset = self.headkey.read(buffer, offset)
        end = offset + self.headkey['fObjlen']
        nkeys, offset = TKeyList.nKeys.unpack_from(buffer, offset)
        while offset < end:
            key = TKey()
            offset = key.read(buffer, offset)
            self._keys[key['fName']] = key
        if len(self._keys) != nkeys:
            raise RuntimeError("Expected to read %d keys but got %d in %r" % (nkeys, len(self._keys), self))
        return offset

    def __iter__(self):
        return iter(self._keys)

    def __len__(self):
        return len(self._keys)

    def __getitem__(self, key):
        return self._keys.__getitem__(key)

    def __repr__(self):
        return "<TKeyList instance at 0x%x with keys: [\n    %s\n]>" % (id(self), "\n    ".join(repr(k) for k in self._keys.values()))


class TDirectory(ROOTObject):
    header1 = NamedStruct(">hIIii", ['fVersion', 'fDatimeC', 'fDatimeM', 'fNbytesKeys', 'fNbytesName'])
    header2_small = NamedStruct(">iii", ['fSeekDir', 'fSeekParent', 'fSeekKeys'])
    header2_big = NamedStruct(">qqq", ['fSeekDir', 'fSeekParent', 'fSeekKeys'])

    def read(self, buffer, offset=0):
        offset = NameTitle.read(self, buffer, offset)
        fields, offset = TDirectory.header1.unpack_from(buffer, offset)
        self._fields.update(fields)
        if self._fields['fVersion'] < 1000000:
            fields, offset = TDirectory.header2_small.unpack_from(buffer, offset)
            self._fields.update(fields)
        else:
            fields, offset = TDirectory.header2_big.unpack_from(buffer, offset)
            self._fields.update(fields)
        return offset

    def __getitem__(self, key):
        return self._fields[key]


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
        self._fields.update(fields)
        if self._fields['magic'] == 'L4':
            self._fields['checksum'], offset = CompressionHeader.l4header.unpack_from(buffer, offset)
        return offset

    def decompressor(self):
        decompressor = Compression.decompressor(self._fields['magic'])
        if self.l4dochecksum and self._fields['magic'] == 'L4':
            def decompress(bytes, uncompressed_size):
                import xxhash
                out = decompressor(bytes, uncompressed_size=self._fields['uncompressed_size'])
                if xxhash.xxh64(out).intdigest() != self._fields['checksum']:
                    raise RuntimeError("Checksum mismatch while decompressing")
                return out
            return decompress
        return partial(decompressor, uncompressed_size=self._fields['uncompressed_size'])


class TObject(TStreamed):
    header1 = NamedStruct(">HII", ['whatsthis', 'fBits', 'fUniqueID'])

    def read(self, buffer, offset=0):
        offset = super().read(buffer, offset)
        fields, offset = TObject.header1.unpack_from(buffer, offset)
        self._fields.update(fields)
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
        if self._fields['fVersion'] != 2:
            raise RuntimeError("Unrecognized TAttLine version")
        fields, offset = TAttLine.header1.unpack_from(buffer, offset)
        self._fields.update(fields)
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
            self._fields[cls.__name__] = superinfo

        if self._fields['fVersion'] == 20:
            fields, offset = TTree.header_v20.unpack_from(buffer, offset)
            self._fields.update(fields)
        else:
            raise RuntimeError("Unknown TTree class version")
        print(buffer[offset:offset + 60])
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

    def __init__(self, url):
        self._file = XRootDFile(url)
        self._open_readstep = 300
        self.fileheader = TFile()
        self.rootkey = TKey()
        self.rootdir = TDirectory()
        self.keyslist = TKeyList()

    async def open(self):
        await self._file.open()

        headbytes = await self._file.read(0, self._open_readstep)

        offset = self.fileheader.read(headbytes)
        if offset + self.rootkey.minsize() > len(headbytes):
            warnings.warn("Readahead too small in file open", RuntimeWarning)
            headbytes = headbytes + (await self._file.read(len(headbytes), self._open_readstep))

        offset = self.rootkey.read(headbytes, offset)
        if self.rootkey['fSeekKey'] != self.fileheader['fBEGIN']:
            raise RuntimeError("Root directory object not immediately after key")
        if offset + self.rootkey['fObjlen'] > len(headbytes):
            warnings.warn("Readahead too small in file open", RuntimeWarning)
            headbytes = headbytes + (await self._file.read(len(headbytes), self._open_readstep))

        offset = self.rootdir.read(headbytes, offset)
        keysbytes = await self._file.read(self.rootdir['fSeekKeys'], self.rootdir['fNbytesKeys'])

        offset = self.keyslist.read(keysbytes)
        return self

    async def close(self):
        return await self._file.close()

    async def __aenter__(self):
        return await self.open()

    async def __aexit__(self, exc_type, exc, tb):
        return await self.close()

    def keys(self):
        return self.keyslist.keys()

    async def __getitem__(self, key):
        key = self.keyslist[key]
        print(key)
        obj = ROOTFile.classmap[key['fClassName']]()
        # worth reading key at object? not doing now
        objbytes = await self._file.read(key['fSeekKey'] + key['fKeylen'], key['fNbytes'] - key['fKeylen'])

        if key.compressed:
            cheader = CompressionHeader()
            offset = cheader.read(objbytes)
            print(cheader)
            # TODO run in thread pool
            objbytes = cheader.decompressor()(memoryview(objbytes)[offset:])

        obj.read(objbytes)
        return obj


async def main():
    urls = [
        "root://cmseos.fnal.gov//eos/uscms/store/user/lpcbacon/dazsle/zprimebits-v15.01/GluGluHToBB_M125_13TeV_powheg_pythia8/Output_job10_job10_file600to659.root",
        "root://cmseos.fnal.gov//eos/uscms/store/user/lpcbacon/dazsle/zprimebits-v15.01/GluGluHToBB_M125_13TeV_powheg_pythia8/Output_job11_job11_file660to719.root",
        "root://cmseos.fnal.gov//eos/uscms/store/user/lpcbacon/dazsle/zprimebits-v15.01/GluGluHToBB_M125_13TeV_powheg_pythia8/Output_job12_job12_file720to779.root",
        "root://cmseos.fnal.gov//eos/uscms/store/user/lpcbacon/dazsle/zprimebits-v15.01/GluGluHToBB_M125_13TeV_powheg_pythia8/Output_job13_job13_file780to839.root",
        "root://cmseos.fnal.gov//eos/uscms/store/user/lpcbacon/dazsle/zprimebits-v15.01/GluGluHToBB_M125_13TeV_powheg_pythia8/Output_job14_job14_file840to899.root",
        "root://cmseos.fnal.gov//eos/uscms/store/user/lpcbacon/dazsle/zprimebits-v15.01/GluGluHToBB_M125_13TeV_powheg_pythia8/Output_job15_job15_file900to959.root",
        "root://cmseos.fnal.gov//eos/uscms/store/user/lpcbacon/dazsle/zprimebits-v15.01/GluGluHToBB_M125_13TeV_powheg_pythia8/Output_job16_job16_file960to1019.root",
        "root://cmseos.fnal.gov//eos/uscms/store/user/lpcbacon/dazsle/zprimebits-v15.01/GluGluHToBB_M125_13TeV_powheg_pythia8/Output_job17_job17_file1020to1079.root",
        "root://cmseos.fnal.gov//eos/uscms/store/user/lpcbacon/dazsle/zprimebits-v15.01/GluGluHToBB_M125_13TeV_powheg_pythia8/Output_job18_job18_file1080to1139.root",
        "root://cmseos.fnal.gov//eos/uscms/store/user/lpcbacon/dazsle/zprimebits-v15.01/GluGluHToBB_M125_13TeV_powheg_pythia8/Output_job19_job19_file1140to1199.root",
        "root://cmseos.fnal.gov//eos/uscms/store/user/lpcbacon/dazsle/zprimebits-v15.01/GluGluHToBB_M125_13TeV_powheg_pythia8/Output_job1_job1_file60to119.root",
        "root://cmseos.fnal.gov//eos/uscms/store/user/lpcbacon/dazsle/zprimebits-v15.01/GluGluHToBB_M125_13TeV_powheg_pythia8/Output_job20_job20_file1200to1259.root",
        "root://cmseos.fnal.gov//eos/uscms/store/user/lpcbacon/dazsle/zprimebits-v15.01/GluGluHToBB_M125_13TeV_powheg_pythia8/Output_job21_job21_file1260to1319.root",
        "root://cmseos.fnal.gov//eos/uscms/store/user/lpcbacon/dazsle/zprimebits-v15.01/GluGluHToBB_M125_13TeV_powheg_pythia8/Output_job22_job22_file1320to1379.root",
        "root://cmseos.fnal.gov//eos/uscms/store/user/lpcbacon/dazsle/zprimebits-v15.01/GluGluHToBB_M125_13TeV_powheg_pythia8/Output_job23_job23_file1380to1439.root",
        "root://cmseos.fnal.gov//eos/uscms/store/user/lpcbacon/dazsle/zprimebits-v15.01/GluGluHToBB_M125_13TeV_powheg_pythia8/Output_job24_job24_file1440to1499.root",
        "root://cmseos.fnal.gov//eos/uscms/store/user/lpcbacon/dazsle/zprimebits-v15.01/GluGluHToBB_M125_13TeV_powheg_pythia8/Output_job25_job25_file1500to1559.root",
        "root://cmseos.fnal.gov//eos/uscms/store/user/lpcbacon/dazsle/zprimebits-v15.01/GluGluHToBB_M125_13TeV_powheg_pythia8/Output_job26_job26_file1560to1619.root",
        "root://cmseos.fnal.gov//eos/uscms/store/user/lpcbacon/dazsle/zprimebits-v15.01/GluGluHToBB_M125_13TeV_powheg_pythia8/Output_job27_job27_file1620to1679.root",
        "root://cmseos.fnal.gov//eos/uscms/store/user/lpcbacon/dazsle/zprimebits-v15.01/GluGluHToBB_M125_13TeV_powheg_pythia8/Output_job28_job28_file1680to1739.root",
    ]

    url = urls[0]
    tic = time.time()
    async with ROOTFile(url) as file:
        print(file.keyslist.headkey)
        print(list(file.keys()))
        print(await file[b'Events'])
    toc = time.time()
    print("Elapsed:", toc - tic)


if __name__ == '__main__':
    asyncio.run(main(), debug=True)
