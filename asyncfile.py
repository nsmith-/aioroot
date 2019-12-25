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


def unpack_string(buffer, offset):
    nBytes = NamedStruct(">B", 'nBytes')
    nBytesLong = NamedStruct(">i", 'nBytes')
    length, offset = nBytes.unpack_from(buffer, offset)
    if length == 255:
        length, offset = nBytesLong.unpack_from(buffer, offset)
    string, offset = bytes(buffer[offset:offset + length]), offset + length
    return string, offset


class TFile:
    header1 = NamedStruct(">4sii", ['magic', 'fVersion', 'fBEGIN'])
    header2_small = NamedStruct(">iiiiiBiii18s", ['fEND', 'fSeekFree', 'fNbytesFree', 'nfree', 'fNbytesName', 'fUnits', 'fCompress', 'fSeekInfo', 'fNbytesInfo', 'fUUID'])
    header2_big = NamedStruct(">qqiiiBiqi18s", ['fEND', 'fSeekFree', 'fNbytesFree', 'nfree', 'fNbytesName', 'fUnits', 'fCompress', 'fSeekInfo', 'fNbytesInfo', 'fUUID'])

    @classmethod
    def minsize(cls):
        '''If this much can be read, we know how much the whole thing needs'''
        return cls.header1.size

    def __init__(self):
        self._fields = {}

    def read(self, buffer, offset=0):
        fields, offset = self.header1.unpack_from(buffer, offset)
        self._fields.update(fields)
        if self._fields['magic'] != b'root':
            raise RuntimeError("Wrong magic")
        if self._fields['fVersion'] < 1000000:
            fields, offset = self.header2_small.unpack_from(buffer, offset)
            self._fields.update(fields)
        else:
            fields, offset = self.header2_big.unpack_from(buffer, offset)
            self._fields.update(fields)
        # all zeros unti fBEGIN in theory
        offset = self._fields['fBEGIN']
        return offset

    @property
    def fields(self):
        return self._fields

    @property
    def size(self):
        return self._fields['fBEGIN']

    def __getitem__(self, key):
        return self._fields[key]

    def __repr__(self):
        return "<TFile instance at 0x%x with fields: %r>" % (id(self), self._fields)

    def decompressor(self):
        return Compression.decompressor(self._fields['fCompress'])


class TKey:
    header1 = NamedStruct(">ihiIhh", ['fNbytes', 'fVersion', 'fObjlen', 'fDatime', 'fKeylen', 'fCycle'])
    header2_small = NamedStruct(">ii", ['fSeekKey', 'fSeekPdir'])
    header2_big = NamedStruct(">qq", ['fSeekKey', 'fSeekPdir'])

    @classmethod
    def minsize(cls):
        return cls.header1.size

    def __init__(self):
        self._fields = {}

    def read(self, buffer, offset=0):
        fields, offset = self.header1.unpack_from(buffer, offset)
        self._fields.update(fields)
        if self._fields['fVersion'] < 1000:
            fields, offset = self.header2_small.unpack_from(buffer, offset)
            self._fields.update(fields)
        else:
            fields, offset = self.header2_big.unpack_from(buffer, offset)
            self._fields.update(fields)
        self._fields['fClassName'], offset = unpack_string(buffer, offset)
        self._fields['fName'], offset = unpack_string(buffer, offset)
        self._fields['fTitle'], offset = unpack_string(buffer, offset)
        return offset

    @property
    def fields(self):
        return self._fields

    @property
    def size(self):
        return self._fields['fKeylen']

    def __getitem__(self, key):
        return self._fields[key]

    def __repr__(self):
        return "<TKey instance at 0x%x with fields: %r>" % (id(self), self._fields)

    @property
    def compressed(self):
        return self._fields['fNbytes'] < (self._fields['fKeylen'] + self._fields['fObjlen'])


class TKeyList(Mapping):
    nKeys = NamedStruct(">i", 'nKeys')

    def __init__(self):
        self.headkey = TKey()
        self._keys = {}

    def read(self, buffer, offset=0):
        offset = self.headkey.read(buffer, offset)
        end = offset + self.headkey['fObjlen']
        nkeys, offset = self.nKeys.unpack_from(buffer, offset)
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


class TNamed:
    def __init__(self):
        self._fields = {}

    def read(self, buffer, offset=0):
        self._fields['fName'], offset = unpack_string(buffer, offset)
        self._fields['fTitle'], offset = unpack_string(buffer, offset)
        return offset

    def __repr__(self):
        return "<%s instance at 0x%x with fields: %r>" % (type(self).__name__, id(self), self._fields)


class TDirectory(TNamed):
    header1 = NamedStruct(">hIIii", ['fVersion', 'fDatimeC', 'fDatimeM', 'fNbytesKeys', 'fNbytesName'])
    header2_small = NamedStruct(">iii", ['fSeekDir', 'fSeekParent', 'fSeekKeys'])
    header2_big = NamedStruct(">qqq", ['fSeekDir', 'fSeekParent', 'fSeekKeys'])

    def read(self, buffer, offset=0):
        offset = super().read(buffer, offset)
        fields, offset = self.header1.unpack_from(buffer, offset)
        self._fields.update(fields)
        if self._fields['fVersion'] < 1000000:
            fields, offset = self.header2_small.unpack_from(buffer, offset)
            self._fields.update(fields)
        else:
            fields, offset = self.header2_big.unpack_from(buffer, offset)
            self._fields.update(fields)
        return offset

    @property
    def fields(self):
        return self._fields

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
            from zlib import decompress as zlib_decompress

            def decompress(bytes, uncompressed_size):
                return zlib_decompress(bytes)

            return decompress
        elif fCompress == cls.kLZMA:
            from lzma import decompress as lzma_decompress

            def decompress(bytes, uncompressed_size):
                return lzma_decompress(bytes)

            return decompress
        elif fCompress == cls.kOldCompressionAlgo:
            raise NotImplementedError("kOldCompressionAlgo")
        elif fCompress == cls.kLZ4:
            from lz4.block import decompress as lz4_decompress

            def decompress(bytes, uncompressed_size):
                return lz4_decompress(bytes, uncompressed_size=uncompressed_size)

            return decompress
        elif fCompress == cls.kZSTD:
            raise NotImplementedError("kZSTD")
        raise RuntimeError("Compression not supported")


class CompressionHeader:
    header = NamedStruct(">2sB3s3s", ['magic', 'version', 'compressed_size', 'uncompressed_size'])
    l4header = NamedStruct(">Q", 'checksum')
    l4dochecksum = True

    def __init__(self):
        self._fields = {}

    def decode_size(self, field):
        decoder = struct.Struct("<I")  # big-endian everywhere else but here
        return decoder.unpack(field + b'\x00')[0]

    def read(self, buffer, offset=0):
        fields, offset = self.header.unpack_from(buffer, offset)
        # 3 byte fields is absolutely positively disgusting and you should feel bad
        fields['compressed_size'] = self.decode_size(fields['compressed_size'])
        fields['uncompressed_size'] = self.decode_size(fields['uncompressed_size'])
        self._fields.update(fields)
        if self._fields['magic'] == 'L4':
            self._fields['checksum'], offset = self.l4header.unpack_from(buffer, offset)
        return offset

    def __repr__(self):
        return "<%s instance at 0x%x with fields: %r>" % (type(self).__name__, id(self), self._fields)

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


class ROOTFile:
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
        # Worth reading key at object?
        objbytes = await self._file.read(key['fSeekKey'] + key['fKeylen'], key['fNbytes'] - key['fKeylen'])
        print(key)

        if key.compressed:
            cheader = CompressionHeader()
            offset = cheader.read(objbytes)
            print(cheader)
            # TODO run in thread pool
            objbytes = cheader.decompressor()(memoryview(objbytes)[offset:])

        print(len(objbytes), objbytes[:30].hex())
        return objbytes


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
        await file[b'Events']
    toc = time.time()
    print("Elapsed:", toc - tic)


if __name__ == '__main__':
    asyncio.run(main(), debug=True)
