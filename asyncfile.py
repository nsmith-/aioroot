import time
import asyncio
import struct
import warnings
from functools import partial
from pyxrootd.client import File


class AsyncFile:
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


class KeysList:
    nKeys = NamedStruct(">i", 'nKeys')

    def __init__(self):
        self.headkey = TKey()
        self.keys = []

    def read(self, buffer, offset=0):
        offset = self.headkey.read(buffer, offset)
        end = offset + self.headkey['fObjlen']
        print(self.headkey, end)
        nkeys, offset = self.nKeys.unpack_from(buffer, offset)
        while offset < end:
            key = TKey()
            offset = key.read(buffer, offset)
            self.keys.append(key)
        if len(self.keys) != nkeys:
            raise RuntimeError("Expected to read %d keys but got %d in %r" % (nkeys, len(self.keys), self))
        return offset

    def __iter__(self):
        return iter(self.keys)

    def __repr__(self):
        return "<KeysList instance at 0x%x with keys: [\n    %s\n]>" % (id(self), "\n    ".join(repr(k) for k in self.keys))


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


async def getsize(url):
    async with AsyncFile(url) as f:
        size = (await f.stat())['size']

        readhint = 300
        headbytes = await f.read(0, readhint)
        fileheader = TFile()
        offset = fileheader.read(headbytes)
        print("TFile:", fileheader)

        rootkey = TKey()
        if offset + rootkey.minsize() > len(headbytes):
            warnings.warn("Readahead too small in file open", RuntimeWarning)
            headbytes = headbytes + (await f.read(len(headbytes), readhint))
        offset = rootkey.read(headbytes, offset)
        print("Root TKey:", rootkey)

        if rootkey['fSeekKey'] != fileheader['fBEGIN']:
            raise RuntimeError("Root directory object not immediately after key")
        if offset + rootkey['fObjlen'] > len(headbytes):
            warnings.warn("Readahead too small in file open", RuntimeWarning)
            headbytes = headbytes + (await f.read(len(headbytes), readhint))
        rootdir = TDirectory()
        offset = rootdir.read(headbytes, offset)
        print("Root directory:", rootdir)

        keysbytes = await f.read(rootdir['fSeekKeys'], rootdir['fNbytesKeys'])
        keyslist = KeysList()
        offset = keyslist.read(keysbytes)
        print("Directory Keylist:", keyslist)

    return url, size


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

    urls = urls[:1]
    tic = time.time()
    for future in asyncio.as_completed(map(getsize, urls)):
        size = await future
        print(size)
    toc = time.time()
    print("Elapsed:", toc - tic)


if __name__ == '__main__':
    asyncio.run(main(), debug=True)
