import struct
from functools import partial
from collections.abc import Mapping


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


ClassMap = {
    b'TTree': TTree,
}
