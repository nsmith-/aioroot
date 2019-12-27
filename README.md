aioroot
-------
An asynchronous library for reading ROOT files over xrootd.

This package is an investigation into using asynchronous programming via `async/await`
syntax in python 3 to make remote reading of ROOT files as concurrent as possible.
The package takes a lot of inspiration from [uproot](https://github.com/scikit-hep/uproot).

The initial goal is to provide an equivalent to the `uproot.numentries` function, which is optimized
to extract the number of entries in a given TTree embedded in a ROOT file as quickly as possible.
In both implementations, the typical file requires 3 synchronous xrootd `read()` calls, however in
this library we: trigger the sequence of calls concurrently for all files to be read; do any decompression
in a thread pool outside the loop, and never block on read IO so the main thread can continue unpacking
the structures.  The net effect is an on average 5x speedup for a list of 10 files as shown in `demo.py`.
Running this demo at home (in the US):
```
$ ./demo.py
Elapsed (async): 3.28 s
Elapsed (uproot): 24.47 s
All entries agree? True
```
and running it on a CERN lxplus machine (very close to the file servers):
```
$ ./demo.py
Elapsed (async): 0.05 s
Elapsed (uproot): 0.24 s
All entries agree? True
```

In preparing this implementation, the structure unpacking functions had to be largely reworked
from uproot, as there the unpacking and IO are heavily intertwined.  I think the [sans-io](https://sans-io.readthedocs.io/)
philosophy may apply also to this case, and uproot could easily become both a sync/async library if the structure
unpacking is separated from IO fully.
