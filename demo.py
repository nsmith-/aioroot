#!/usr/bin/env python
import time
import asyncio
import uproot
from functools import partial
from concurrent.futures import ThreadPoolExecutor
from aioroot import ROOTFile


async def getentries(url, threadpool=None):
    async with ROOTFile(url, threadpool=threadpool) as file:
        tree = await file[b'Events']
        # from pprint import pprint
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
        print("Elapsed (async): %.2f s" % (toc - tic))

        # prime the plumbing
        await asyncio.get_event_loop().run_in_executor(threadpool, uproot.numentries, urls[0], b'Events')

        entries_uproot = {}
        tic = time.time()
        for url in urls:
            entries = await asyncio.get_event_loop().run_in_executor(threadpool, uproot.numentries, url, b'Events')
            entries_uproot[url] = entries
        toc = time.time()
        print("Elapsed (uproot): %.2f s" % (toc - tic))

        print("All entries agree?", all(entries_async[url] == entries_uproot[url] for url in urls))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.set_debug(False)
    loop.run_until_complete(main())
