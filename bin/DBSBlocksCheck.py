#!/bin/python

import sys
import os
import time
import logging
import json
import re
import pickle
import datetime

import db_config
import cx_Oracle

from unittest import TestCase
from argparse import ArgumentParser
from pprint import pformat, pprint

from rucio.client import Client

if __name__ == '__main__':
    """
    __DBSBlocksCheck__

    """

    FORMAT = "%(asctime)s:%(levelname)s:%(module)s:%(funcName)s(): %(message)s"
    logging.basicConfig(stream=sys.stdout, format=FORMAT, level=logging.INFO)
    logger = logging.getLogger(__name__)
    # reset_logging()

    logger.info("We are here")
    opt = ArgumentParser(usage=__doc__)
    opt.add_argument("-c", "--config", dest="config", required=False,
                     help="Service configuration file.")
    opts, args = opt.parse_known_args()

    rcl = Client(account='FIXME')

    con = cx_Oracle.connect(db_config.oraUser, db_config.oraPw, db_config.oraDsn)
    cursor = con.cursor()

    owner = db_config.oraOwner
    sqlStr = f"SELECT block_id, block_name FROM {owner}.blocks WHERE ROWNUM <= 5"
    pprint(cursor.execute(sqlStr).fetchall())

    blocksFilePath = os.environ.get('WMA_ROOT_DIR', "/data/WMAgent.venv3") + '/srv/DBSAux/debug/blocks/'
    blocksFileName =  'dbsuploader_all_blocks.json'
    with open(blocksFilePath + '/../'  + blocksFileName, 'rb') as blocksFile:
        blocksList = json.load(blocksFile)

    # Reduce the block records and put them into a dictionary.
    blockRecords = {}
    for blockRec in blocksList:
        blockName = blockRec['block']['block_name']
        blockHash = blockName.split('#')[1]
        print(f"BlockName: {blockName}")
        print(f"BlockHash: {blockHash}")
        blockRecords[blockName] = blockRec
        # with open(blocksFilePath + blockHash + '.json', 'w') as blockRecFile:
        #     json.dump(blockRec, blockRecFile, indent=4)

    # Extract only the filenames per block:
    blockFilesLists = {}
    for blockName, block in blockRecords.items():
        fileList = [file['lfn'] for file in block['file_conf_list']]
        blockFilesLists[blockName] = fileList
        print("----------------------------------------------------------------------")
        print(f"BlockName:{blockName}:")
        print(f"BlockFileList: {pformat(fileList)}")

    lfnAll = 0
    for block in blockFilesLists:
        lfnAll += len(blockFilesLists[block])

    # Checking for existing lfn records:
    blockDBSRecResults = {}
    queryNum = 0
    for blockName, lfnList in blockFilesLists.items():
        print(f"Block: {blockName}:")
        if blockName not in blockDBSRecResults:
            blockDBSRecResults[blockName] = {}

        # Fetch DBS Status for the block:
        sqlStr=f"SELECT block_name, open_for_writing, file_count FROM {db_config.oraOwner}.blocks WHERE block_name='{blockName}'"
        res = cursor.execute(sqlStr).fetchall()
        if res:
            blockDBSRecResults[blockName]['dbsStatus'] = dict(zip(('blockName', 'isOpen', 'fileCount'), res[0]))
        else:
            blockDBSRecResults[blockName]['dbsStatus'] = 'MISSING'

        # Fetch Rucio Status for the block:
        res = []
        # Convert all datetime strings returned from Rucio to strings digestible by json
        for rucioRec in rcl.list_dataset_replicas('cms', blockName):
            for recField in ['created_at','updated_at','accessed_at']:
                if isinstance(rucioRec[recField], datetime.datetime):
                    rucioRec[recField] = rucioRec[recField].strftime('%Y-%m-%dT%H:%M:%S.%f')
            res.append(rucioRec)
        blockDBSRecResults[blockName]['rucioStatus'] = res or 'MISSING'

        # Fetch DBS status per lfn
        blockDBSRecResults[blockName]['files'] = {}
        for lfn in lfnList:
            blockDBSRecResults[blockName]['files'][lfn] = {}
            queryNum += 1
            sqlStr=f"SELECT block_name FROM {db_config.oraOwner}.blocks WHERE block_id = (SELECT block_id FROM {db_config.oraOwner}.files WHERE logical_file_name = '{lfn}')"
            res = cursor.execute(sqlStr).fetchall()
            if res:
                if res[0][0] == blockName:
                    blockDBSRecResults[blockName]['files'][lfn]['dbsStatus'] = 'OK'
                    blockDBSRecResults[blockName]['files'][lfn]['blockName'] = res[0][0]
                    print(f"\t{queryNum}: LFN: {lfn}: OK")
                else:
                    blockDBSRecResults[blockName]['files'][lfn]['dbsStatus'] = 'BLOCKMISMATCH'
                    blockDBSRecResults[blockName]['files'][lfn]['blockName'] = res[0][0]
                    print(f"\t{queryNum}: LFN: {lfn}: BLOCKMISMATCH: {res[0][0]}")
            else:
                blockDBSRecResults[blockName]['files'][lfn]['dbsStatus'] = 'MISSING'
                blockDBSRecResults[blockName]['files'][lfn]['blockName'] = ""
                print(f"{queryNum}: LFN: {lfn}: MISSING")

    # Save the results
    with open(blocksFilePath + 'blockDBSRecords.pkl', 'wb') as blockDBSRecordsFile:
        pickle.dump(blockDBSRecResults, blockDBSRecordsFile)
    with open(blocksFilePath + 'blockDBSRecords.json', 'w') as blockDBSRecordsFile:
         json.dump(blockDBSRecResults, blockDBSRecordsFile, indent=4)

    # continue to aggregate the dbsStatus results:
    blockDBSRecResultsReduced={}
    for blockName,block in blockDBSRecResults.items():
        blockDBSRecResultsReduced[blockName]={}
        blockDBSRecResultsReduced[blockName]['blockDBSStatus'] = ['OK'] if isinstance(block['dbsStatus'], dict) else  [block['dbsStatus']]
        blockDBSRecResultsReduced[blockName]['filesDBSStatus'] = list(set([file['dbsStatus'] for file in block['files'].values()]))


    # Final reduce of all blocks info at DBS and result:
    blockDBSRecResultsReducedFinal = {}
    for blockName, block in blockDBSRecResultsReduced.items():
        if block['blockDBSStatus'] == block['filesDBSStatus'] == ['OK']:
            blockDBSRecResultsReducedFinal[blockName] = 'OK'
            print(f"\tblockName: {blockName}: OK")
        elif block['blockDBSStatus'] == block['filesDBSStatus'] == ['MISSING']:
            blockDBSRecResultsReducedFinal[blockName] = 'MISSING'
            print(f"\tblockName: {blockName}: MISSING")
        else:
            blockDBSRecResultsReducedFinal[blockName] = {}
            blockDBSRecResultsReducedFinal[blockName]['blockDBSStatus'] = block['blockDBSStatus']
            blockDBSRecResultsReducedFinal[blockName]['filesDBSStatus'] = block['filesDBSStatus']
            print(f"\tblockName: {blockName}: \n\t\tblockDBSStatus: {block['blockDBSStatus']}\n\t\tfilesDBSStatus: {block['filesDBSStatus']}")


    # -------------------------------------------------------------------------------
    # NOTE: The initial blocks list json was containing 3 records per block
    #       All of the code bellow served only to crosscheck if those 3 records
    #       per block were identical.
    # blockRecList = {}
    # blockRecRegList = {}
    # blockRecNamesList = {}
    # fileCount=0;
    # for blockRec in blocksList:
    #     fileCount += 1
    #     blockName = blockRec['block']['block_name']
    #     blockHash = blockName.split('#')[1]
    #     print(f"BlockName: {blockName}")
    #     print(f"BlockHash: {blockHash}")
    #     blockRecList[f"{blockName}_{fileCount}"] = blockRec
    #     blockRecRegList[blockName] = re.compile(f'{blockName}_.*')
    #     with open(blocksFilePath + blockHash + '.json', 'w') as blockRecFile:
    #         json.dump(blockRec, blockRecFile)

    # for blockRec in blocksList:
    #     blockName = blockRec['block']['block_name']
    #     print(f"BlockName: {blockName}")
    #     if blockName not in blockRecNamesList:
    #         blockRecNamesList[blockName] = set()

    #     for blockRecName in blockRecList.keys():
    #         if re.match(blockRecRegList[blockName], blockRecName):
    #             blockRecNamesList[blockName].add(blockRecName)

    # blockRecNamesCombinations = {}
    # for blockName in blockRecNamesList:
    #     blockRecNamesCombinations[blockName]=list(combinations(blockRecNamesList[blockName], 2))

    # test = TestCase()
    # for blockName, combs  in blockRecNamesCombinations.items():
    #     for comb in combs:
    #         d1 = blockRecList[comb[0]]
    #         d2 = blockRecList[comb[1]]
    #         print(f'd1: blockRecList{[comb[0]]}')
    #         print(f'd2: blockRecList{[comb[1]]}')
    #         test.assertEqual(d1, d2)
    # -------------------------------------------------------------------------------
