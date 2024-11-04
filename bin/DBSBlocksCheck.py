#!/bin/python

import sys
import os
import time
import logging
import json
import re

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

    # rcl = Client(account=msConfig['rucioAccount'])
    rcl = Client(account='FIXME')

    con = cx_Oracle.connect(db_config.oraUser, db_config.oraPw, db_config.oraDsn)
    cursor = con.cursor()

    owner = db_config.oraOwner
    sqlStr = f"SELECT block_id, block_name FROM {owner}.blocks WHERE ROWNUM <= 5"
    pprint(cursor.execute(sqlStr).fetchall())

    blocksFilePath = os.environ.get('WMA_ROOT_DIR', "/data/WMAgent.venv3") + '/debug/blocks/'
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
        #     json.dump(blockRec, blockRecFile)

    # Extract only the filenames per block:
    blockFilesLists = {}
    for blockName, block in blockRecords.items():
        fileList = [file['lfn'] for file in block['file_conf_list']]
        blockFilesLists[blockName] = fileList
        print("----------------------------------------------------------------------")
        print(f"BlockName:{blockName}:")
        print(f"BlockFileList: {pformat(fileList)}")


    # Checking for existing lfn records:
    blockDBSRecResults = {}
    for blockName, lfnList in blockFilesLists.items():
        if blockName not in blockDBSRecResults:
            blockDBSRecResults[blockName] = {}
        print(f"Block: {blockName}:")
        for lfn in lfnList:
            sqlStr=f"SELECT block_name FROM {db_config.oraOwner}.blocks WHERE block_id = (SELECT block_id FROM cms_dbs3_k8s_global_owner.files WHERE logical_file_name = '{lfn}')"
            res = cursor.execute(sqlStr).fetchall()
            if res:
                if res[0][0] == blockName:
                    blockDBSRecResults[blockName][lfn] = 'OK'
                    print(f"\tLFN: {lfn}: OK")
                else:
                    blockDBSRecResults[blockName][lfn] = f"Block MISMATCH: {res}"
                    print(f"\tLFN: {lfn}: Block MISMATCH: {res}")
            else:
                blockDBSRecResults[blockName][lfn] = 'MISSING'
                print(f"LFN: {lfn}: MISSING")

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
