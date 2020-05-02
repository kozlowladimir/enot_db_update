import os
import mysql.connector

import pandas as pd
from loguru import logger
from pathlib import Path

import config
from providers import UhhtBarcodeReference, UhhtBarcodeReferenceRemote, WriterProvider


def main():
    with open('current_commit_hash', 'r') as f:
        current_commits_hash = f.read()

    try:
        with open('last_commit_hash', 'r') as f:
            last_commits_hash = f.read()
    except FileNotFoundError:
        last_commits_hash = ''

    if current_commits_hash == last_commits_hash:
        logger.debug('Nothing to update. Commits hashes are equal.')
        return 0
    
    init_path = Path(config.path_to_repositiry_data)
    
    uhhtBarcodeReference = UhhtBarcodeReference(init_path)
    uhhtBarcodeReferenceRemote = UhhtBarcodeReferenceRemote(config.connection_params)
    
    local_barcodes = uhhtBarcodeReference.get_barcodes()
    remote_barcodes = uhhtBarcodeReferenceRemote.get_barcodes()
    
    new_barcodes = local_barcodes - remote_barcodes
    if len(new_barcodes) > 0:
        logger.debug('There are {0} new barcodes'.format(len(new_barcodes)))
        
        new_table = uhhtBarcodeReference.get_table_by_barcodes(new_barcodes)
        writerProvider = WriterProvider(config.connection_params)
        writerProvider.insert_into_db(new_table)
        
    else:
        logger.debug('There are no new barcodes')
    
    with open('last_commit_hash', 'w') as f:
        f.write("{0}".format(current_commits_hash))

if __name__ == '__main__':
    main()
