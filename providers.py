import os
import pandas as pd
from pathlib import Path
from loguru import logger
from abc import ABC, abstractmethod
import mysql.connector

import config


class RemoteProvider:
    def __init__(self, connection_params):
        logger.debug('__init__')
        self.connection_params = connection_params
        self.connection = None
        
    def open_connection(self):
        logger.debug('open_connection')
        self.connection = mysql.connector.connect(**self.connection_params)
    
    def close_connection(self):
        logger.debug('close_connection')
        self.connection.close()
        
    def commit(self):
        logger.debug('commit')
        self.connection.commit()
        
    def create_cursor(self):
        logger.debug('create_cursor')
        self.cursor = self.connection.cursor()
        
    def close_cursor(self):
        logger.debug('close_cursor')
        self.cursor.close()


class WriterProvider(RemoteProvider):
    def __init__(self, *args, **kwargs):
        logger.debug('__init__')
        super().__init__(*args, **kwargs)
         
    def __process_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Функция для процессинга названий товаров."""
        logger.debug('__process_names')
        df.loc[:, 'name'] = df.loc[:, 'name'].str.replace("'", "")
        df.loc[:, 'name'] = df.loc[:, 'name'].str.replace('"', '')
        df.loc[:, 'name'] = df.loc[:, 'name'].str.replace('\\', '')
        return df
    
    def insert_into_db(self, new_data):
        logger.debug('insert_into_db')
        request_begin = "INSERT INTO u0492853_enot_test.{0} values".format(config.table)
        new_data = self.__process_names(new_data)
        
        self.open_connection()
        self.create_cursor()
        
        batch_size = 20000
        logger.debug('batch_size {0}'.format(batch_size))
        
        for x in range(new_data.shape[0]//batch_size+1):
            values = [request_begin]
            for ind, row in new_data[x*batch_size:batch_size*(x+1)].iterrows():
                value = "({1}, \'{2}\', {3}),".format(
                    request_begin,
                    row['barcode'],
                    row['name'],
                    row['owner']
                )
                values.append(value)
            values[-1] = values[-1][:-1]
            sql_request = " ".join(values)
            
            self.cursor.execute(sql_request)
            logger.debug('Step done: {0}/{1}'.format(x+1, new_data.shape[0]//batch_size+1))
            
        self.commit()
        self.close_cursor()
        self.close_connection()            
    

class UhhtBarcodeReference():
    def __init__(self, init_path: Path):
        logger.debug('__init__')
        self.init_pass = init_path
        self.all_data = self.__get_table()

    def __load_tables(self, cols: list = []) -> pd.DataFrame:
        logger.debug('__load_tables')
        dfs = []
        for path in os.listdir(self.init_pass):
            if path not in config.trash_csv:
                
                try:
                    df = pd.read_csv(
                        self.init_pass / path,
                        sep='\t',
                        usecols=cols,
                    )
                except pd.errors.ParserError:
                    logger.debug('{0}'.format(path))
                    continue
                dfs.append(df)             
        df = pd.concat(dfs)
        return df

    def __get_table(self) -> pd.DataFrame:
        logger.debug('__get_table')
        df = self.__load_tables(config.cols_to_load)
        df['owner'] = 100
        df = df.rename(columns={'UPCEAN': 'barcode', 'Name': 'name'})
        df = df.drop_duplicates('barcode')
        return df
    
    def get_barcodes(self):
        logger.debug('get_barcodes')
        return set(self.all_data['barcode'].unique())
    
    def get_table_by_barcodes(self, barcodes):
        logger.debug('get_table_by_barcodes')
        return self.all_data[self.all_data['barcode'].isin(barcodes)].reset_index(drop=True)


class UhhtBarcodeReferenceRemote(RemoteProvider):
    def __init__(self, *args, **kwargs):
        logger.debug('__init__')
        super().__init__(*args, **kwargs) 
        
    def get_barcodes(self):
        logger.debug('get_barcodes')
        self.open_connection()

        df = pd.read_sql(
            'select barcode from u0492853_enot_test.{0}'.format(config.table),
            con=self.connection
        )
        self.close_connection()
        return set(df['barcode'].unique())
