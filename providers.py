import os

from pathlib import Path
import pandas as pd
from loguru import logger
import mysql.connector

from config import connection_params, TABLE_NAME, trash_csv, cols_to_load


class RemoteProvider:
    """Ьазовый класс для удаленных провайдеров."""
    def __init__(self, connection_params):
        logger.debug('__init__')
        self.connection_params = connection_params
        self.connection = None
        self.cursor = None

    def open_connection(self):
        """Открывает коннект к базе."""
        logger.debug('open_connection')
        self.connection = mysql.connector.connect(**self.connection_params)

    def close_connection(self):
        """Закрывает коннект к базе."""
        logger.debug('close_connection')
        self.connection.close()

    def commit(self):
        """Кммитит изменения в базе."""
        logger.debug('commit')
        self.connection.commit()

    def create_cursor(self):
        """Создает курсор."""
        logger.debug('create_cursor')
        self.cursor = self.connection.cursor()

    def close_cursor(self):
        """Закрывает курсор."""
        logger.debug('close_cursor')
        self.cursor.close()


class WriterProvider(RemoteProvider):
    """Провайдер для записи в базу"""
    def __init__(self, *args, **kwargs):
        logger.debug('__init__')
        super().__init__(*args, **kwargs)

    @staticmethod
    def process_names(dataframe: pd.DataFrame) -> pd.DataFrame:
        """Функция для процессинга названий товаров."""
        logger.debug('__process_names')
        dataframe.loc[:, 'name'] = dataframe.loc[:, 'name'].str.replace("'", "")
        dataframe.loc[:, 'name'] = dataframe.loc[:, 'name'].str.replace('"', '')
        dataframe.loc[:, 'name'] = dataframe.loc[:, 'name'].str.replace('\\', '')
        return dataframe

    def insert_into_db(self, new_data):
        """Добавляет в базу новые штрихкоды."""
        logger.debug('insert_into_db')
        request_begin = "INSERT INTO {0}.{1} values".format(
            connection_params['database'],
            TABLE_NAME,
        )
        new_data = self.process_names(new_data)

        self.open_connection()
        self.create_cursor()

        batch_size = 20000
        logger.debug('batch_size {0}'.format(batch_size))

        for n_iter in range(new_data.shape[0]//batch_size+1):
            values = [request_begin]
            for _, row in new_data[n_iter*batch_size:batch_size*(n_iter+1)].iterrows():
                value = "({1}, \'{2}\', {3}),".format(
                    row['barcode'],
                    row['name'],
                    row['owner'],
                )
                values.append(value)
            values[-1] = values[-1][:-1]
            sql_request = " ".join(values)

            self.cursor.execute(sql_request)
            logger.debug('Step done: {0}/{1}'.format(n_iter+1, new_data.shape[0]//batch_size+1))

        self.commit()
        self.close_cursor()
        self.close_connection()


class UhhtBarcodeReference():
    """Провайдер для чтения из репозитрия."""
    def __init__(self, init_path: Path):
        logger.debug('__init__')
        self.init_pass = init_path
        self.all_data = self.__get_table()

    def __load_tables(self, cols: list) -> pd.DataFrame:
        logger.debug('__load_tables')
        dataframes = []
        for path in os.listdir(self.init_pass):
            if path not in trash_csv:
                try:
                    dataframe = pd.read_csv(
                        self.init_pass / path,
                        sep='\t',
                        usecols=cols,
                    )
                except pd.errors.ParserError:
                    logger.debug('{0}'.format(path))
                    continue
                dataframes.append(dataframe)
        dataframe = pd.concat(dataframes)
        return dataframe

    def __get_table(self) -> pd.DataFrame:
        logger.debug('__get_table')
        dataframe = self.__load_tables(cols_to_load)
        dataframe['owner'] = 100
        dataframe = dataframe.rename(columns={'UPCEAN': 'barcode', 'Name': 'name'})
        dataframe = dataframe.drop_duplicates('barcode')
        return dataframe

    def get_barcodes(self):
        """Возвращает штрихкоды."""
        logger.debug('get_barcodes')
        return set(self.all_data['barcode'].unique())

    def get_table_by_barcodes(self, barcodes):
        """Возвращает таблицу с новыми штрихкодами и названиями товаров."""
        logger.debug('get_table_by_barcodes')
        return self.all_data[self.all_data['barcode'].isin(barcodes)].reset_index(drop=True)


class UhhtBarcodeReferenceRemote(RemoteProvider):
    """Удаленный провайдер для чтения из базы."""
    def __init__(self, *args, **kwargs):
        logger.debug('__init__')
        super().__init__(*args, **kwargs)

    def get_barcodes(self):
        """Получает щтрихкода из базы"""
        logger.debug('get_barcodes')

        sql_request = 'select barcode from {0}.{1}'.format(
            connection_params['database'],
            TABLE_NAME,
        )

        logger.debug('{0}'.format(sql_request))
        self.open_connection()
        dataframe = pd.read_sql(sql_request, con=self.connection)
        self.close_connection()
        return set(dataframe['barcode'].unique())
