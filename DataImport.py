import sys
import sqlalchemy as db
from typing import Generator
from sqlalchemy import Column, Integer, String
from Tools import elapsed


class DataImport:
    """Imports data from csv file to sqlite"""

    def __init__(self, address, separator=';', header=False):
        """C'str"""
        self.address = address
        self.separator = separator
        self.connected = False
        self.first_line_as_header = header
        self.engine = db.null
        self.metadata = db.MetaData()
        self.encoding = 'ISO-8859-1'
        self.columns_name = ''
        self.INSERT_QUERY = 'INSERT INTO {table} ({fields} VALUES ({values}))'

    def _read_line(self) -> Generator[list, list, str]:
        """Reads line of csv file"""
        with open(self.address, 'r', encoding=self.encoding) as ad:
            for line in ad:
                yield line.split(self.separator)
        return "Done"

    def _save_line(self, table_name: str, line: list) -> None:
        """Insert data into database"""
        query = self.INSERT_QUERY.format(table=table_name, fields=self.columns_name, values=','.join(line))

    def _create_table(self, name: str, number_of_columns: int, names_of_columns: list = None) -> None:
        """Creating table for sqlite database"""
        list_of_columns = list()
        list_of_columns.append(Column('id', Integer, primary_key=True, autoincrement=True))
        if names_of_columns is None:
            names_of_columns = list()
            for i in range(number_of_columns):
                name_of_column = 'column_{:03}'.format(i + 1)
                names_of_columns.append(name_of_column)
                list_of_columns.append(Column(name_of_column, String))
            self.columns_name = ','.join(names_of_columns)
        else:
            for col in names_of_columns:
                list_of_columns.append(Column(col, String))
            self.columns_name = ','.join(names_of_columns)
        try:
            database = db.Table(name, self.metadata, *list_of_columns)
            database.create(self.engine)
        except Exception as e:
            ex = sys.exc_info()
            print('Unexpected error: {}\n{}'.format(str(ex[0]), e))

    def _get_columns_name(self) -> str:
        pass

    @elapsed
    def data_import(self, table_name: str, **kwargs) -> None:
        """Importing data from csv file to sqlite database"""
        if self.connected:
            generator = self._read_line()
            is_first = True
            while True:
                try:
                    line = next(generator)
                    if is_first:
                        is_first = False
                        if self.first_line_as_header:
                            self._create_table(table_name, len(line), line)
                        else:
                            self._create_table(table_name, len(line))
                            self._save_line(table_name, line)
                    else:
                        self._save_line(table_name, line)
                except StopIteration:
                    break
        else:
            print('First connect to your database engine')

    @elapsed
    def create_engine(self, **kwargs) -> None:
        """Creating connection to sqlite database"""
        self.engine = db.create_engine('sqlite:///music.sqlite')
        self.connected = self.engine.connect()
