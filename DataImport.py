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

    def _read_line(self) -> Generator[list, list, str]:
        """Reads line of csv file"""
        with open(self.address, 'r', encoding='ISO-8859-1') as ad:
            for line in ad:
                yield line.split(self.separator)
        return "Done"

    def _save_line(self, table_name: str, line: list) -> None:
        pass

    def _create_table(self, name: str, number_of_columns: int, names_of_columns: list = None) -> None:
        """Creating table for sqlite database"""
        list_of_columns = list(Column('id', Integer, primary_key=True))
        if names_of_columns is None:
            for i in range(number_of_columns):
                list_of_columns.append(Column('column_{:03}'.format(i + 1), String))
        else:
            for col in names_of_columns:
                list_of_columns.append(Column(col, String))
        try:
            db.Table(name, self.metadata, list_of_columns)
        except:
            ex = sys.exc_info()
            print("Unexpected error:" + str(ex[0]))

    @elapsed
    def data_import(self, table_name: str, **kwargs) -> None:
        """Importing data from csv file to sqlite database"""
        if self.connected:
            print(True)
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
