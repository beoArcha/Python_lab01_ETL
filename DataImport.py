import sys
import sqlalchemy as db
from typing import Generator
from sqlalchemy import Column, Integer, String
from Tools import elapsed


class DataImport:
    """Imports data from csv file to sqlite database"""

    def __init__(self, address, separator=';', header=False, test=False):
        """C'str"""
        self.address = address
        self.separator = separator
        self.connected = False
        self.first_line_as_header = header
        self.engine = db.null
        self.metadata = db.MetaData()
        self.encoding = 'ISO-8859-1'
        self.columns_name_list = list()
        self.columns_name = ''
        self.test = test
        self.INSERT_QUERY = 'INSERT INTO {table} ({fields}) VALUES ({values})'
        self.DATABASE = 'sqlite:///music.sqlite'
        self.TEST_LIMIT = 10000

    def _read_line(self) -> Generator[list, list, str]:
        """Reads line of csv file"""
        with open(self.address, 'r', encoding=self.encoding) as ad:
            cnt = 0
            for line in ad:
                yield line.split(self.separator)
                cnt += 1
                if self.test and cnt >= self.TEST_LIMIT:
                    break
        return "Done, yielded {} lines".format(cnt)

    def _save_line(self, table_name: str, line: list) -> None:
        """Insert data into database"""
        values = ','.join(['\'{}\''.format(val.replace('\'', '\'\'')) for val in line])
        query = self.INSERT_QUERY.format(table=table_name,
                                         fields=self.columns_name,
                                         values=values)
        try:
            self.connected.execute(query)
        except Exception as e:
            raise

    def _create_table(self, name: str, number_of_columns: int, names_of_columns: list = None) -> None:
        """Creating table for sqlite database"""
        list_of_columns = list()
        list_of_columns.append(Column('id', Integer, primary_key=True, autoincrement=True))
        if names_of_columns is None and self.columns_name_list is None:
            names_of_columns = list()
            for i in range(number_of_columns):
                name_of_column = 'column_{:03}'.format(i + 1)
                names_of_columns.append(name_of_column)
                list_of_columns.append(Column(name_of_column, String))
            self.columns_name = ','.join(names_of_columns)
        else:
            if self.columns_name_list is not None:
                names_of_columns = self.columns_name_list
            for col in names_of_columns:
                list_of_columns.append(Column(col, String))
            self.columns_name = ','.join(names_of_columns)
        try:
            database = db.Table(name, self.metadata, *list_of_columns)
            database.create(self.engine)
        except Exception as e:
            ex = sys.exc_info()
            print('Unexpected error: {}\n{}'.format(str(ex[0]), e))

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
                except Exception as e:
                    print(e)
                    break
        else:
            print('First connect to your database engine')

    @elapsed
    def create_engine(self, **kwargs) -> None:
        """Creating connection to sqlite database"""
        self.engine = db.create_engine(self.DATABASE)
        self.connected = self.engine.connect()

    @elapsed
    def execute(self, query: str, **kwargs) -> list:
        """Execute query to sqlite database"""
        if self.connected:
            return self.connected.execute(query)
        else:
            self.create_engine()
            list_to_return = list()
            try:
                ret = self.connected.execute(query)
                list_to_return = [i for i in ret]
            except Exception as e:
                print(e)
            self.disconnect_engine()
            return list_to_return

    def disconnect_engine(self) -> None:
        """Disconnecting sqlite database engine"""
        if self.connected:
            self.connected.close()
            self.connected = False
        else:
            print('Not connected to database')
