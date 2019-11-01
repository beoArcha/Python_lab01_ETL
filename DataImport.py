import sqlalchemy as db
from typing import Generator
from Tools import elapsed


class DataImport:
    """Imports data from csv file to sqlite"""
    def __init__(self, address, separator=';', header=False):
        """C'stor"""
        self.address = address
        self.separator = separator
        self.connected = False
        self.first_line_as_header = header
        self.engine = db.null

    def _read_line(self) -> Generator[list, list, str]:
        """Reads line of csv file"""
        with open(self.address, 'r', encoding='ISO-8859-1') as ad:
            for line in ad:
                yield line.split(self.separator)
        return "Done"

    def _save_line(self, line: list) -> None:
        pass

    def _create_table(self, number_of_columns: int, names_of_columns: list = None):
        pass

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
                            self._create_table(len(line), line)
                        else:
                            self._create_table(len(line))
                            self._save_line(line)
                    else:
                        self._save_line(line)
                except StopIteration:
                    break
        else:
            print('First connect to your database engine')

    @elapsed
    def create_engine(self, **kwargs) -> None:
        """Creating connection to sqlite database"""
        self.engine = db.create_engine('sqlite:///music.sqlite')
        self.connected = self.engine.connect()
