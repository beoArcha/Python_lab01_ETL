import sqlalchemy as db
from Tools import elapsed


class DataImport:
    """Imports data from csv file to sqlite"""
    def __init__(self, address, separator=';'):
        self.address = address
        self.separator = separator
        self.connected = False
        self.engine = db.null

    @elapsed
    def data_import(self, **kwargs):
        """Importing dat from csv file to sqlite database"""
        if self.connected:
            pass
        else:
            print('First connect to your database engine')

    @elapsed
    def create_engine(self, **kwargs):
        """Creating connection to sqlite database"""
        self.engine = db.create_engine('sqlite:///music.sqlite')
        self.connected = self.engine.connect()
