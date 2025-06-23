from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from settings import DATABASE_PROVIDER, DATABASE_ADAPTER, DATABASE_USER, DATABASE_PASSWORD, DATABASE_HOST, \
    DATABASE_PORT, DATABASE_NAME


class DBConnectionHandler:

    def __init__(self) -> None:
        self.__connection_string = (f"{DATABASE_PROVIDER}+{DATABASE_ADAPTER}://"
                                    f"{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:"
                                    f"{DATABASE_PORT}/{DATABASE_NAME}")
        self.__engine = self.__create_database_engine()
        self.session = None

    def __create_database_engine(self):
        engine = create_engine(self.__connection_string)
        return engine

    def get_engine(self):
        return self.__engine

    def __enter__(self):
        session_make = sessionmaker(bind=self.__engine)
        self.session = session_make()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()
