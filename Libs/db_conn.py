#libraries
from sqlalchemy import create_engine
import sqlite3, inspect
# self libraries
import Libs.basic_function as bs
#origen database
source_sqlite = 'C:/App/SQLite/Learn/DataManagement.db'


#generar conexión con sqlalchemy
def sqlite_with_alchemy():
    try:
        engine = create_engine('sqlite:///'+source_sqlite)
        conn = engine.raw_connection()
        cursor = conn.cursor()
        bs.time_trans('Conexión con SQLAlchemy exitosa')
        return engine, conn, cursor
    except Exception as error:
        msg = 'Error on ' + inspect.currentframe().f_code.co_name
        bs.time_trans(msg, error)
#generar conexión con libreria sqlite3
def sqlite_with_lib():
    try:
        conn = sqlite3.connect(source_sqlite)
        cursor = conn.cursor()
        bs.time_trans('Conexión con Sqlite3 exitosa')
        return conn, cursor
    except Exception as error:
        msg = 'Error on ' + inspect.currentframe().f_code.co_name
        bs.time_trans(msg, error)

