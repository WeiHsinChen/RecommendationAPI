import sys
sys.path.append("../..")

from sqlite3 import dbapi2 as sqlite

from sqlalchemy import create_engine

def get_engine():
	return None

def my_con_func():
	# print "My Connection func"
	import sqlite3.dbapi2 as sqlite
	import os
	con = sqlite.connect(os.path.join(os.path.dirname(os.path.abspath(__file__)),'..','data/rs.sqlite'))
	con.text_factory=str
	return con

def get_sqlite_db():
	engine = create_engine('sqlite+pysqlite:///',creator=my_con_func)
	# engine.connect().connection.connection.text_factory = str
	return engine

