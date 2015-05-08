from sqlalchemy import Table, BigInteger, Boolean, Column, DateTime, Float, Integer, Numeric, String, Text, UnicodeText, MetaData, ForeignKey, Sequence

metadata = MetaData()

# SP [begin] =============================================================
customer = Table('CUSTOMER', metadata,
        Column('ID', Integer, primary_key=True),
        Column('NAME', String),
    )

goods = Table('GOODS', metadata,
        Column('ID', Integer, primary_key=True),
        Column('NAME', String),
    )

rate = Table('RATE', metadata,
        Column('CID', Integer, primary_key=True),
        Column('GID', Integer, primary_key=True),
        Column('RATE', Integer),
        Column('REAL', Integer)
    )

sqlite_sequence = Table('sqlite_sequence', metadata,
        Column('name', Integer),
        Column('seq', Integer)
    )