
# ********************** SEGUNDA FORMA DE HACER ESTE PROYECTO *****************************************************

import os
from flask_sqlalchemy import SQLAlchemy
import sqlalchemy
from sqlalchemy import *
import pandas as pd
import psycopg2
from sqlalchemy.sql import *

# Credenciales Base de Datos. load the .env file variables

user = 'postgres'       #0
password = 'america'    #1
host = 'localhost'      #2
port = 5432             #3
database = 'postgres'   #4

# 1) Connect to the database here using the SQLAlchemy's create_engine function
engine = create_engine(url='postgresql://{0}:{1}@{2}:{3}/{4}'.format(user,password,host,port,database)).execution_options(autocommit=True)
#conn=engine.connect()
metadata_obj = MetaData()


# 2) Execute the SQL sentences to create your tables using the SQLAlchemy's execute function


publishers=Table("publishers",metadata_obj,
    Column("publisher_id",Integer,nullable=False, primary_key=True),
    Column("name",String(255),nullable=False))

authors=Table('authors',metadata_obj,
        Column("author_id",Integer,nullable=False,primary_key=True),
        Column('first_name',String(100),nullable=False),
        Column('middle_name',String(50),nullable=True),
        Column('last_name',String(100),nullable=True))

books=Table('books',metadata_obj,
        Column("book_id",Integer,nullable=False,primary_key=True),
        Column('title',String(255),nullable=False),
        Column('total_pages', Integer,nullable=True),
        Column('rating', Numeric(4,2),nullable=True),
        Column('isbn',String(13),nullable=True),
        Column('published_date',Date),
        Column('publisher_id', Integer, ForeignKey('publishers.publisher_id',name='fk_publisher_id'), nullable=False))

book_authors=Table('book_authors',metadata_obj,
        Column('book_id', Integer, ForeignKey('books.book_id', name='fk_book_id', ondelete='CASCADE'), nullable=False),
        Column('author_id', Integer, ForeignKey('authors.author_id', name='fk_author_id', ondelete='CASCADE'), nullable=False),
        PrimaryKeyConstraint('book_id', 'author_id'))
        
metadata_obj.create_all(engine) 

# 3) Execute the SQL sentences to insert your data using the SQLAlchemy's execute function

tabla1 = Table('publishers', metadata_obj, autoload_with=engine)

with engine.connect() as conn:
        result = engine.connect().execute(tabla1.select())
        if not result.fetchall():
                data1 = [{'publisher_id': 1, 'name': 'O Reilly Media'}, {'publisher_id': 2, 'name': 'A Book Apart'},
                {'publisher_id': 3, 'name': 'A K PETERS'}, {'publisher_id': 4, 'name':'Academic Press'},
                {'publisher_id': 5, 'name':'Addison Wesley' },{'publisher_id': 6, 'name':'Albert&Sweigart'},
                {'publisher_id': 7, 'name':'Alfred A. Knopf'}]
        
                stmt1 = tabla1.insert().values([dict(row) for row in data1])
                conn.execute(stmt1)
                conn.commit()

tabla2 = Table('authors', metadata_obj, autoload_with=engine)

with engine.connect() as conn:
    result = engine.connect().execute(tabla2.select())
    if not result.fetchall():
        data2 = [{'author_id':1, 'first_name': 'Merritt', 'middle_name':Null(),'last_name':'Eric'},
        {'author_id':2, 'first_name': 'Linda', 'middle_name': Null(),'last_name':'Mui'},
        {'author_id':3, 'first_name': 'Alecos', 'middle_name':Null(),'last_name':'Papadatos'},
        {'author_id':4, 'first_name': 'Anthony', 'middle_name': Null(),'last_name':'Molinaro'},
        {'author_id':5, 'first_name': 'David', 'middle_name': Null(),'last_name':'Cronin'},
        {'author_id':6, 'first_name': 'Richard', 'middle_name': Null(),'last_name':'Blum'},
        {'author_id':7, 'first_name': 'Yuval', 'middle_name': 'Noah','last_name':'Harari'},
        {'author_id':8, 'first_name': 'Paul', 'middle_name': Null(),'last_name':'Albitz'}]

        stmt2 = tabla2.insert().values([dict(row) for row in data2])
        conn.execute(stmt2)
        conn.commit()

tabla3 = Table('books', metadata_obj, autoload_with=engine)

with engine.connect() as conn:
    result = engine.connect().execute(tabla3.select())
    if not result.fetchall():
        data3 = [
        {"book_id": 1, "title": "Lean Software Development: An Agile Toolkit", "total_pages": 240, "rating": 4.17, "isbn": "9780320000000", "published_date": "2003-05-18", "publisher_id": 5},
        {"book_id": 2, "title": "Facing the Intelligence Explosion", "total_pages": 91, "rating": 3.87, "isbn": None, "published_date": "2013-02-01", "publisher_id": 7},
        {"book_id": 3, "title": "Scala in Action", "total_pages": 419, "rating": 3.74, "isbn": "9781940000000", "published_date": "2013-04-10", "publisher_id": 1},
        {"book_id": 4, "title": "Patterns of Software: Tales from the Software Community", "total_pages": 256, "rating": 3.84, "isbn": "9780200000000", "published_date": "1996-08-15", "publisher_id": 1},
        {"book_id": 5, "title": "Anatomy Of LISP", "total_pages": 446, "rating": 4.43, "isbn": "9780070000000", "published_date": "1978-01-01", "publisher_id": 3},
        {"book_id": 6, "title": "Computing machinery and intelligence", "total_pages": 24, "rating": 4.17, "isbn": None, "published_date": "2009-03-22", "publisher_id": 4},
        {"book_id": 7, "title": "XML: Visual QuickStart Guide", "total_pages": 269, "rating": 3.66, "isbn": "9780320000000", "published_date": "2009-01-01", "publisher_id": 5},
        {"book_id": 8, "title": "SQL Cookbook", "total_pages": 595, "rating": 3.95, "isbn": "9780600000000", "published_date": "2005-12-01", "publisher_id": 7},
        {"book_id": 9, "title": "The Apollo Guidance Computer: Architecture And Operation (Springer Praxis Books / Space Exploration)", "total_pages": 439, "rating": 4.29, "isbn": "9781440000000", "published_date": "2010-07-01", "publisher_id": 6},
        {"book_id": 10, "title": "Minds and Computers: An Introduction to the Philosophy of Artificial Intelligence", "total_pages": 222, "rating": 3.54, "isbn": "9780750000000", "published_date": "2007-02-13", "publisher_id": 7}
        ]

        stmt3 = tabla3.insert().values([dict(row) for row in data3])
        conn.execute(stmt3)
        conn.commit()

tabla4 = Table('book_authors', metadata_obj, autoload_with=engine)

with engine.connect() as conn:
    result = engine.connect().execute(tabla4.select())
    if not result.fetchall():
        data4 = [{'book_id':1, 'author_id':1},
                {'book_id':2, 'author_id':8},
                {'book_id':3, 'author_id':7},
                {'book_id':4, 'author_id':6},
                {'book_id':5, 'author_id':5},
                {'book_id':6, 'author_id':4},
                {'book_id':7, 'author_id':3},
                {'book_id':8, 'author_id':2},
                {'book_id':9, 'author_id':4},
                {'book_id':10, 'author_id':1},
        ]

        stmt4 = tabla4.insert().values([dict(row) for row in data4])
        conn.execute(stmt4)
        conn.commit() 

#4) Use pandas to print one of the tables as dataframes using read_sql function
connection = engine.connect()
query = 'SELECT * FROM books'
df = pd.read_sql(query, connection)
print(df)
connection.close()
