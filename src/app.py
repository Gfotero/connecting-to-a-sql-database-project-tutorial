
# ****************** FORMA SOLICITADA PARA HACER EL EJERCICIO ****************************************************

import os
from flask_sqlalchemy import SQLAlchemy
import sqlalchemy
from sqlalchemy import *
import pandas as pd
import psycopg2
from sqlalchemy.sql import *

# Credenciales Base de Datos. 

user = 'postgres'       #0
password = 'america'    #1
host = 'localhost'      #2
port = 5432             #3
database = 'postgres'   #4

# 1) Connect to the database here using the SQLAlchemy's create_engine function
engine = create_engine(url='postgresql://{0}:{1}@{2}:{3}/{4}'.format(user,password,host,port,database)).execution_options(autocommit=True)

# 2) Execute the SQL sentences to create your tables using the SQLAlchemy's execute function
insp = inspect(engine)
metadata_obj = MetaData()


if 'publishers' not in insp.get_table_names():
        with engine.connect() as conn:
                conn.execute(text(""" CREATE TABLE publishers(publisher_id INT NOT NULL,
                name VARCHAR(255) NOT NULL,PRIMARY KEY(publisher_id))"""))
                conn.commit()

if 'authors' not in insp.get_table_names():
        with engine.connect() as conn:
                conn.execute(text("""CREATE TABLE authors(author_id INT NOT NULL,first_name VARCHAR(100) NOT NULL,
                middle_name VARCHAR(50) NULL,
                last_name VARCHAR(100) NULL,
                PRIMARY KEY(author_id))"""))
                conn.commit()

if 'books' not in insp.get_table_names():
        with engine.connect() as conn:  
                conn.execute(text("""CREATE TABLE books(book_id INT NOT NULL,title VARCHAR(255) NOT NULL,
                total_pages INT NULL, rating DECIMAL(4, 2) NULL, isbn VARCHAR(13) NULL, published_date DATE,
                publisher_id INT NULL, PRIMARY KEY(book_id),
                CONSTRAINT fk_publisher FOREIGN KEY(publisher_id) REFERENCES publishers(publisher_id))"""))
                conn.commit()

if 'book_authors' not in insp.get_table_names():
        with engine.connect() as conn:  
                conn.execute(text("""CREATE TABLE book_authors (book_id INT NOT NULL, author_id INT NOT NULL,
                PRIMARY KEY(book_id, author_id),
                CONSTRAINT fk_author FOREIGN KEY(author_id) REFERENCES authors(author_id) ON DELETE CASCADE)"""))
                conn.commit()
        with engine.connect() as conn:  
                conn.execute(text("""ALTER TABLE book_authors ADD CONSTRAINT fk_book FOREIGN KEY(book_id) REFERENCES books(book_id) ON DELETE CASCADE"""))
                conn.commit()


# 3) Execute the SQL sentences to insert your data using the SQLAlchemy's execute function

tabla1 = Table('publishers', metadata_obj, autoload_with=engine)

with engine.connect() as conn:
        result = engine.connect().execute(tabla1.select())
        if not result.fetchall():

                conn.execute(text("""INSERT INTO publishers(publisher_id, name) VALUES (1, 'O Reilly Media');
                INSERT INTO publishers(publisher_id, name) VALUES (2, 'A Book Apart');
                INSERT INTO publishers(publisher_id, name) VALUES (3, 'A K PETERS');
                INSERT INTO publishers(publisher_id, name) VALUES (4, 'Academic Press');
                INSERT INTO publishers(publisher_id, name) VALUES (5, 'Addison Wesley');
                INSERT INTO publishers(publisher_id, name) VALUES (6, 'Albert&Sweigart');
                INSERT INTO publishers(publisher_id, name) VALUES (7, 'Alfred A. Knopf')"""))
                conn.commit()

tabla2 = Table('authors', metadata_obj, autoload_with=engine)
with engine.connect() as conn:  
        result = engine.connect().execute(tabla2.select())
        if not result.fetchall():
                conn.execute(text("""INSERT INTO authors (author_id, first_name, middle_name, last_name) VALUES (1, 'Merritt', null, 'Eric');
                INSERT INTO authors (author_id, first_name, middle_name, last_name) VALUES (2, 'Linda', null, 'Mui');
                INSERT INTO authors (author_id, first_name, middle_name, last_name) VALUES (3, 'Alecos', null, 'Papadatos');
                INSERT INTO authors (author_id, first_name, middle_name, last_name) VALUES (4, 'Anthony', null, 'Molinaro');
                INSERT INTO authors (author_id, first_name, middle_name, last_name) VALUES (5, 'David', null, 'Cronin');
                INSERT INTO authors (author_id, first_name, middle_name, last_name) VALUES (6, 'Richard', null, 'Blum');
                INSERT INTO authors (author_id, first_name, middle_name, last_name) VALUES (7, 'Yuval', 'Noah', 'Harari');
                INSERT INTO authors (author_id, first_name, middle_name, last_name) VALUES (8, 'Paul', null, 'Albitz')"""))
                conn.commit()


tabla3 = Table('books', metadata_obj, autoload_with=engine)
with engine.connect() as conn:  
        result = engine.connect().execute(tabla3.select())
        if not result.fetchall():
                conn.execute(text("""INSERT INTO books (book_id, title, total_pages, rating, isbn, published_date, publisher_id) VALUES (1, 'Lean Software Development: An Agile Toolkit', 240, 4.17, '9780320000000', '2003-05-18', 5);
                INSERT INTO books (book_id, title, total_pages, rating, isbn, published_date, publisher_id) VALUES (2, 'Facing the Intelligence Explosion', 91, 3.87, null, '2013-02-01', 7);
                INSERT INTO books (book_id, title, total_pages, rating, isbn, published_date, publisher_id) VALUES (3, 'Scala in Action', 419, 3.74, '9781940000000', '2013-04-10', 1);
                INSERT INTO books (book_id, title, total_pages, rating, isbn, published_date, publisher_id) VALUES (4, 'Patterns of Software: Tales from the Software Community', 256, 3.84, '9780200000000', '1996-08-15', 1);
                INSERT INTO books (book_id, title, total_pages, rating, isbn, published_date, publisher_id) VALUES (5, 'Anatomy Of LISP', 446, 4.43, '9780070000000', '1978-01-01', 3);
                INSERT INTO books (book_id, title, total_pages, rating, isbn, published_date, publisher_id) VALUES (6, 'Computing machinery and intelligence', 24, 4.17, null, '2009-03-22', 4);
                INSERT INTO books (book_id, title, total_pages, rating, isbn, published_date, publisher_id) VALUES (7, 'XML: Visual QuickStart Guide', 269, 3.66, '9780320000000', '2009-01-01', 5);
                INSERT INTO books (book_id, title, total_pages, rating, isbn, published_date, publisher_id) VALUES (8, 'SQL Cookbook', 595, 3.95, '9780600000000', '2005-12-01', 7);
                INSERT INTO books (book_id, title, total_pages, rating, isbn, published_date, publisher_id) VALUES (9, 'The Apollo Guidance Computer: Architecture And Operation (Springer Praxis Books / Space Exploration)', 439, 4.29, '9781440000000', '2010-07-01', 6);
                INSERT INTO books (book_id, title, total_pages, rating, isbn, published_date, publisher_id) VALUES (10, 'Minds and Computers: An Introduction to the Philosophy of Artificial Intelligence', 222, 3.54, '9780750000000', '2007-02-13', 7)"""))
                conn.commit()

tabla4 = Table('book_authors', metadata_obj, autoload_with=engine)
with engine.connect() as conn:  
        result = engine.connect().execute(tabla4.select())
        if not result.fetchall():
                conn.execute(text("""INSERT INTO book_authors (book_id, author_id) VALUES (1, 1);
                INSERT INTO book_authors (book_id, author_id) VALUES (2, 8);
                INSERT INTO book_authors (book_id, author_id) VALUES (3, 7);
                INSERT INTO book_authors (book_id, author_id) VALUES (4, 6);
                INSERT INTO book_authors (book_id, author_id) VALUES (5, 5);
                INSERT INTO book_authors (book_id, author_id) VALUES (6, 4);
                INSERT INTO book_authors (book_id, author_id) VALUES (7, 3);
                INSERT INTO book_authors (book_id, author_id) VALUES (8, 2);
                INSERT INTO book_authors (book_id, author_id) VALUES (9, 4);
                INSERT INTO book_authors (book_id, author_id) VALUES (10, 1)"""))
                conn.commit()

#4) Use pandas to print one of the tables as dataframes using read_sql function
connection = engine.connect()
query = 'SELECT * FROM books'
df = pd.read_sql(query, connection)
print(df)
connection.close()

