# ************* CONEXION A LA BASE DE DATOS USANDO LA LIBRERIA psycopg2 *******************************************

import psycopg2
try:
    Connection=psycopg2.connect(
        host='localhost',
        user='postgres',
        password='america',
        database='postgres'
        )
    
    print("Conexion Existosa")

    cursor=Connection.cursor()
    cursor.execute('SELECT version()')
    fila=cursor.fetchone()
    print(fila)

except Exception as ex:
    print(ex)

finally:
    Connection.close()
    print('Conexion Finalizada')