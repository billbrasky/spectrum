import csv, re
import yaml
import psycopg2 as pg
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def show_query(cur, title, qry):
    print('%s' % (title))
    cur.execute(qry)
    for row in cur.fetchall():
        print(row)
    print('')


def setup( 
        query = None, 
        database = 'coffee', 
        schema = 'coffee', 
        user = 'escdata' ):
    conn = pg.connect( dbname = 'postgres', user = user )
    conn.set_isolation_level( ISOLATION_LEVEL_AUTOCOMMIT )

    cur = conn.cursor()

    cur.execute( 'DROP DATABASE IF EXISTS {0};'.format( database ))
    cur.execute( 'CREATE DATABASE {0};'.format( database ))
    show_query( cur, 'current database', 'SELECT current_database()' )
    cur.close()
    conn.close()

    conn = pg.connect( dbname = database, user = user )
    cur = conn.cursor()

    show_query( cur, 'current database', 'SELECT current_database()' )



    if query is not None:
        cur.execute( query )
    
    show_query( cur, 'sdfds', 'select * from information_schema.tables')
    conn.commit()

    return cur

def writetable( tn, td, schema = 'coffee' ):

    res = 'CREATE TABLE {0}.{1} (\n{2}\n);'.format( schema, tn, ',\n'.join( td ))

    return res


def getdataplan():
    with open( 'dataplan.yml', 'r' ) as f:
        data = yaml.load( f )
    return data

def processdataplan( dataplan, database = 'coffee', schema = 'coffee' ):

    definitions = dataplan['definitions']
    tables = dataplan['tables']

    query = """DROP SCHEMA IF EXISTS {0};
CREATE SCHEMA {0};

""".format( schema )


    foreignkeys = ''
    alter = """
ALTER TABLE {schema}.{table} 
    ADD FOREIGN KEY ("{key}")
    REFERENCES {schema}.{ftable} ("{fkey}");
"""
    insertion = ''
    for tablename, table in tables.items():
        insert = """INSERT INTO {0}.{1}
    ({{domain}})
    VALUES
    ({{values}});\n\n""".format( schema, tablename )

        td = []
        temp = { 'domain': [], 'values': [] }
        values = []
        for columnname, column in table.items():

            text = '    {0} {1}'.format( columnname, column['type'] )
            
            isprimary = column.get( 'pk', False )
            isforeign = column.get( 'fk', False )

            if isforeign:
                o = {
                    'schema': schema,
                    'table': tablename,
                    'ftable': '_'.join( columnname.split( '_' )[:-1] ),
                    'key': columnname,
                    'fkey': columnname.split( '_' )[-1]
                }

                foreignkeys += alter.format( **o )
    
            elif isprimary:
                text += ' PRIMARY KEY'

            else:
                s = '{{{}}}'
                if re.match( '^(var)?char', column['type'] ):
                    s = "'{{{}}}'"
                temp['domain'].append( columnname )
                temp['values'].append( s.format( column['origin'] ))

            td.append( text )

        query += writetable( tablename, td ) + '\n\n'

        temp = {x: ','.join( y ) for x, y in temp.items() }
        insertion += insert.format( **temp )
    query += foreignkeys

    return query, insertion

def builddatabase( query, database = 'coffee', schema = 'coffee', user = 'escadmin' ):
    cur = setup( query, database, schema, user )
    return cur

def insertdata( cur ):

    with open( '../data/arabica_data_cleaned.csv', newline = '' ) as f:
        raw = csv.reader( f, quotechar = '"', delimiter = ',' )
        headers = next( raw )

        for row in raw:
            datapoint = {h: row[headers.index(h)] for h in headers}

            query = insertion.format( **datapoint )

            print( query )
            cur.execute( query )



dataplan = getdataplan()
query, insertion = processdataplan( dataplan )

with open( 'query.sql', 'w' ) as f:
    f.write( query )

cur = builddatabase( query, user = 'dsuar' )

insertdata( cur )



