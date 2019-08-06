import csv, re, dpath
import yaml, sys
import psycopg2 as pg
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from color.colors import *


def show_query(cur, title, qry):
    print('%s' % (title))
    cur.execute(qry)
    widths = None
    result = list( cur.fetchall())
    for row in result:
        if widths is None:
            widths = [len( x ) for x in row ]
        else:
            for i in range( len( row )):
                word = row[i]
                width = widths[i]
                if word is None:
                    continue
                if len( word ) > width:
                    widths[i] = len( word )

    with open( 'output.txt', 'w' ) as f:
        for row in result:
            words = []
            for i in range( len( row )):
                word = str( row[i] )
                width = widths[i]
                while len( word ) < width:
                    word += ' '
                words.append( word )

            output = ' | '.join( words )
            f.write( output + '\n' )
#            print( output )
        


def setup( 
        query = None, 
        database = 'coffee', 
        schema = 'coffee', 
        user = 'escdata' ):
    conn = pg.connect( dbname = 'postgres', user = user )
    conn.set_isolation_level( ISOLATION_LEVEL_AUTOCOMMIT )

    cur = conn.cursor()

    try:
        cur.execute( 'DROP DATABASE IF EXISTS {0};'.format( database ))
        cur.execute( 'CREATE DATABASE {0};'.format( database ))
        cur.close()
        conn.close()
    except Exception as e:
        e = colorstr( ' '.join( e.args ))
        s = colorstr( 'Couldn\'t drop and create database!' )
        print( s.bold.yellow )
        print( e.bold.red )
        return (None, None)

    conn = pg.connect( dbname = database, user = user )
    cur = conn.cursor()

    if query is not None:
        try:
            cur.execute( query )
        except Exception as e:
            e = colorstr( ' '.join( e.args ))
            s = colorstr( 'Query broke!' )
            print( s.bold.yellow )
            print( e.bold.red )
            cur.close()
            conn.close()
            return (None, None)
    
    conn.commit()

    return (conn, cur)

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
    datatypes = {}
    tableitems = list( tables.items())
    tableitems.sort()
    for tablename, table in tableitems:
        insert = """INSERT INTO {0}.{1}
    ({{domain}})
    VALUES
    ({{values}});\n\n""".format( schema, tablename )

        td = []
        temp = { 'domain': [], 'values': [] }
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
                temp['domain'].append( columnname )
                temp['values'].append( '{{{}}}'.format( column['origin'] ))
                datatypes[column['origin']] = column['type']

            td.append( text )

        query += writetable( tablename, td ) + '\n\n'

        temp = {x: ','.join( y ) for x, y in temp.items() }
        insertion += insert.format( **temp )
    query += foreignkeys

    return query, insertion, datatypes

def builddatabase( query, database = 'coffee', schema = 'coffee' ):
    conn, cur = setup( query, database, schema )
    return conn, cur

def sqlrepl( m ):
    prefix = m.group(1)
    suffix = m.group(3)
    s = colorstr( m.group(2))
#    print( m )
    res = prefix + s.blue.bold + suffix
    
    return res

def sqlrepl1( m ):
    prefix = m.group(1)
    suffix = m.group(3)
    s = colorstr( m.group(2))
#    print( m )
    res = prefix + s.green + suffix
    
    return res

def processor( s, datatype, columnname ):

    months = {
        'Abril': 'April',
        'Julio': 'July',
        'Mayo': 'May',
        'Marzo': 'March'
    }

    if columnname == 'Harvest_Year':
        monthregex = re.compile( '(' + '|'.join( list( months.keys())) + ')' )
        s = re.sub( r'(\s*(\/|-)\s*)|( (a|to) )', '-', s )
        s = monthregex.sub( lambda m: months[m.group(1)], s )
        

    if "'" in s:
        s = s.replace( "'", "''" )

    if s is None or re.match( r'^(NA|na)?$', s ):
        s = 'NULL'

    elif re.match( '^(var)?char', datatype ):
        s = "'{}'".format( s )
    
    return s

def insertdata( conn, cur, datatypes ):
    if conn is None or cur is None:
        return
    with open( '../data/arabica_data_cleaned.csv', newline = '' ) as f:
        raw = csv.reader( f, quotechar = '"', delimiter = ',' )
        headers = next( raw )

        for row in raw:
            datapoint = {}
            for h in headers[1:]:
                value = row[headers.index(h)]
                value = processor( value, datatypes[h], h )
                datapoint[h] = value


            query = insertion.format( **datapoint )
            try:
                cur.execute( query )
            except Exception as e:
                e = colorstr( ' '.join( e.args ))
                s = colorstr( 'The insertion broke' )
                print( s.bold.yellow )
                print( e.bold.red )
                output = re.sub( sqlcolors, sqlrepl, query )
                output = re.sub( nonsqlcolors, sqlrepl1, output )
                print( output )
                cur.close()
                conn.close()
                break
    conn.commit()

dataplan = getdataplan()

query, insertion, datatypes = processdataplan( dataplan )

with open( 'setup.sql', 'w' ) as f:
    f.write( query )
conn, cur = builddatabase( query )

insertdata( conn, cur, datatypes )

query = """

SELECT DISTINCT expiration, grading_date, harvest_year FROM coffee.beans;

"""

show_query( cur, '', query )

cur.close()
conn.close()
