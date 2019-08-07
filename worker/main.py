import csv, re, dpath
import yaml, sys
import psycopg2 as pg
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from color.colors import *


        
# returns a cursor object
def getcursor( database = 'coffee', user = 'escdata', setisolation = False ):
    conn = pg.connect( dbname = database, user = user )

    if setisolation:
        conn.set_isolation_level( ISOLATION_LEVEL_AUTOCOMMIT )

    cur = conn.cursor( cursor_factory = db )

    return cur

# sets up database
def setup( 
        query = None, 
        database = 'coffee', 
        schema = 'coffee', 
        user = 'escdata' ):
    
    cur = getcursor( 'postgres', user, True )

    try:
        cur.execute( 'DROP DATABASE IF EXISTS {0};'.format( database ))
        cur.execute( 'CREATE DATABASE {0};'.format( database ))

    except Exception as e:
        e = colorstr( ' '.join( e.args ))
        s = colorstr( 'Couldn\'t drop and create database!' )
        print( s.bold.yellow )
        print( e.bold.red )
        cur.closeall()
        return

    cur = getcursor( database, user )

    if query is not None:
        try:
            cur.execute( query )
        except Exception as e:
            e = colorstr( ' '.join( e.args ))
            s = colorstr( 'Query broke!' )
            print( s.bold.yellow )
            print( e.bold.red )

            cur.closeall()
            return
    
        cur.connection.commit()

    return cur


# Writes the SQL code for an individual create table statement
def writetable( tn, td, schema = 'coffee' ):

    res = 'CREATE TABLE {0}.{1} (\n{2}\n);'.format( schema, tn, ',\n'.join( td ))

    return res


# Reads the YAML data plan
def getdataplan():
    with open( 'dataplan.yml', 'r' ) as f:
        data = yaml.load( f )
    return data


# Processes data plan to create the desired database architecure.
def processdataplan( dataplan, database = 'coffee', schema = 'coffee' ):

#    definitions = dataplan['definitions']
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
    for tablename, table in tables.items(): # < 3.6 order is not preserved
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


# Not really necesary. Runs setup() assuming the existence of a query
def builddatabase( query, database = 'coffee', schema = 'coffee' ):
    cur = setup( query, database, schema )
    return cur

# NEEDS TO BE DEPRECATEDD
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


# Attempt at a data processor prior to insertion into database. Current issue 
# is that at this step, the information on the column in question is not 
# explicitly available...
def processor( s, datatype, columnname ):

    months = {
        'Abril': 'April',
        'Julio': 'July',
        'Mayo': 'May',
        'Marzo': 'March'
    }

    if columnname == 'Harvest_Year':

        if re.match( r'^(mmm|TEST)$', s ):
            s = 'NULL'

        monthregex = re.compile( '(' + '|'.join( list( months.keys())) + ')' )
        s = re.sub( r'(\s*(\/|-)\s*|\s+(a|to|through)\s+)', '-', s, re.I )
        s = monthregex.sub( lambda m: months[m.group(1)], s )
        s = re.sub( r'( crop| in Colombia\.)', '', s )
        s = s.replace( '4T72010', '4T-2010' )
        s = re.sub( r'^(\d)(t|7)', r'\g<1>T', s )
        s = re.sub( r'(^|\D)(\d{2})(?!\d)', r'\g<1>20\g<2>', s )
        

    if "'" in s:
        s = s.replace( "'", "''" )

    if s is None or re.match( r'^(NA|na)?$', s ):
        s = 'NULL'

    if s != 'NULL':

        if re.match( '^(var)?char', datatype ):
            s = "'{}'".format( s )
        
        if datatype == 'date':
            s = "to_date( '{}', 'Month DDth, YYYY' )".format( s )
    
    return s




# mains script...loosely
def main( 
    writetofile = False, 
    build = True,
    insertdata = True ):
    dataplan = getdataplan()

    setupquery, insertion, datatypes = processdataplan( dataplan )

    if writetofile:
        with open( 'setup.sql', 'w' ) as f:
            f.write( setupquery )

    if build:
        cur = builddatabase( setupquery )

        if insertdata:
            cur.insertdata( datatypes, insertion )

        conn = cur.connection
        cur.close()
        conn.close()

def reload():
    try:
        cur.closeall()
    except Exception as e:
        colorstr( ' '.join( e.args )).bold.red

    main()

    return getcursor()


# class extension of cursor object
class db( pg.extensions.cursor ):
    def closeall( self ):
        conn = self.connection
        self.close()
        conn.close()

    # inserts data
    def insertdata( self, datatypes, insertion ):

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
                    self.execute( query )

                except Exception as e:
                    e = colorstr( ' '.join( e.args ))
                    s = colorstr( 'The insertion broke' )
                    print( s.bold.yellow )
                    print( e.bold.red )
                    output = re.sub( sqlcolors, sqlrepl, query )
                    output = re.sub( nonsqlcolors, sqlrepl1, output )
                    print( output )

                    self.closeall()
                    break

        self.connection.commit()

    def query( self, query, title = '' ):
        print( title )

        self.execute( query )

        widths = None
        result = list( self.fetchall())

        for row in result:
            if widths is None:
                widths = [len( str( x )) for x in row ]

            else:
                for i in range( len( row )):
                    word = str( row[i] )
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

                print( output )
