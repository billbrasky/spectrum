import csv, re
import yaml, sys, os
import psycopg2 as pg
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Attempts to load WIP color library
try:
    from color.color import *
except ImportError as e:
    print( ' '.join( e.args ))
    print( 'No Colors!' )

# Hacky way of logging... use logging?
def log( listoftuples ):
    for tple in listoftuples:
        text, affects = tple
        if 'color' not in sys.modules:
            print( text )

        else:
            text = colorstr( text )
            for affect in affects:
                text = getattr( text, affect )
            print( text )

        
# returns a cursor object
def getcursor( database = 'coffee', user = 'postgres', setisolation = False ):
    if user == 'postgres':
        password = getyaml( 'local-password' )['password']
    else:
        password = None

    o = {'db': database, 'u': user, 'p': password }    

    if setisolation:
        o['db'] = 'postgres'

    conn = pg.connect( "dbname = '{db}' user = '{u}' password = '{p}'".format( **o ))
    
    if setisolation:
        conn.set_isolation_level( ISOLATION_LEVEL_AUTOCOMMIT )

    cur = conn.cursor( cursor_factory = db )

    return cur

# sets up database
def setup( 
        query = None, 
        database = 'coffee', 
        schema = 'coffee', 
        user = 'postgres' ):
    
    cur = getcursor( database, user, True )

    try:
        cur.execute( 'DROP DATABASE IF EXISTS {0};'.format( database ))
        cur.execute( 'CREATE DATABASE {0};'.format( database ))

    except Exception as e:
        e = ' '.join( e.args )
        s = 'Couldn\'t drop and create database!'
        logtext = [(s, ['bold', 'yellow']), (e, ['bold', 'red'])]
        log( logtext )
        cur.closeall()
        return

    cur = getcursor( database, user )

    if query is not None:
        try:
            cur.execute( query )
        except Exception as e:
            e = colorstr( ' '.join( e.args ))
            s = colorstr( 'Query broke!' )
            logtext = [(s, ['bold', 'yellow']), (e, ['bold', 'red'])]
            log( logtext )
            return
    
        cur.connection.commit()

    return cur


# Writes the SQL code for an individual create table statement
def writetable( tn, td, schema = 'coffee' ):

    res = 'CREATE TABLE {0}.{1} (\n{2}\n);'.format( schema, tn, ',\n'.join( td ))

    return res


# Reads a YAML file
def getyaml( s ):
    with open( s + '.yml', 'r' ) as f:
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

            columntype = column['type']
            columnlength = column.get( 'length' )
            isnotnull = column.get( 'notNull', False )

            sqltype = columntype.upper()

            if columnlength is not None:
                sqltype += '({})'.format( columnlength )

            if isnotnull:
                sqltype += ' NOT NULL'

            text = '    {0} {1}'.format( columnname, sqltype )

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
# def builddatabase( query, database = 'coffee', schema = 'coffee' ):
#     cur = setup( query, database, schema )
#     return cur


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
    dirpath,
    database = 'coffee',
    schema = 'coffee',
    user = 'escdata',
    writetofile = False, 
    build = True,
    insertdata = True ):

    if not os.path.exists( 'log' ):
        os.mkdir( 'log' )

    dataplan = getyaml( dirpath + '/dataplan' )

    setupquery, insertion, datatypes = processdataplan( dataplan )

    if writetofile:
        with open( 'log/setup.sql', 'w' ) as f:
            f.write( setupquery )

    if build:
        cur = setup( setupquery, database, schema, user )

        if insertdata:
            datapath = dirpath + '/../data/arabica_data_cleaned.csv'
            cur.insertdata( datatypes, insertion, datapath, writetofile )

        conn = cur.connection
        cur.close()
        conn.close()


def reload():
    try:
        cur.closeall()
    except Exception as e:
        log( [(' '.join( e.args ), ['bold', 'red'] )] )

    main()

    return getcursor()


# class extension of cursor object
class db( pg.extensions.cursor ):
    def closeall( self ):
        conn = self.connection
        self.close()
        conn.close()

    # inserts data
    def insertdata( self, datatypes, insertion, datapath, writetofile = False ):

        querylog = ''

        with open( datapath, newline = '' ) as f:
            raw = csv.reader( f, quotechar = '"', delimiter = ',' )
            headers = next( raw )

            for row in raw:
                datapoint = {}
                for h in headers[1:]:
                    value = row[headers.index(h)]
                    value = processor( value, datatypes[h], h )
                    datapoint[h] = value


                query = insertion.format( **datapoint )
                if writetofile:
                    querylog += query

#                print( query )

                try:
                    self.execute( query )

                except Exception as e:
                    e = colorstr( ' '.join( e.args ))
                    s = colorstr( 'The insertion broke' )
                    logtext = [(s, ['bold', 'yellow']), (e, ['bold', 'red'])]
                    log( logtext )
#                    output = re.sub( sqlcolors, sqlrepl, query )
#                    output = re.sub( nonsqlcolors, sqlrepl1, output )
#                    print( output )

                    self.closeall()
                    break

        if writetofile:
            with open( 'log/insert.sql', 'w' ) as f:
                f.write( querylog )

        self.connection.commit()

    # Excute a select query that is pretty printed to terminal
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

        with open( 'log/output.txt', 'w' ) as f:
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

    # apply updates to database
    def update( self, s ):

        queries = getyaml( s )['queries']

        with open( s + '.sql', 'r' ) as f:
            updatetemplate = f.read()

        for query in queries:

            sqlupdate = updatetemplate.format( select = query )
            print( sqlupdate)
            print( '-----------')
            self.execute( sqlupdate )
            self.connection.commit()

args = sys.argv

if len( args ) < 2:
    print( 'What do you want to do?' )
    exit()

fstring = args[1]

f = vars().get( fstring )

if f is None:
    print( "The function '{}' does not exist.".format( fstring ))
    exit()


parameters = {}
for parameter in args[2:]:
    if '=' not in parameter:
        print( 'You have to specify the variable name along with the value.' )
        exit()

    key, value = parameter.split( '=' )

    parameters[key] = value

try:
    f( **parameters )

except TypeError as e:
    print( 'Parameter Names:\n' )
    for var in f.__code__.co_varnames[:f.__code__.co_argcount]:
        print( '\t' + var ) 




