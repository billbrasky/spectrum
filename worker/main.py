import csv, re
import yaml



def writetable():
    pass

import psycopg2 as pg
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def checktype( s ):

    types = {
        'i': 'int',
        'vc': 'varchar',
        'c': 'char'
    }

    tipe = types.get( s, 'none' )

    if tipe == 'none':
        print( 'There is a type not being accounted for. {}'.format( s ))

    return tipe


def checkrule( s ):
    
    if s == 'f':
        return 'busy'

    rules = {
        'p': 'PRIMARY KEY'
    }

    rule = rules.get( s, 'none' )
    if rule == 'none':
        print( 'There is a rule not being accounted for. {}'.format( s ))

    return rule


def writetable( tn, td ):

    res = 'CREATE TABLE "coffee.{0}" (\n{1}\n);'.format( tn, ',\n'.join( td ))

    return res

with open( 'dataplan.yml', 'r' ) as f:

    data = yaml.load( f )

definitions = data['definitions']
tables = data['tables']
foreignkeys = []
res = ''
for tablename, table in tables.items():
    td = []
    for columnname, column in table.items():
        text = '\t{0} {1}'.format( columnname, column['type'] )
        
        isprimary = column.get( 'pk', False )
        isforeign = column.get( 'fk', False )

        if isforeign:
            foreignkeys.append( (tablename, columnname) )

        if isprimary:

            text += ' PRIMARY KEY'

        td.append( text )

    res += writetable( tablename, td ) + '\n\n'

res += '\n\n'
alter = 'ALTER TABLE "{table}" ADD FOREIGN KEY ("{fkey}") REFERENCES "{ftable}" ("{key}");\n'
for item in foreignkeys:

    table, fkey = item

    ftable = fkey[:-3]
    key = 'id'

    res += alter.format( table = table, fkey = fkey, key = key, ftable = ftable )


res = 'DROP SCHEMA IF EXISTS coffee;\nCREATE SCHEMA coffee;\n\n' + res
print( res )

conn = pg.connect( dbname='postgres', user='escdata' )
conn.set_isolation_level( ISOLATION_LEVEL_AUTOCOMMIT )

cur = conn.cursor()
cur.execute( 'DROP DATABASE IF EXISTS coffee;' )
cur.execute( 'CREATE DATABASE coffee;' )
#conn.commit()
cur.execute( res )

cur.close()
conn.close()
