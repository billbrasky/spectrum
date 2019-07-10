import csv, re
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

    res = 'CREATE TABLE "{0}" (\n{1}\n);'.format( tn, ',\n'.join( td ))

    return res


def writeforeignkey( items ):

    template = '\n\nALTER TABLE "{0}" ADD FOREIGN KEY ("{1}") REFERENCES "{2}" ("id");'
    res = ''
    for item in items:
        tablename, fk = item

        res += template.format( tablename, fk, fk[:-3] )

    return res


#main
with open( 'dataplan.txt', 'r' ) as f:

    tablename = None
    tabledata = []
    output = ''
    foreignkeys = []
    for line in f:
        
        line = line.strip()

        if line[0] == '#':

            if tablename is None:
                tablename = line[1:]

            else:
                output += writetable( tablename, tabledata ) + '\n\n'
                tabledata = []

            continue


        thing = line.split( ' ' )

        vardef = '\t"{}" '.format( thing[0] )

        # foreign key
        if 'f' in thing:
            foreignkeys.append( (tablename, thing[0]) )
            thing.remove( 'f' )

        if len( thing ) == 3:
            vardef += checktype( thing[1] ) + ' ' + checkrule( thing[2] )

        elif len( thing ) == 2:
            vardef += checktype( thing[1] )

        else:
            vardef += 'varchar'


        tabledata.append( vardef )

    output += writeforeignkey( foreignkeys )
#    print( output )

conn = pg.connect( 'dbname=coffee user=' )