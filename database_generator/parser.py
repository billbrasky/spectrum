import csv, re
<<<<<<< HEAD
import yaml



def writetable():
    pass

=======
import psycopg2 as pg
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
>>>>>>> a0047dfd2a439715c3b53933ce91d0d6796278ef

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

<<<<<<< HEAD
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
=======

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
>>>>>>> a0047dfd2a439715c3b53933ce91d0d6796278ef
        
        isprimary = column.get( 'pk', False )
        isforeign = column.get( 'fk', False )

        if isforeign:
            foreignkeys.append( (tablename, columnname) )

        if isprimary:

            text += ' PRIMARY KEY'

        td.append( text )

<<<<<<< HEAD
    res += writetable( tablename, td ) + '\n\n'
=======
        # foreign key
        if 'f' in thing:
            foreignkeys.append( (tablename, thing[0]) )
            thing.remove( 'f' )

        if len( thing ) == 3:
            vardef += checktype( thing[1] ) + ' ' + checkrule( thing[2] )
>>>>>>> a0047dfd2a439715c3b53933ce91d0d6796278ef

res += '\n\n'
alter = 'ALTER TABLE "{table}" ADD FOREIGN KEY ("{fkey}") REFERENCES "{ftable}" ("{key}");\n'
for item in foreignkeys:

    table, fkey = item

    ftable = fkey[:-3]
    key = 'id'

    res += alter.format( table = table, fkey = fkey, key = key, ftable = ftable )

<<<<<<< HEAD
print( res )
=======
    output += writeforeignkey( foreignkeys )
#    print( output )
>>>>>>> a0047dfd2a439715c3b53933ce91d0d6796278ef

conn = pg.connect( 'dbname=coffee user=' )