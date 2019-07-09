import csv, re




def writetable():
    pass


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

with open( 'dataplan.txt', 'r' ) as f:

    tablename = None
    tabledata = []
    output = ''

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

        if len( thing ) == 3:
            vardef += checktype( thing[1] ) + ' ' + checkrule( thing[2] )

        elif len( thing ) == 2:
            vardef += checktype( thing[1] )

        else:
            vardef += 'varchar'


        tabledata.append( vardef )


