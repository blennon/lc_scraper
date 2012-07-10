import sys
from pymongo import Connection
from pymongo.errors import ConnectionFailure
import datetime

def get_db(db_name):
    '''Connect to MongoDB'''
    try:
        c = Connection(host='localhost', port=27017)
        print 'Connected to DB successfully'
    except ConnectionFailure, e:
        sys.stderr.write('Could not connect to MongoDB: %s' % e)
        sys.exit(1)
        
    # Get a database handle to a database named 'mydb'
    dbh = c[db_name]
    
    assert dbh.connection == c
    print 'Successfully set up a database handle'
    
    return dbh
    

if __name__ == '__main__':
    
    dbh = get_db('lc_db')
    
    dbh.notes.create_index('noteID', unique=True)
    dbh.loans.create_index('loanID', unique=True)
