
'''
Reusable utilities
'''
import datetime

def is_numeric( input_value ):
    '''
    Check if input_value is a valid floating point number
    Sample call: is_numeric( value )
    '''
    try:
        float( input_value )
        return True
    except ValueError:
        return False

def is_valid_date( date_value, date_format ):
    '''
    Check if input date_value is in required date_format
    Sample call: is_valid_date( "05/18/2021", "%m/%d/%Y" )
    '''
    try:
        datetime.datetime.strptime( date_value, date_format )
        return True
    except ValueError:
        return False
