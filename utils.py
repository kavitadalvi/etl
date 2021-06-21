
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

def merge_dicts(target, source):
    '''
    Appends source dictionary into target
    '''
    if not isinstance(target, dict) or not isinstance(source, dict):
        return source
    for k in source:
        if k in target:
            target[k] = merge_dicts(target[k], source[k])
        else:
            target[k] = source[k]
    return target

def get_data_by_group(group_fields, leaf_fields):
    '''
    Generate dictionary where
    key: group_fields (nested if multiple values in group_fields list)
    value: leaf_fields
    '''
    data = leaf_fields
    for field in reversed(group_fields):
        data = {field: data}
    return data
