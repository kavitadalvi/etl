import utils

def test_date_format_with_correct_date():
    date_value = "11/14/2021"
    date_format = "%m/%d/%Y"
    ret_val = utils.is_valid_date(date_value, date_format)
    assert ret_val == True

def test_date_format_with_incorrect_date():
    date_value = "14/11/2021"
    date_format = "%m/%d/%Y"
    ret_val = utils.is_valid_date(date_value, date_format)
    assert ret_val == False
    
def test_number_with_text_input():
    ret_val = utils.is_numeric("Asia")
    assert ret_val == False

def test_number_with_float_input_1():
    ret_val = utils.is_numeric("19.10")
    assert ret_val == True

def test_number_with_float_input_2():
    ret_val = utils.is_numeric(19.10)
    assert ret_val == True

def test_number_with_integer_input():
    ret_val = utils.is_numeric("19")
    assert ret_val == True

def test_merge_dicts():
    data_one = {'Asia': {'Online': [{'Country':'Japan', 'ItemType': 'Cosmetics'}]}}
    data_two = {'Asia': {'Offline': [{'Country':'Japan', 'ItemType': 'Cereal'}, {'Country':'Japan', 'ItemType': 'Baby Food'}]}}
    expected_data = {'Asia': {'Online': [{'Country':'Japan', 'ItemType': 'Cosmetics'}], 'Offline': [{'Country':'Japan', 'ItemType': 'Cereal'},{'Country':'Japan', 'ItemType': 'Baby Food'}]}}
    output = {}
    output = utils.merge_dicts(data_one,data_two)
    assert data_one == expected_data
    assert output == expected_data

def test_get_data_by_group():
    high_level_data = ['Asia', 'Offline']
    detail_data = [{'Country':'Japan', 'ItemType':'Baby Food', 'Units Sold': 10}]
    output = utils.get_data_by_group(high_level_data, detail_data)
    assert output == {'Asia': {'Offline': [{'Country':'Japan', 'ItemType':'Baby Food', 'Units Sold': 10}]}}
