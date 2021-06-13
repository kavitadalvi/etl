import utils

def test_date_format_with_correct_date():
    date_value = "11/14/2021"
    date_format = "%m/%d/%Y"
    ret_val = utils.is_valid_date(date_value, date_format)
    assert ret_val == True

def test_date_format_with_incorrect_date_1():
    date_value = "14/11/2021"
    date_format = "%m/%d/%Y"
    ret_val = utils.is_valid_date(date_value, date_format)
    assert ret_val == False

def test_date_format_with_incorrect_date_1():
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


