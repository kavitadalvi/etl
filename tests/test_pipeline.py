from io import StringIO
from os import pipe
import pytest
import unittest.mock as mock
from unittest.mock import mock_open
import yaml
from yaml.scanner import Scanner, ScannerError
import pipeline

TRANSFORM_NAME = 'sales-summary'
SOME_INVALID_TRANSFORM_NAME = 'some invalid transform name'
SOURCE_FILENAME = "sales-records.csv"
TRANSFORM_CONFIG_VALID = '''
source_fields: [Region,Country,Item Type,Sales Channel,Order Priority,Order Date,Order ID,Ship Date,Units Sold,Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit]
data_validations_check:
  Region: [Asia,Australia and Oceania,Central America and the Caribbean,Europe,Middle East and North Africa,North America,Sub-Saharan Africa]
  Sales Channel: [Online,Offline]
  Order Priority: [H,M,L,C]
date_field_check: [Order Date,Ship Date]
float_field_check: [Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit]
number_field_check: [Units Sold]
output_file: output/sales-transformed.json
expand_output:
  Order Priority:
    H: High
    M: Medium
    L: Low
    C: Critical
'''
TRANSFORM_CONFIG_INVALID = '''
'''
TRANSFORM_CONFIG_VALID_DATA = '''
source_fields: [Region,Country,Item Type,Sales Channel,Order Priority,Order Date,Order ID,Ship Date,Units Sold,Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit]
data_validations_check:
  Region: [Asia,Australia and Oceania,Central America and the Caribbean,Europe,Middle East and North Africa,North America,Sub-Saharan Africa]
  Sales Channel: [Online,Offline]
  Order Priority: [H,M,L,C]
date_field_check: [Order Date,Ship Date]
float_field_check: [Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit]
number_field_check: [Units Sold]
output_file: output/sales-transformed.json
expand_output:
  Order Priority:
    H: High
    M: Medium
    L: Low
    C: Critical
'''
TRANSFORM_CONFIG_SOURCE_FIELDS_NOT_SPECIFIED = '''
data_validations_check:
  Region: [Asia,Australia and Oceania,Central America and the Caribbean,Europe,Middle East and North Africa,North America,Sub-Saharan Africa]
  Sales Channel: [Online,Offline]
  Order Priority: [H,M,L,C]
date_field_check: [Order Date,Ship Date]
float_field_check: [Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit]
number_field_check: [Units Sold]
output_file: output/sales-transformed.json
expand_output:
  Order Priority:
    H: High
    M: Medium
    L: Low
    C: Critical
'''
TRANSFORM_CONFIG_FILE_INVALID_FORMAT = '''
data_validations_check:
  Region: [Asia,Australia and Oceania,Central America and the Caribbean,Europe,Middle East and North Africa,North America,Sub-Saharan Africa]
  Sales Channel: [Online,Offline]
  Order Priority: [H,M,L,C]
date_field_check: [Order Date,Ship Date]
float_field_check: [Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit]
number_field_check: [Units Sold]
output_file: output/sales-transformed.json
expand_output
  Order Priority:
    H: High
    M: Medium
    L: Low
    C: Critical
'''
SOURCE_FIELDS_VALID = ['Region', 'Country', 'Item Type', 'Sales Channel', 'Order Priority', 'Order Date', 'Order ID', 'Ship Date', 'Units Sold', 'Unit Price', 'Unit Cost', 'Total Revenue', 'Total Cost', 'Total Profit']
SOURCE_DATA_VALID = '''Region,Country,Item Type,Sales Channel,Order Priority,Order Date,Order ID,Ship Date,Units Sold,Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit
Middle East and North Africa,Libya,Cosmetics,Offline,M,10/18/2014,686800706,10/31/2014,8446,437.2,263.33,3692591.2,2224085.18,1468506.02
North America,Canada,Vegetables,Online,M,11/7/2011,185941302,12/8/2011,3018,154.06,90.93,464953.08,274426.74,190526.34
Middle East and North Africa,Libya,Baby Food,Offline,C,10/31/2016,246222341,12/9/2016,1517,255.28,159.42,387259.76,241840.14,145419.62
Asia,Japan,Cereal,Offline,C,4/10/2010,161442649,5/12/2010,3322,205.7,117.11,683335.4,389039.42,294295.98'''
EXTRACTED_SOURCE_DATA_VALID = \
{
    1:{
        'Region': 'Middle East and North Africa',
        'Country': 'Libya',
        'Item Type': 'Cosmetics',
        'Sales Channel': 'Offline',
        'Order Priority': 'M',
        'Order Date': '10/18/2014',
        'Order ID': '686800706',
        'Ship Date': '10/31/2014',
        'Units Sold': '8446',
        'Unit Price': '437.2',
        'Unit Cost': '263.33',
        'Total Revenue': '3692591.2',
        'Total Cost': '2224085.18',
        'Total Profit': '1468506.02'
    },
    2:{
        'Region': 'North America',
        'Country': 'Canada',
        'Item Type': 'Vegetables',
        'Sales Channel': 'Online',
        'Order Priority': 'M',
        'Order Date': '11/7/2011',
        'Order ID': '185941302',
        'Ship Date': '12/8/2011',
        'Units Sold': '3018',
        'Unit Price': '154.06',
        'Unit Cost': '90.93',
        'Total Revenue': '464953.08',
        'Total Cost': '274426.74',
        'Total Profit': '190526.34'
    },
    3:{
        'Region': 'Middle East and North Africa',
        'Country': 'Libya',
        'Item Type': 'Baby Food',
        'Sales Channel': 'Offline',
        'Order Priority': 'C',
        'Order Date': '10/31/2016',
        'Order ID': '246222341',
        'Ship Date': '12/9/2016',
        'Units Sold': '1517',
        'Unit Price': '255.28',
        'Unit Cost': '159.42',
        'Total Revenue': '387259.76',
        'Total Cost': '241840.14',
        'Total Profit': '145419.62'
    },
    4:{
        'Region': 'Asia',
        'Country': 'Japan',
        'Item Type': 'Cereal',
        'Sales Channel': 'Offline',
        'Order Priority': 'C',
        'Order Date': '4/10/2010',
        'Order ID': '161442649',
        'Ship Date': '5/12/2010',
        'Units Sold': '3322',
        'Unit Price': '205.7',
        'Unit Cost': '117.11',
        'Total Revenue': '683335.4',
        'Total Cost': '389039.42',
        'Total Profit': '294295.98'
    }
}

@pytest.fixture
def my_valid_pipeline():
    my_class = pipeline.Pipeline(TRANSFORM_NAME, SOURCE_FILENAME)
    print(my_class.source_data)
    return my_class


# source data with only headers. no actual data rows
SOURCE_NO_DATA_1 = '''Region,Country,Item Type,Sales Channel,Order Priority,Order Date,Order ID,Ship Date,Units Sold,Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit'''
# no data in source
SOURCE_NO_DATA_2 = ''
# source data contains fewer fields than expected
SOURCE_DATA_INVALID = '''Region,Country,Item Type,Sales Channel,Order Priority,Order Date,Order ID,Ship Date,Units Sold,Unit Price,Unit Cost,Total Revenue,Total Profit
Middle East and North Africa,Libya,Cosmetics,Offline,M,10/18/2014,686800706,10/31/2014,8446,437.2,263.33,3692591.2,1468506.02
North America,Canada,Vegetables,Online,M,11/7/2011,185941302,12/8/2011,3018,154.06,90.93,464953.08,190526.34
Middle East and North Africa,Libya,Baby Food,Offline,C,10/31/2016,246222341,12/9/2016,1517,255.28,159.42,387259.76,145419.62
Asia,Japan,Cereal,Offline,C,4/10/2010,161442649,5/12/2010,3322,205.7,117.11,683335.4,294295.98'''

# Done
def test_pipeline_initialisation():
    p = pipeline.Pipeline(TRANSFORM_NAME, SOURCE_FILENAME)
    assert len(p.__dict__.keys()) == 6
    assert p.transform_name == TRANSFORM_NAME
    assert p.transform_config_file == "transforms\\" + TRANSFORM_NAME + ".yaml"
    assert p.source_filename == SOURCE_FILENAME
    assert p.source_file_format == "csv"
    assert p.config == {}
    assert p.preprocess_tasks == []

def test_get_config_with_invalid_transform_name_passed():
    p = pipeline.Pipeline(SOME_INVALID_TRANSFORM_NAME, SOURCE_FILENAME)
    with pytest.raises(SystemExit):
        p.get_config()

def test_get_preprocess_tasks():
    all_preprocess_tasks = [
    'data_completeness_check',
    'data_validations_check',
    'date_field_check',
    'float_field_check',
    'number_field_check'
    ]
    p = pipeline.Pipeline(TRANSFORM_NAME, SOURCE_FILENAME)
    p.get_preprocess_tasks(yaml.safe_load(TRANSFORM_CONFIG_VALID))
    assert p.preprocess_tasks == all_preprocess_tasks
    
# Done
@mock.patch('builtins.open', new_callable=mock_open, create=True, read_data=TRANSFORM_CONFIG_VALID)
def test_get_config_with_valid_transform_passed(mock_open):
    expected_config = yaml.safe_load(TRANSFORM_CONFIG_VALID)
    p = pipeline.Pipeline(TRANSFORM_NAME, SOURCE_FILENAME)
    p.get_config()
    assert type(p.config) == dict
    assert p.config == expected_config

@mock.patch('builtins.open', new_callable=mock_open, create=True, read_data=TRANSFORM_CONFIG_VALID_DATA)
def test_extract_initialisation(mock_open):
    p = pipeline.Pipeline(TRANSFORM_NAME, SOURCE_FILENAME)
    with mock.patch.object(pipeline.Pipeline, 'get_config', return_value=yaml.safe_load(TRANSFORM_CONFIG_VALID_DATA)):
        e = pipeline.Extract(p)
    assert len(e.source_fields) > 1

@mock.patch('builtins.open', new_callable=mock_open, create=True, read_data=TRANSFORM_CONFIG_VALID_DATA)
def test_transform_initialisation(mock_open):
    p = pipeline.Pipeline(TRANSFORM_NAME, SOURCE_FILENAME)
    with mock.patch.object(pipeline.Extract, 'get_config', return_value=yaml.safe_load(TRANSFORM_CONFIG_VALID_DATA)):
        e = pipeline.Extract(p)
        e.extract()
    t = pipeline.Transform(p, e)
    assert t.source_fields == e.source_fields
    for row in t.source_data.keys():
        assert 'count_cols' in t.source_data[row].keys()
        assert 'is_valid' in t.source_data[row].keys()
        assert 'err_msg' in t.source_data[row].keys()
        assert t.source_data[row]['count_cols'] == len(e.source_data[row])
        assert t.source_data[row]['is_valid'] is True
        assert t.source_data[row]['err_msg'] == []
    assert t.date_fields == []
    assert t.float_fields == []
    assert t.numeric_fields == []
    assert t.preprocessed_data == {}
    assert t.output_file == ''
    assert t.lookup_to_expand_fields == {}
    assert t.transformed_data == {}
    assert t.rejected_data == {}

@mock.patch('builtins.open', new_callable=mock_open, create=True, read_data=SOURCE_DATA_VALID)
def test_validate_data(mock_open):
    p = pipeline.Pipeline(TRANSFORM_NAME, SOURCE_FILENAME)
    with mock.patch.object(pipeline.Extract, 'get_config', return_value=yaml.safe_load(TRANSFORM_CONFIG_VALID_DATA)):
        e = pipeline.Extract(p)
        e.extract()
    t = pipeline.Transform(p, e)
    print(t.source_data[1])
    print(e.config['data_validations']['Region'])
    t.validate_data('Region', e.config['data_validations']['Region'])
    assert t.source_data[1]['is_valid'] == False
