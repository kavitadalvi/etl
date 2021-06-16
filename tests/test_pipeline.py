import pytest
import unittest.mock as mock
from unittest.mock import mock_open
import yaml
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
target_db:
  host: localhost
  port: 27017
  name: sales
  collection: sales_summary
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
SOURCE_DATA_VALID = '''Region,Country,Item Type,Sales Channel,Order Priority,Order Date,Order ID,Ship Date,Units Sold,Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit
Middle East and North Africa,Libya,Cosmetics,Offline,M,10/18/2014,686800706,10/31/2014,8446,437.2,263.33,3692591.2,2224085.18,1468506.02
North America,Canada,Vegetables,Online,M,11/7/2011,185941302,12/8/2011,3018,154.06,90.93,464953.08,274426.74,190526.34
Middle East and North Africa,Morocco,Baby Food,Offline,C,10/31/2016,246222341,12/9/2016,1517,255.28,159.42,387259.76,241840.14,145419.62'''

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

# source data with only headers. no actual data rows
SOURCE_NO_DATA_1 = '''Region,Country,Item Type,Sales Channel,Order Priority,Order Date,Order ID,Ship Date,Units Sold,Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit'''

# no data in source
SOURCE_NO_DATA_2 = ''

# source data contains fewer fields than expected
SOURCE_DATA_MISSING_FIELD = '''Region,Country,Item Type,Sales Channel,Order Priority,Order Date,Order ID,Ship Date,Units Sold,Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit
Middle East and North Africa,Libya,Cosmetics,Offline,M,10/18/2014,686800706,10/31/2014,8446,437.2,263.33,3692591.2,2224085.18,1468506.02
North America,Canada,Vegetables,Online,M,11/7/2011,185941302,12/8/2011,3018,154.06,90.93,464953.08,274426.74,190526.34
Middle East and North Africa,Libya,Baby Food,C,10/31/2016,246222341,12/9/2016,1517,255.28,159.42,387259.76,241840.14,145419.62'''

SOURCE_DATA_MISSING_DATA = '''Region,Country,Item Type,Sales Channel,Order Priority,Order Date,Order ID,Ship Date,Units Sold,Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit
Middle East and North Africa,Libya,,Offline,M,10/18/2014,686800706,10/31/2014,8446,437.2,263.33,3692591.2,2224085.18,1468506.02
North America,Canada,Vegetables,Online,M,11/7/2011,185941302,12/8/2011,3018,154.06,90.93,464953.08,274426.74,190526.34
Middle East and North Africa,Libya,Baby Food,Offline,C,10/31/2016,246222341,12/9/2016,1517,255.28,159.42,387259.76,241840.14,145419.62'''

SOURCE_DATA_INVALID_REGION = '''Region,Country,Item Type,Sales Channel,Order Priority,Order Date,Order ID,Ship Date,Units Sold,Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit
Middle East and North Africa,Libya,Cosmetics,Offline,M,10/18/2014,686800706,10/31/2014,8446,437.2,263.33,3692591.2,2224085.18,1468506.02
North America,Canada,Vegetables,Online,M,11/7/2011,185941302,12/8/2011,3018,154.06,90.93,464953.08,274426.74,190526.34
MiddleEast and North Africa,Libya,Baby Food,Offline,C,10/31/2016,246222341,12/9/2016,1517,255.28,159.42,387259.76,241840.14,145419.62'''

SOURCE_DATA_INVALID_SALES_CHANNEL = '''Region,Country,Item Type,Sales Channel,Order Priority,Order Date,Order ID,Ship Date,Units Sold,Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit
Middle East and North Africa,Libya,Cosmetics,On line,M,10/18/2014,686800706,10/31/2014,8446,437.2,263.33,3692591.2,2224085.18,1468506.02
North America,Canada,Vegetables,Online,M,11/7/2011,185941302,12/8/2011,3018,154.06,90.93,464953.08,274426.74,190526.34
Middle East and North Africa,Libya,Baby Food,Offline,C,10/31/2016,246222341,12/9/2016,1517,255.28,159.42,387259.76,241840.14,145419.62'''

SOURCE_DATA_INVALID_PRIORITY = '''Region,Country,Item Type,Sales Channel,Order Priority,Order Date,Order ID,Ship Date,Units Sold,Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit
Middle East and North Africa,Libya,Cosmetics,Offline,M,10/18/2014,686800706,10/31/2014,8446,437.2,263.33,3692591.2,2224085.18,1468506.02
North America,Canada,Vegetables,Online,A,11/7/2011,185941302,12/8/2011,3018,154.06,90.93,464953.08,274426.74,190526.34
Middle East and North Africa,Libya,Baby Food,Offline,B,10/31/2016,246222341,12/9/2016,1517,255.28,159.42,387259.76,241840.14,145419.62'''

SOURCE_DATA_INVALID_ORDER_DATE= '''Region,Country,Item Type,Sales Channel,Order Priority,Order Date,Order ID,Ship Date,Units Sold,Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit
Middle East and North Africa,Libya,Cosmetics,Offline,M,18/10/2014,686800706,10/31/2014,8446,437.2,263.33,3692591.2,2224085.18,1468506.02
North America,Canada,Vegetables,Online,M,11/7/2011,185941302,12/8/2011,3018,154.06,90.93,464953.08,274426.74,190526.34
Middle East and North Africa,Libya,Baby Food,Offline,C,10/31/2016,246222341,12/9/2016,1517,255.28,159.42,387259.76,241840.14,145419.62'''

SOURCE_DATA_INVALID_SHIP_DATE = '''Region,Country,Item Type,Sales Channel,Order Priority,Order Date,Order ID,Ship Date,Units Sold,Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit
Middle East and North Africa,Libya,Cosmetics,Offline,M,10/18/2014,686800706,31/10/2014,8446,437.2,263.33,3692591.2,2224085.18,1468506.02
North America,Canada,Vegetables,Online,M,11/7/2011,185941302,12/8/2011,3018,154.06,90.93,464953.08,274426.74,190526.34
Middle East and North Africa,Libya,Baby Food,Offline,C,10/31/2016,246222341,19/2/2016,1517,255.28,159.42,387259.76,241840.14,145419.62'''

SOURCE_DATA_INVALID_UNIT_PRICE = '''Region,Country,Item Type,Sales Channel,Order Priority,Order Date,Order ID,Ship Date,Units Sold,Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit
Middle East and North Africa,Libya,Cosmetics,Offline,M,10/18/2014,686800706,10/31/2014,8446,'one',263.33,3692591.2,2224085.18,1468506.02
North America,Canada,Vegetables,Online,M,11/7/2011,185941302,12/8/2011,3018,154.06,90.93,464953.08,274426.74,190526.34
Middle East and North Africa,Libya,Baby Food,Offline,C,10/31/2016,246222341,12/9/2016,1517,'ten',159.42,387259.76,241840.14,145419.62'''

SOURCE_DATA_INVALID_UNITS_SOLD = '''Region,Country,Item Type,Sales Channel,Order Priority,Order Date,Order ID,Ship Date,Units Sold,Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit
Middle East and North Africa,Libya,Cosmetics,Offline,M,10/18/2014,686800706,10/31/2014,8446.5,437.2,263.33,3692591.2,2224085.18,1468506.02
North America,Canada,Vegetables,Online,M,11/7/2011,185941302,12/8/2011,3018.0,154.06,90.93,464953.08,274426.74,190526.34
Middle East and North Africa,Libya,Baby Food,Offline,C,10/31/2016,246222341,12/9/2016,1517,255.28,159.42,387259.76,241840.14,145419.62'''

@mock.patch('builtins.open', new_callable=mock_open, create=True, read_data=TRANSFORM_CONFIG_VALID_DATA)
def setup_valid_pipeline(mock_open):
    p = pipeline.Pipeline(TRANSFORM_NAME, SOURCE_FILENAME)
    p.get_config()
    p.get_preprocess_tasks(p.config)
    return p

def test_pipeline_initialisation():
    p = pipeline.Pipeline(TRANSFORM_NAME, SOURCE_FILENAME)
    assert len(p.__dict__.keys()) == 7
    assert p.transform_name == TRANSFORM_NAME
    assert p.transform_config_file == "transforms\\" + TRANSFORM_NAME + ".yaml"
    assert p.source_filename == SOURCE_FILENAME
    assert p.source_file_format == "csv"
    assert p.config == {}
    assert p.preprocess_tasks == []
    assert p.source_fields == []

@mock.patch('builtins.open', new_callable=mock_open, create=True, read_data=TRANSFORM_CONFIG_SOURCE_FIELDS_NOT_SPECIFIED)
def test_pipeline_setup_with_no_source_fields_in_config(mock_open):
    p = pipeline.Pipeline(TRANSFORM_NAME, SOURCE_FILENAME)
    with pytest.raises(SystemExit):
        p.get_config()

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

def test_get_preprocess_tasks_invalid():
    all_preprocess_tasks = [
    'data_completeness_check',
    'data_validations_check',
    'float_field_check',
    'number_field_check'
    ]
    p = pipeline.Pipeline(TRANSFORM_NAME, SOURCE_FILENAME)
    p.get_preprocess_tasks(yaml.safe_load(TRANSFORM_CONFIG_VALID))
    assert len(all_preprocess_tasks) == len(p.preprocess_tasks) - 1
    
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
    p.get_config()
    e = pipeline.Extract(p)
    assert len(e.__dict__.keys()) == 8

@mock.patch('builtins.open', new_callable=mock_open, create=True, read_data=TRANSFORM_CONFIG_VALID_DATA)
def test_transform_initialisation(mock_open):
    p = pipeline.Pipeline(TRANSFORM_NAME, SOURCE_FILENAME)
    p.get_config()
    p.get_preprocess_tasks(p.config)
    e = pipeline.Extract(p)
    t = pipeline.Transform(p,e)
    assert len(t.__dict__.keys()) == 12

@mock.patch('builtins.open', new_callable=mock_open, create=True, read_data=SOURCE_DATA_VALID)
def test_run_data_completeness_check_with_valid_data(mock_open):
    p = setup_valid_pipeline()
    e = pipeline.Extract(p)
    e.extract()
    t = pipeline.Transform(p,e)
    t.transform()
    assert len(t.transformed_data) == 3
    assert len(t.rejected_data) == 0

@mock.patch('builtins.open', new_callable=mock_open, create=True, read_data=SOURCE_DATA_MISSING_FIELD)
def test_run_data_completeness_check_with_missing_field(mock_open):
    p = setup_valid_pipeline()
    e = pipeline.Extract(p)
    e.extract()
    t = pipeline.Transform(p,e)
    t.transform()
    assert len(t.transformed_data) == 2
    assert len(t.rejected_data) == 1

@mock.patch('builtins.open', new_callable=mock_open, create=True, read_data=SOURCE_DATA_MISSING_DATA)
def test_run_data_completeness_check_with_missing_data(mock_open):
    p = setup_valid_pipeline()
    e = pipeline.Extract(p)
    e.extract()
    t = pipeline.Transform(p,e)
    t.transform()
    assert len(t.transformed_data) == 2
    assert len(t.rejected_data) == 1

@mock.patch('builtins.open', new_callable=mock_open, create=True, read_data=SOURCE_DATA_INVALID_REGION)
def test_run_data_completeness_check_with_invalid_region(mock_open):
    p = setup_valid_pipeline()
    e = pipeline.Extract(p)
    e.extract()
    t = pipeline.Transform(p,e)
    t.transform()
    assert len(t.transformed_data) == 2
    assert len(t.rejected_data) == 1

@mock.patch('builtins.open', new_callable=mock_open, create=True, read_data=SOURCE_DATA_INVALID_SALES_CHANNEL)
def test_run_data_completeness_check_with_invalid_sales_channel(mock_open):
    p = setup_valid_pipeline()
    e = pipeline.Extract(p)
    e.extract()
    t = pipeline.Transform(p,e)
    t.transform()
    assert len(t.transformed_data) == 2
    assert len(t.rejected_data) == 1

@mock.patch('builtins.open', new_callable=mock_open, create=True, read_data=SOURCE_DATA_INVALID_PRIORITY)
def test_run_data_completeness_check_with_invalid_priority(mock_open):
    p = setup_valid_pipeline()
    e = pipeline.Extract(p)
    e.extract()
    t = pipeline.Transform(p,e)
    t.transform()
    assert len(t.transformed_data) == 1
    assert len(t.rejected_data) == 2


@mock.patch('builtins.open', new_callable=mock_open, create=True, read_data=SOURCE_DATA_INVALID_ORDER_DATE)
def test_run_data_completeness_check_with_invalid_order_date(mock_open):
    p = setup_valid_pipeline()
    e = pipeline.Extract(p)
    e.extract()
    t = pipeline.Transform(p,e)
    t.transform()
    assert len(t.transformed_data) == 2
    assert len(t.rejected_data) == 1


@mock.patch('builtins.open', new_callable=mock_open, create=True, read_data=SOURCE_DATA_INVALID_SHIP_DATE)
def test_run_data_completeness_check_with_invalid_ship_date(mock_open):
    p = setup_valid_pipeline()
    e = pipeline.Extract(p)
    e.extract()
    t = pipeline.Transform(p,e)
    t.transform()
    assert len(t.transformed_data) == 1
    assert len(t.rejected_data) == 2

@mock.patch('builtins.open', new_callable=mock_open, create=True, read_data=SOURCE_DATA_INVALID_UNIT_PRICE)
def test_run_data_completeness_check_with_invalid_unit_price(mock_open):
    p = setup_valid_pipeline()
    e = pipeline.Extract(p)
    e.extract()
    t = pipeline.Transform(p,e)
    t.transform()
    assert len(t.transformed_data) == 1
    assert len(t.rejected_data) == 2

@mock.patch('builtins.open', new_callable=mock_open, create=True, read_data=SOURCE_DATA_INVALID_UNITS_SOLD)
def test_run_data_completeness_check_with_invalid_units_sold(mock_open):
    p = setup_valid_pipeline()
    e = pipeline.Extract(p)
    e.extract()
    t = pipeline.Transform(p,e)
    t.transform()
    assert len(t.transformed_data) == 1
    assert len(t.rejected_data) == 2

def test_calc_revenue():
    result = pipeline.calc_revenue(EXTRACTED_SOURCE_DATA_VALID, 'Middle East and North Africa', 'Libya')
    expected_result = 4079850.96
    assert result == expected_result

def test_calc_profit():
    result = pipeline.calc_profit(EXTRACTED_SOURCE_DATA_VALID, 'Middle East and North Africa', 'Libya')
    expected_result = 1613925.64
    assert result == expected_result

@mock.patch('builtins.open', new_callable=mock_open, create=True, read_data=SOURCE_DATA_VALID)
def test_gen_sales_summary(mock_open):
    p = setup_valid_pipeline()
    e = pipeline.Extract(p)
    e.extract()
    t = pipeline.Transform(p,e)
    t.transform()
    result = t.gen_sales_summary()

    assert len(result) == 2
    assert len(result['Middle East and North Africa']['Offline']) == 2
    assert len(result['North America']['Online']) == 1

    for i in range(0,len(result['Middle East and North Africa']['Offline'])):
        assert len((result['Middle East and North Africa']['Offline'][i]).keys()) == 12
        assert 'Country' in (result['Middle East and North Africa']['Offline'][i]).keys()
        assert 'ItemType' in (result['Middle East and North Africa']['Offline'][i]).keys()
        assert 'OrderPriority' in (result['Middle East and North Africa']['Offline'][i]).keys()
        assert 'OrderDate' in (result['Middle East and North Africa']['Offline'][i]).keys()
        assert 'OrderId' in (result['Middle East and North Africa']['Offline'][i]).keys()
        assert 'ShipDate' in (result['Middle East and North Africa']['Offline'][i]).keys()
        assert 'UnitsSold' in (result['Middle East and North Africa']['Offline'][i]).keys()
        assert 'UnitPrice' in (result['Middle East and North Africa']['Offline'][i]).keys()
        assert 'UnitCost' in (result['Middle East and North Africa']['Offline'][i]).keys()
        assert 'TotalRevenue' in (result['Middle East and North Africa']['Offline'][i]).keys()
        assert 'TotalCost' in (result['Middle East and North Africa']['Offline'][i]).keys()
        assert 'TotalProfit' in (result['Middle East and North Africa']['Offline'][i]).keys()

    for i in range(0,len(result['North America']['Online'])):
        assert len((result['North America']['Online'][i]).keys()) == 12
        assert 'Country' in (result['North America']['Online'][i]).keys()
        assert 'ItemType' in (result['North America']['Online'][i]).keys()
        assert 'OrderPriority' in (result['North America']['Online'][i]).keys()
        assert 'OrderDate' in (result['North America']['Online'][i]).keys()
        assert 'OrderId' in (result['North America']['Online'][i]).keys()
        assert 'ShipDate' in (result['North America']['Online'][i]).keys()
        assert 'UnitsSold' in (result['North America']['Online'][i]).keys()
        assert 'UnitPrice' in (result['North America']['Online'][i]).keys()
        assert 'UnitCost' in (result['North America']['Online'][i]).keys()
        assert 'TotalRevenue' in (result['North America']['Online'][i]).keys()
        assert 'TotalCost' in (result['North America']['Online'][i]).keys()
        assert 'TotalProfit' in (result['North America']['Online'][i]).keys()

@mock.patch('builtins.open', new_callable=mock_open, create=True, read_data=SOURCE_DATA_VALID)
def test_gen_sales_aggregate(mock_open):
    p = setup_valid_pipeline()
    e = pipeline.Extract(p)
    e.extract()
    t = pipeline.Transform(p,e)
    t.transform()
    result = t.gen_sales_aggregate()

    assert len(result) == 2
    assert len(result['Middle East and North Africa']) == 2
    assert len(result['North America']) == 1

    for i in range(0,len(result['Middle East and North Africa'])):
        assert len((result['Middle East and North Africa'][i]).keys()) == 3
        assert 'Country' in (result['Middle East and North Africa'][i]).keys()
        assert 'CountryRevenue' in (result['Middle East and North Africa'][i]).keys()
        assert 'CountryProfit' in (result['Middle East and North Africa'][i]).keys()

    for i in range(0,len(result['North America'])):
        assert len((result['North America'][i]).keys()) == 3
        assert 'Country' in (result['North America'][i]).keys()
        assert 'CountryRevenue' in (result['North America'][i]).keys()
        assert 'CountryProfit' in (result['North America'][i]).keys()
