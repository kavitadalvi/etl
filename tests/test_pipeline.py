from typing import final
import pytest
import unittest.mock as mock
from unittest.mock import mock_open
import yaml
import pipeline
from deepdiff import DeepDiff

TRANSFORM_NAME = 'sales-summary'
SOME_INVALID_TRANSFORM_NAME = 'some invalid transform name'
SOURCE_FILENAME = "sales-records.csv"
TRANSFORM_CONFIG_VALID = '''
source:
  file: input/sales-records.csv
  fields: [Region,Country,Item Type,Sales Channel,Order Priority,Order Date,Order ID,Ship Date,Units Sold,Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit]
checks:
  data:
    Region: [Asia,Australia and Oceania,Central America and the Caribbean,Europe,Middle East and North Africa,North America,Sub-Saharan Africa]
    Sales Channel: [Online,Offline]
    Order Priority: [H,M,L,C]
  date_field: [Order Date,Ship Date]
  float_field: [Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit]
  number_field: [Units Sold]
output:
  file: output/sales-transformed.json
  fields: [Region, Sales Channel, Country, Item Type, Order Priority, Order Date, Order ID, Ship Date, Units Sold, Unit Price, Unit Cost, Total Revenue, Total Cost, Total Profit]
  group_fields: [Region,Sales Channel]
  leaf_fields:
    Country: [Country, '']
    Item Type: [ItemType, '']
    Order Priority: [OrderPriority, '']
    Order Date: [OrderDate, '']
    Order ID: [OrderId, '']
    Ship Date: [ShipDate, '']
    Units Sold: [UnitsSold, sum]
    Unit Price: [UnitPrice, sum]
    Unit Cost: [UnitCost, sum]
    Total Revenue: [TotalRevenue, avg]
    Total Cost: [TotalCost, sum]
    Total Profit: [TotalProfit, sum]
  field_expansion:
    Order Priority:
      H: High
      M: Medium
      L: Low
      C: Critical
  db:
    host: localhost
    port: 27017
    name: sales
    collection: sales_summary
'''
TRANSFORM_CONFIG_SOURCE_FILENAME_NOT_SPECIFIED = '''
source:
  fields: [Region,Country,Item Type,Sales Channel,Order Priority,Order Date,Order ID,Ship Date,Units Sold,Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit]
checks:
  data:
    Region: [Asia,Australia and Oceania,Central America and the Caribbean,Europe,Middle East and North Africa,North America,Sub-Saharan Africa]
    Sales Channel: [Online,Offline]
    Order Priority: [H,M,L,C]
  date_field: [Order Date,Ship Date]
  float_field: [Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit]
  number_field: [Units Sold]
output:
  file: output/sales-transformed.json
  fields: [Region, Sales Channel, Country, Item Type, Order Priority, Order Date, Order ID, Ship Date, Units Sold, Unit Price, Unit Cost, Total Revenue, Total Cost, Total Profit]
  group_fields: [Region,Sales Channel]
  leaf_fields:
    Country: [Country, '']
    Item Type: [ItemType, '']
    Order Priority: [OrderPriority, '']
    Order Date: [OrderDate, '']
    Order ID: [OrderId, '']
    Ship Date: [ShipDate, '']
    Units Sold: [UnitsSold, sum]
    Unit Price: [UnitPrice, sum]
    Unit Cost: [UnitCost, sum]
    Total Revenue: [TotalRevenue, sum]
    Total Cost: [TotalCost, sum]
    Total Profit: [TotalProfit, sum]
  field_expansion:
    Order Priority:
      H: High
      M: Medium
      L: Low
      C: Critical
  db:
    host: localhost
    port: 27017
    name: sales
    collection: sales_summary
'''
TRANSFORM_CONFIG_SOURCE_FIELDS_NOT_SPECIFIED = '''
source:
  file: input/sales-records.csv
checks:
  data:
    Region: [Asia,Australia and Oceania,Central America and the Caribbean,Europe,Middle East and North Africa,North America,Sub-Saharan Africa]
    Sales Channel: [Online,Offline]
    Order Priority: [H,M,L,C]
  date_field: [Order Date,Ship Date]
  float_field: [Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit]
  number_field: [Units Sold]
output:
  file: output/sales-transformed.json
  fields: [Region, Sales Channel, Country, Item Type, Order Priority, Order Date, Order ID, Ship Date, Units Sold, Unit Price, Unit Cost, Total Revenue, Total Cost, Total Profit]
  group_fields: [Region,Sales Channel]
  leaf_fields:
    Country: [Country, '']
    Item Type: [ItemType, '']
    Order Priority: [OrderPriority, '']
    Order Date: [OrderDate, '']
    Order ID: [OrderId, '']
    Ship Date: [ShipDate, '']
    Units Sold: [UnitsSold, sum]
    Unit Price: [UnitPrice, sum]
    Unit Cost: [UnitCost, sum]
    Total Revenue: [TotalRevenue, sum]
    Total Cost: [TotalCost, sum]
    Total Profit: [TotalProfit, sum]
  field_expansion:
    Order Priority:
      H: High
      M: Medium
      L: Low
      C: Critical
  db:
    host: localhost
    port: 27017
    name: sales
    collection: sales_summary
'''
TRANSFORM_CONFIG_OUTPUT_FILENAME_NOT_SPECIFIED = '''
source:
  file: input/sales-records.csv
  fields: [Region,Country,Item Type,Sales Channel,Order Priority,Order Date,Order ID,Ship Date,Units Sold,Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit]
checks:
  data:
    Region: [Asia,Australia and Oceania,Central America and the Caribbean,Europe,Middle East and North Africa,North America,Sub-Saharan Africa]
    Sales Channel: [Online,Offline]
    Order Priority: [H,M,L,C]
  date_field: [Order Date,Ship Date]
  float_field: [Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit]
  number_field: [Units Sold]
output:
  fields: [Region, Sales Channel, Country, Item Type, Order Priority, Order Date, Order ID, Ship Date, Units Sold, Unit Price, Unit Cost, Total Revenue, Total Cost, Total Profit]
  group_fields: [Region,Sales Channel]
  leaf_fields:
    Country: [Country, '']
    Item Type: [ItemType, '']
    Order Priority: [OrderPriority, '']
    Order Date: [OrderDate, '']
    Order ID: [OrderId, '']
    Ship Date: [ShipDate, '']
    Units Sold: [UnitsSold, sum]
    Unit Price: [UnitPrice, sum]
    Unit Cost: [UnitCost, sum]
    Total Revenue: [TotalRevenue, sum]
    Total Cost: [TotalCost, sum]
    Total Profit: [TotalProfit, sum]
  field_expansion:
    Order Priority:
      H: High
      M: Medium
      L: Low
      C: Critical
  db:
    host: localhost
    port: 27017
    name: sales
    collection: sales_summary
'''
TRANSFORM_CONFIG_OUTPUT_FIELDS_NOT_SPECIFIED = '''
source:
  file: input/sales-records.csv
  fields: [Region,Country,Item Type,Sales Channel,Order Priority,Order Date,Order ID,Ship Date,Units Sold,Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit]
checks:
  data:
    Region: [Asia,Australia and Oceania,Central America and the Caribbean,Europe,Middle East and North Africa,North America,Sub-Saharan Africa]
    Sales Channel: [Online,Offline]
    Order Priority: [H,M,L,C]
  date_field: [Order Date,Ship Date]
  float_field: [Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit]
  number_field: [Units Sold]
output:
  file: output/sales-transformed.json
  group_fields: [Region,Sales Channel]
  leaf_fields:
    Country: [Country, '']
    Item Type: [ItemType, '']
    Order Priority: [OrderPriority, '']
    Order Date: [OrderDate, '']
    Order ID: [OrderId, '']
    Ship Date: [ShipDate, '']
    Units Sold: [UnitsSold, sum]
    Unit Price: [UnitPrice, sum]
    Unit Cost: [UnitCost, sum]
    Total Revenue: [TotalRevenue, sum]
    Total Cost: [TotalCost, sum]
    Total Profit: [TotalProfit, sum]
  field_expansion:
    Order Priority:
      H: High
      M: Medium
      L: Low
      C: Critical
  db:
    host: localhost
    port: 27017
    name: sales
    collection: sales_summary
'''
TRANSFORM_CONFIG_SOME_CHECK_NOT_SPECIFIED = '''
source:
  file: input/sales-records.csv
  fields: [Region,Country,Item Type,Sales Channel,Order Priority,Order Date,Order ID,Ship Date,Units Sold,Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit]
checks:
  data:
    Region: [Asia,Australia and Oceania,Central America and the Caribbean,Europe,Middle East and North Africa,North America,Sub-Saharan Africa]
    Sales Channel: [Online,Offline]
    Order Priority: [H,M,L,C]
  float_field: [Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit]
  number_field: [Units Sold]
output:
  file: output/sales-transformed.json
  fields: [Region, Sales Channel, Country, Item Type, Order Priority, Order Date, Order ID, Ship Date, Units Sold, Unit Price, Unit Cost, Total Revenue, Total Cost, Total Profit]
  group_fields: [Region,Sales Channel]
  leaf_fields:
    Country: [Country, '']
    Item Type: [ItemType, '']
    Order Priority: [OrderPriority, '']
    Order Date: [OrderDate, '']
    Order ID: [OrderId, '']
    Ship Date: [ShipDate, '']
    Units Sold: [UnitsSold, sum]
    Unit Price: [UnitPrice, sum]
    Unit Cost: [UnitCost, sum]
    Total Revenue: [TotalRevenue, sum]
    Total Cost: [TotalCost, sum]
    Total Profit: [TotalProfit, sum]
  field_expansion:
    Order Priority:
      H: High
      M: Medium
      L: Low
      C: Critical
  db:
    host: localhost
    port: 27017
    name: sales
    collection: sales_summary
'''
TRANSFORM_CONFIG_DB_NOT_SPECIFIED = '''
source:
  file: input/sales-records.csv
  fields: [Region,Country,Item Type,Sales Channel,Order Priority,Order Date,Order ID,Ship Date,Units Sold,Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit]
checks:
  data:
    Region: [Asia,Australia and Oceania,Central America and the Caribbean,Europe,Middle East and North Africa,North America,Sub-Saharan Africa]
    Sales Channel: [Online,Offline]
    Order Priority: [H,M,L,C]
  date_field: [Order Date,Ship Date]
  float_field: [Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit]
  number_field: [Units Sold]
output:
  file: output/sales-transformed.json
  fields: [Region, Sales Channel, Country, Item Type, Order Priority, Order Date, Order ID, Ship Date, Units Sold, Unit Price, Unit Cost, Total Revenue, Total Cost, Total Profit]
  group_fields: [Region,Sales Channel]
  leaf_fields:
    Country: [Country, '']
    Item Type: [ItemType, '']
    Order Priority: [OrderPriority, '']
    Order Date: [OrderDate, '']
    Order ID: [OrderId, '']
    Ship Date: [ShipDate, '']
    Units Sold: [UnitsSold, sum]
    Unit Price: [UnitPrice, sum]
    Unit Cost: [UnitCost, sum]
    Total Revenue: [TotalRevenue, sum]
    Total Cost: [TotalCost, sum]
    Total Profit: [TotalProfit, sum]
  field_expansion:
    Order Priority:
      H: High
      M: Medium
      L: Low
      C: Critical
'''
SOURCE_DATA_VALID = '''Region,Country,Item Type,Sales Channel,Order Priority,Order Date,Order ID,Ship Date,Units Sold,Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit
Middle East and North Africa,Libya,Cosmetics,Offline,M,10/18/2014,686800706,10/31/2014,8446,437.2,263.33,3692591.2,2224085.18,1468506.02
Middle East and North Africa,Morocco,Baby Food,Offline,C,10/31/2016,246222341,12/9/2016,1517,255.28,159.42,387259.76,241840.14,145419.62
North America,Canada,Vegetables,Online,M,11/7/2011,185941302,12/8/2011,3018,154.06,90.93,464953.08,274426.74,190526.34
Middle East and North Africa,Morocco,Baby Food,Offline,C,10/31/2016,246222341,12/9/2016,1517,255.28,159.42,387259.76,241840.14,145419.62'''

EXPECTED_OUTPUT = {
  'Middle East and North Africa': {
    'Offline':[
      {'Country': 'Libya', 'ItemType': 'Cosmetics', 'OrderPriority': 'Medium', 'OrderDate': '10/18/2014', 'OrderId': '686800706', 'ShipDate': '10/31/2014', 'UnitsSold': 8446, 'UnitPrice': 437.2, 'UnitCost': 263.33, 'TotalRevenue': 3692591.2, 'TotalCost': 2224085.18, 'TotalProfit': 1468506.02},
      {'Country': 'Morocco', 'ItemType': 'Baby Food', 'OrderPriority': 'Critical', 'OrderDate': '10/31/2016', 'OrderId': '246222341', 'ShipDate': '12/9/2016', 'UnitsSold': 3034, 'UnitPrice': 510.56, 'UnitCost': 318.84, 'TotalRevenue': 387259.76, 'TotalCost': 483680.28, 'TotalProfit': 290839.24}
      ]
  },
  'North America': {
    'Online':[
      {'Country': 'Canada', 'ItemType': 'Vegetables', 'OrderPriority': 'Medium', 'OrderDate': '11/7/2011', 'OrderId': '185941302', 'ShipDate': '12/8/2011', 'UnitsSold': 3018, 'UnitPrice': 154.06, 'UnitCost': 90.93, 'TotalRevenue': 464953.08, 'TotalCost': 274426.74, 'TotalProfit': 190526.34}
      ]
  }
}

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

@mock.patch('builtins.open', new_callable=mock_open, create=True, read_data=TRANSFORM_CONFIG_VALID)
def setup_valid_pipeline(mock_open):
    p = pipeline.Pipeline(TRANSFORM_NAME)
    p.get_config()
    p.configure_preprocess_checks()
    return p

def test_pipeline_initialisation():
    p = pipeline.Pipeline(TRANSFORM_NAME)
    assert len(p.__dict__.keys()) == 9
    assert p.transform_name == TRANSFORM_NAME
    assert p.transform_config_file == 'transforms\\' + TRANSFORM_NAME + '.yaml'
    assert p.config == {}
    assert p.source_file == ''
    assert p.source_file_format == 'csv'
    assert p.source_fields == []
    assert p.output_file == ''
    assert p.output_fields == []
    assert p.preprocess_checks == []

@mock.patch('builtins.open', new_callable=mock_open, create=True, read_data=TRANSFORM_CONFIG_SOURCE_FILENAME_NOT_SPECIFIED)
def test_pipeline_setup_with_no_source_filename_in_config(mock_open):
    p = pipeline.Pipeline(TRANSFORM_NAME)
    with pytest.raises(SystemExit):
        p.get_config()

@mock.patch('builtins.open', new_callable=mock_open, create=True, read_data=TRANSFORM_CONFIG_SOURCE_FIELDS_NOT_SPECIFIED)
def test_pipeline_setup_with_no_source_fields_in_config(mock_open):
    p = pipeline.Pipeline(TRANSFORM_NAME)
    with pytest.raises(SystemExit):
        p.get_config()

@mock.patch('builtins.open', new_callable=mock_open, create=True, read_data=TRANSFORM_CONFIG_OUTPUT_FILENAME_NOT_SPECIFIED)
def test_pipeline_setup_with_no_output_filename_in_config(mock_open):
    p = pipeline.Pipeline(TRANSFORM_NAME)
    with pytest.raises(SystemExit):
        p.get_config()

@mock.patch('builtins.open', new_callable=mock_open, create=True, read_data=TRANSFORM_CONFIG_OUTPUT_FIELDS_NOT_SPECIFIED)
def test_pipeline_setup_with_no_output_fields_in_config(mock_open):
    p = pipeline.Pipeline(TRANSFORM_NAME)
    with pytest.raises(SystemExit):
        p.get_config()

@mock.patch('builtins.open', new_callable=mock_open, create=True, read_data=TRANSFORM_CONFIG_SOME_CHECK_NOT_SPECIFIED)
def test_pipeline_setup_with_some_check_not_in_config(mock_open):
    all_preprocess_checks = [
    'data_completeness',
    'data',
    'date_field',
    'float_field',
    'number_field'
    ]
    p = pipeline.Pipeline(TRANSFORM_NAME)
    p.get_config()
    p.configure_preprocess_checks()
    assert len(p.preprocess_checks) == len(all_preprocess_checks) - 1

@mock.patch('builtins.open', new_callable=mock_open, create=True, read_data=TRANSFORM_CONFIG_DB_NOT_SPECIFIED)
def test_pipeline_setup_db_not_specified(mock_open):
    p = pipeline.Pipeline(TRANSFORM_NAME)
    p.get_config()
    p.configure_preprocess_checks()
    e = pipeline.Extract(p)
    t = pipeline.Transform(p,e)
    with pytest.raises(SystemExit):
        t.get_db_connection()

def test_get_config_with_invalid_transform_name_passed():
    p = pipeline.Pipeline(SOME_INVALID_TRANSFORM_NAME)
    with pytest.raises(SystemExit):
        p.get_config()

@mock.patch('builtins.open', new_callable=mock_open, create=True, read_data=TRANSFORM_CONFIG_VALID)
def test_configure_preprocess_checks(mock_open):
    all_preprocess_checks = [
    'data_completeness',
    'data',
    'date_field',
    'float_field',
    'number_field'
    ]
    p = pipeline.Pipeline(TRANSFORM_NAME)
    p.get_config()
    p.configure_preprocess_checks()
    assert p.preprocess_checks == all_preprocess_checks

@mock.patch('builtins.open', new_callable=mock_open, create=True, read_data=TRANSFORM_CONFIG_VALID)
def test_get_config_with_valid_transform_passed(mock_open):
    expected_config = yaml.safe_load(TRANSFORM_CONFIG_VALID)
    p = pipeline.Pipeline(TRANSFORM_NAME)
    p.get_config()
    assert type(p.config) == dict
    assert p.config == expected_config

@mock.patch('builtins.open', new_callable=mock_open, create=True, read_data=TRANSFORM_CONFIG_VALID)
def test_extract_initialisation(mock_open):
    p = pipeline.Pipeline(TRANSFORM_NAME)
    p.get_config()
    e = pipeline.Extract(p)
    assert len(e.__dict__.keys()) == 10

@mock.patch('builtins.open', new_callable=mock_open, create=True, read_data=TRANSFORM_CONFIG_VALID)
def test_transform_initialisation(mock_open):
    p = pipeline.Pipeline(TRANSFORM_NAME)
    p.get_config()
    p.configure_preprocess_checks()
    e = pipeline.Extract(p)
    t = pipeline.Transform(p,e)
    assert len(t.__dict__.keys()) == 13

@mock.patch('builtins.open', new_callable=mock_open, create=True, read_data=SOURCE_DATA_VALID)
def test_run_data_completeness_check_with_valid_data(mock_open):
    p = setup_valid_pipeline()
    e = pipeline.Extract(p)
    e.extract()
    t = pipeline.Transform(p,e)
    t.transform()
    assert len(t.transformed_data) == 4
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

@mock.patch('builtins.open', new_callable=mock_open, create=True, read_data=SOURCE_DATA_VALID)
def test_gen_output(mock_open):
  p = setup_valid_pipeline()
  e = pipeline.Extract(p)
  e.extract()
  t = pipeline.Transform(p,e)
  t.transform()
  data = t.gen_output()
  differences = DeepDiff(data, EXPECTED_OUTPUT)
  assert differences == {}
