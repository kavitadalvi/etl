'''
Pipeline invocation
'''
import sys
import csv
import json
import logging
import copy
from pymongo import MongoClient
import yaml
from yaml.scanner import ScannerError
import utils

ERR_INCOMPLETE_DATA_ROW = "Some fields missing data"
MSG_INVALID_ROW = 'Row %s --> Invalid %s : %s'
PREPROCESS_TASKS = [
    'data_validations_check',
    'date_field_check',
    'float_field_check',
    'number_field_check'
    ]

class Pipeline():
    '''
    Initialise and setup configurations
    to run the stages in the pipeline
    '''
    def __init__(self, transform_name, source_filename):
        self.transform_name = transform_name
        self.transform_config_file = "transforms\\" + transform_name + ".yaml"
        self.source_filename = source_filename
        self.source_file_format = "csv"
        self.config = {}
        self.preprocess_tasks = []

    def get_config(self):
        '''
        read initial required setup from config file
        '''
        try:
            with open(self.transform_config_file, "r") as file:
                self.config = yaml.safe_load(file)
        except FileNotFoundError:
            logging.error("Transform config file %s not found", self.transform_config_file)
            sys.exit(1)
        except ScannerError:
            logging.error("Cannot scan file %s", self.transform_config_file)
            sys.exit(1)

    def get_preprocess_tasks(self, config):
        '''
        read preprocess/validation tasks required
        '''
        # data completeness check mandatory
        self.preprocess_tasks = ['data_completeness_check']

        # rest of the preprocess tasks
        for task in PREPROCESS_TASKS:
            try:
                required_task = config[task]
            except KeyError:
                logging.debug('%s not required', task)
            if required_task:
                self.preprocess_tasks.append(task)

class Extract(Pipeline):
    '''
    Methods required to extract data from given source file
    '''
    def __init__(self, pipeline):
        Pipeline.__init__(self, pipeline.transform_name, pipeline.source_filename)
        self.source_file_format = pipeline.source_file_format
        self.source_filename = pipeline.source_filename
        self.source_fields = pipeline.config['source_fields']
        self.source_data = {}

    def extract(self):
        '''
        Extract data from source file
        '''
        logging.info('Source file format --> %s', self.source_file_format)
        if self.source_file_format.lower() == 'csv':
            try:
                with open( self.source_filename, 'r', encoding='utf-8' ) as source_f:
                    data_rows = list(csv.reader(source_f))
            except FileNotFoundError:
                logging.error('Source file not found')
                sys.exit(1)

            if len(data_rows) <= 1:
                logging.warning('No data in source. Exiting.')
                sys.exit(1)
            else:
                data_rows.pop(0) # Remove header row
                # return source data as nested dictionary
                # Main key is row number
                # Each row value will be dict of form {field1: value1, field2: value2, ...so on}
                rownum = 1
                for row in data_rows:
                    self.source_data[rownum] = dict(zip(self.source_fields,row))
                    rownum = rownum + 1
                logging.info('%s records extracted', len(self.source_data))

class Transform(Pipeline):
    '''
    Methods required to perform transformation
    of input source data
    '''
    # pylint: disable=too-many-instance-attributes
    # 12 is reasonable in this case
    def __init__(self, pipeline, extract):
        '''
        inputs:
        pipeline - instance of class Pipeline
        source_data - source data rows as nested dictionary
        '''
        Pipeline.__init__(self, pipeline.transform_name, pipeline.source_filename)
        self.preprocess_tasks = pipeline.preprocess_tasks
        self.config = pipeline.config
        self.source_fields = extract.source_fields
        self.source_data = extract.source_data
        self.transformed_data = copy.deepcopy(extract.source_data)
        # initialise all initial data as valid
        # data will be individually validated in later stages of pipeline
        for row in self.transformed_data.keys():
            self.transformed_data[row]['is_valid'] = True
            self.transformed_data[row]['err_msg'] = []
        self.rejected_data = {}
        self.output_file = ''
        self.lookup_to_expand_fields = {}

    #@staticmethod
    def run_data_completeness_check(self):
        '''
        Checks all fields have values
        '''
        for row in self.source_data.keys():
            if len(self.source_fields) != len(self.source_data[row]):
                logging.debug('Record #%s has incomplete data.', row)
                self.transformed_data[row]['is_valid'] = False
                self.transformed_data[row]['err_msg'].append(ERR_INCOMPLETE_DATA_ROW)

    #@staticmethod
    def run_data_validations_check(self):
        '''
        Checks fields have expected values
        '''
        valid_data_map = self.config['data_validations_check']
        for field, valid_values in valid_data_map.items():
            for row in self.source_data.keys():
                if ERR_INCOMPLETE_DATA_ROW not in self.transformed_data[row]['err_msg']:
                    err = []
                    if not self.source_data[row][field] in valid_values:
                        err.append("Invalid ({}):{}".format(field,self.source_data[row][field]))
                        logging.debug(MSG_INVALID_ROW, row, field, self.source_data[row][field])
                    if len(err) > 0:
                        self.transformed_data[row]['is_valid'] = False
                        self.transformed_data[row]['err_msg'].append(err)

    #@staticmethod
    def run_date_field_check(self):
        '''
        Checks date fields have valid values
        '''
        fields_to_check = self.config['date_field_check']
        for field in fields_to_check:
            for row in self.source_data.keys():
                if ERR_INCOMPLETE_DATA_ROW not in self.transformed_data[row]['err_msg']:
                    err = []
                    if not utils.is_valid_date(self.source_data[row][field],date_format='%m/%d/%Y'):
                        err.append("Invalid ({}):{}".format(field,self.source_data[row][field]))
                        logging.debug(MSG_INVALID_ROW, row, field, self.source_data[row][field])
                    if len(err) > 0:
                        self.transformed_data[row]['is_valid'] = False
                        self.transformed_data[row]['err_msg'].append(err)

    #@staticmethod
    def run_float_field_check(self):
        '''
        Checks float fields have valid values
        '''
        fields_to_check = self.config['float_field_check']
        for field in fields_to_check:
            for row in self.source_data.keys():
                if ERR_INCOMPLETE_DATA_ROW not in self.transformed_data[row]['err_msg']:
                    err = []
                    if not utils.is_numeric(self.source_data[row][field]):
                        err.append("Invalid ({}):{}".format(field,self.source_data[row][field]))
                        logging.debug(MSG_INVALID_ROW, row, field, self.source_data[row][field])
                    if len(err) > 0:
                        self.transformed_data[row]['is_valid'] = False
                        self.transformed_data[row]['err_msg'].append(err)

    #@staticmethod
    def run_number_field_check(self):
        '''
        Checks numeric fields have valid values
        '''
        fields_to_check = self.config['number_field_check']
        for field in fields_to_check:
            for row in self.source_data.keys():
                if ERR_INCOMPLETE_DATA_ROW not in self.transformed_data[row]['err_msg']:
                    err = []
                    if not self.source_data[row][field].isdigit():
                        err.append("Invalid ({}):{}".format(field,self.source_data[row][field]))
                        logging.debug(MSG_INVALID_ROW, row, field, self.source_data[row][field])
                    if len(err) > 0:
                        self.transformed_data[row]['is_valid'] = False
                        self.transformed_data[row]['err_msg'].append(err)

    def run_preprocess_tasks(self):
        '''
        Executing preprocess/validation tasks of pipeline
        '''
        for task in self.preprocess_tasks:
            logging.info('Task: %s', task)
            try:
                getattr(Transform, 'run_'+task)(self)
            except AttributeError:
                logging.warning('Task \'%s\' undefined', task)
                continue

    def transform(self):
        '''
        Transform source data
        Steps:
            Data validations
            Date field validations
            Float field validations
            Digit field validations
        '''
        for row in [k for (k,v) in self.transformed_data.items() if v['is_valid'] is False]:
            self.rejected_data[row] = self.source_data[row]
            logging.warning("Row %s rejected: %s", row, self.source_data[row])
            del self.transformed_data[row]
            logging.debug('Removed row %s', row)
        if len(self.rejected_data) > 0:
            logging.warning( "%s row(s) rejected", len(self.rejected_data))

        self.output_file = self.config['output_file']
        logging.info("Output file --> %s", self.output_file)

        #if (len(self.transformed_data)) == 0:
        #    # exit if no data is transformed
        #    logging.warning("0 rows transformed. Exiting pipeline.")
        #    sys.exit(1)
        #else:
        #    logging.info( "%s row(s) transformed", len(self.transformed_data))

    def calc_revenue(self, sales_dict, region, country):
        '''
        Calculate total revenue per region-country in transformed data
        '''
        revenue = 0
        for value in sales_dict.values():
            if value['Region']==region and value['Country']==country:
                revenue = revenue + float(value['Total Revenue'])
        return revenue

    def calc_profit(self, sales_dict, region, country):
        '''
        Calculate total profit per region-country in transformed data
        '''
        profit = 0
        for value in sales_dict.values():
            if value['Region']==region and value['Country']==country:
                profit = profit + float(value['Total Profit'])
        return profit

    def gen_sales_aggregate(self):
        '''
        Generate sales aggregate summary from transformed data
        '''
        result = {}
        regions = set(v['Region'] for k,v in self.transformed_data.items())
        for region in regions:
            countries = set(
                [v['Country'] for k,v in self.transformed_data.items() if v['Region']==region]
                )
            region_list = []
            for country in countries:
                profit = self.calc_profit(self.transformed_data,region,country)
                revenue = self.calc_revenue(self.transformed_data,region,country)
                region_list.append({
                    "Country": country
                    ,"CountryRevenue": revenue
                    ,"CountryProfit": profit}
                    )
            result[region] = region_list
        return result

    def gen_sales_summary(self):
        '''
        Generate sales summary from the transformed data
        '''
        result = {}
        regions = set(v['Region'] for k,v in self.transformed_data.items())
        for region in regions:
            sales_channels = set(
                [v['Sales Channel'] for k,v in self.transformed_data.items() if v['Region']==region]
                )
            sales_channel_dict = {}
            for sales_channel in sales_channels:
                summary_list = []
                summary_list = [{
                        "Country": v['Country'],
                        "ItemType": v['Item Type'],
                        "OrderPriority": v['Order Priority'],
                        "OrderDate": v['Order Date'],
                        "OrderId": v['Order ID'],
                        "ShipDate": v['Ship Date'],
                        "UnitsSold": v['Units Sold'],
                        "UnitPrice": v['Unit Price'],
                        "UnitCost": v['Unit Cost'],
                        "TotalRevenue": v['Total Revenue'],
                        "TotalCost": v['Total Cost'],
                        "TotalProfit": v['Total Profit']
                } for k,v in self.transformed_data.items()
                if v['Region']==region and v['Sales Channel'] == sales_channel]
                sales_channel_dict[sales_channel] = summary_list
            result[region] = sales_channel_dict
        return result

    def write_json(self, data, file):
        '''
        Write data from dictionary to json file
        '''
        with open( file, 'w' ) as json_file:
            json_file.write( json.dumps( data, indent=4, sort_keys=True ) )

    def write_to_db(self, data):
        '''
        Write data from dictionary into database
        '''
        # establing connection
        try:
            connect = MongoClient()
            print("Connected successfully!!!")
        except Exception as excp:
            print("Could not connect to MongoDB: {}", format(excp))

        connect.drop_database('sales')
        db_name=connect.sales
        coll=db_name.sales_summary
        coll.insert_one(data)