'''
Pipeline invocation
'''
import sys
import csv
import json
import logging
import copy
from pymongo import MongoClient
from pymongo import errors
import yaml
from yaml.scanner import ScannerError
import utils

ERR_INCOMPLETE_DATA_ROW = "Some fields missing data"
INVALID_MSG="Invalid ({}):{}"
MSG_INVALID_ROW = 'Row %s --> Invalid %s : %s'
PREPROCESS_CHECKS = [
    'data',
    'date_field',
    'float_field',
    'number_field'
    ]

class Pipeline():
    '''
    Initialise and setup configurations
    to run the stages in the pipeline
    '''
    # pylint: disable=too-many-instance-attributes
    # 9 is reasonable in this case
    def __init__(self, transform_name):
        self.transform_name = transform_name
        self.transform_config_file = "transforms\\" + transform_name + ".yaml"
        self.config = {}
        self.source_file_format = "csv"
        self.source_file = ''
        self.source_fields = []
        self.output_file = ''
        self.output_fields = []
        self.preprocess_checks = []

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

        logging.info('Source file format --> %s', self.source_file_format)

        try:
            self.source_file = self.config['source']['file']
        except KeyError:
            logging.error('Source filename not specified')
            sys.exit(1)
        else:
            logging.info('Source file: %s', self.source_file)

        try:
            self.source_fields = self.config['source']['fields']
        except KeyError:
            logging.error('Source fields not specified')
            sys.exit(1)
        else:
            logging.info('Source fields: %s', self.source_fields)

        try:
            self.output_file = self.config['output']['file']
        except KeyError:
            logging.error('Output filename not specified')
            sys.exit(1)
        else:
            logging.info('Output file: %s', self.output_file)

        try:
            self.output_fields = self.config['output']['fields']
        except KeyError:
            logging.error('Output fields not specified')
            sys.exit(1)
        else:
            logging.info('Output fields: %s', self.output_fields)

    def configure_preprocess_checks(self):
        '''
        read preprocess/validation tasks
        '''
        # data completeness check mandatory
        self.preprocess_checks = ['data_completeness']
        # rest of the preprocess tasks
        for task in PREPROCESS_CHECKS:
            try:
                required_task = self.config['checks'][task]
            except KeyError:
                logging.warning('Preprocess check - %s - not specified', task)
                continue
            if required_task:
                self.preprocess_checks.append(task)

class Extract(Pipeline):
    '''
    Methods required to extract data from given source file
    '''
    def __init__(self, pipeline):
        Pipeline.__init__(self, pipeline.transform_name)
        self.source_fields = pipeline.source_fields
        self.source_file = pipeline.source_file
        self.source_data = {}

    def extract(self):
        '''
        Extract data from source file
        '''
        if self.source_file_format.lower() == 'csv':
            try:
                with open( self.source_file, 'r', encoding='utf-8' ) as source_f:
                    data_rows = list(csv.reader(source_f))
            except FileNotFoundError:
                logging.error('Source file not found')
                sys.exit(1)

            if len(data_rows) != 0:
                data_rows.pop(0) # Remove header row

            # create source data as nested dictionary
            # Main key is row number
            # Each row value will be dict of form {field1: value1, field2: value2, ...so on}
            rownum = 1
            for row in data_rows:
                self.source_data[str(rownum)] = dict(zip(self.source_fields,row))
                rownum = rownum + 1
            logging.info('%s records extracted', len(self.source_data))

class Transform(Pipeline):
    '''
    Methods required to perform transformation
    of input source data
    '''
    # pylint: disable=too-many-instance-attributes
    # 9 is reasonable in this case
    def __init__(self, pipeline, extract):
        '''
        inputs:
        pipeline - instance of class Pipeline
        source_data - source data rows as nested dictionary
        '''
        Pipeline.__init__(self, pipeline.transform_name)
        self.preprocess_checks = pipeline.preprocess_checks
        self.output_file = pipeline.output_file
        self.output_fields = pipeline.output_fields
        self.config = pipeline.config
        self.source_fields = extract.source_fields
        self.source_data = extract.source_data
        self.transformed_data = copy.deepcopy(extract.source_data)
        # initialise all initial data as valid
        # data will be individually validated in later stages of pipeline
        for row in self.transformed_data.keys():
            self.transformed_data[row]['col_count'] = len(self.source_data[row])
            self.transformed_data[row]['is_valid'] = True
            self.transformed_data[row]['err_msg'] = []
        self.rejected_data = {}
        self.lookup_to_expand_fields = {}

    def check_missing_fields(self):
        '''
        Check data row is complete
        '''
        for row in self.source_data.keys():
            if len(self.source_fields) != len(self.source_data[row]):
                logging.debug('Record #%s has incomplete data.', row)
                self.transformed_data[row]['is_valid'] = False
                self.transformed_data[row]['err_msg'].append(ERR_INCOMPLETE_DATA_ROW)

    def check_missing_data(self):
        '''
        Check no data row is empty
        '''
        # missing data in fields
        for field in self.source_fields:
            for row in self.source_data.keys():
                if ERR_INCOMPLETE_DATA_ROW not in self.transformed_data[row]['err_msg']:
                    err = []
                    if self.source_data[row][field].strip() == '':
                        err.append(INVALID_MSG.format(field,self.source_data[row][field]))
                        logging.debug(MSG_INVALID_ROW, row, field, self.source_data[row][field])
                    if len(err) > 0:
                        self.transformed_data[row]['is_valid'] = False
                        self.transformed_data[row]['err_msg'].append(err)

    def check_data_completeness(self):
        '''
        Checks all fields have values
        '''
        self.check_missing_fields()
        self.check_missing_data()

    def check_data(self):
        '''
        Checks fields have expected values
        '''
        valid_data_map = self.config['checks']['data']
        for field, valid_values in valid_data_map.items():
            for row in self.source_data.keys():
                if ERR_INCOMPLETE_DATA_ROW not in self.transformed_data[row]['err_msg']:
                    err = []
                    if not self.source_data[row][field] in valid_values:
                        err.append(INVALID_MSG.format(field,self.source_data[row][field]))
                        logging.debug(MSG_INVALID_ROW, row, field, self.source_data[row][field])
                    if len(err) > 0:
                        self.transformed_data[row]['is_valid'] = False
                        self.transformed_data[row]['err_msg'].append(err)

    def check_date_field(self):
        '''
        Checks date fields have valid values
        '''
        fields_to_check = self.config['checks']['date_field']
        for field in fields_to_check:
            for row in self.source_data.keys():
                if ERR_INCOMPLETE_DATA_ROW not in self.transformed_data[row]['err_msg']:
                    err = []
                    if not utils.is_valid_date(self.source_data[row][field],date_format='%m/%d/%Y'):
                        err.append(INVALID_MSG.format(field,self.source_data[row][field]))
                        logging.debug(MSG_INVALID_ROW, row, field, self.source_data[row][field])
                    if len(err) > 0:
                        self.transformed_data[row]['is_valid'] = False
                        self.transformed_data[row]['err_msg'].append(err)

    def check_float_field(self):
        '''
        Checks float fields have valid values
        '''
        fields_to_check = self.config['checks']['float_field']
        for field in fields_to_check:
            for row in self.source_data.keys():
                if ERR_INCOMPLETE_DATA_ROW not in self.transformed_data[row]['err_msg']:
                    err = []
                    if not utils.is_numeric(self.source_data[row][field]):
                        err.append(INVALID_MSG.format(field,self.source_data[row][field]))
                        logging.debug(MSG_INVALID_ROW, row, field, self.source_data[row][field])
                    else:
                        self.transformed_data[row][field] = float(self.transformed_data[row][field])
                    if len(err) > 0:
                        self.transformed_data[row]['is_valid'] = False
                        self.transformed_data[row]['err_msg'].append(err)

    def check_number_field(self):
        '''
        Checks numeric fields have valid values
        '''
        fields_to_check = self.config['checks']['number_field']
        for field in fields_to_check:
            for row in self.source_data.keys():
                if ERR_INCOMPLETE_DATA_ROW not in self.transformed_data[row]['err_msg']:
                    err = []
                    if not self.source_data[row][field].isdigit():
                        err.append(INVALID_MSG.format(field,self.source_data[row][field]))
                        logging.debug(MSG_INVALID_ROW, row, field, self.source_data[row][field])
                    else:
                        self.transformed_data[row][field] = int(self.transformed_data[row][field])
                    if len(err) > 0:
                        self.transformed_data[row]['is_valid'] = False
                        self.transformed_data[row]['err_msg'].append(err)

    def run_preprocess(self):
        '''
        Executing preprocess/validation tasks of pipeline
        '''
        for task in self.preprocess_checks:
            logging.info('Task: %s', task)
            try:
                getattr(Transform, 'check_'+task)(self)
            except AttributeError:
                logging.warning('Task \'%s\' undefined', task)
                continue

    def get_db_connection(self):
        '''
        Get db connection to write transformation output to
        '''
        db_host=''
        db_port=''
        # get db connection
        try:
            db_host = self.config['output']['db']['host']
            db_port = self.config['output']['db']['port']
        except KeyError:
            err_text = 'DB host/port not specified.'
            logging.error(err_text)
            print(err_text + ' Please check logs.')
            sys.exit(1)

        try:
            connect = MongoClient(db_host, db_port)
        except errors.ServerSelectionTimeoutError:
            logging.error('Could not connect to MongoDB')
            sys.exit(1)
        else:
            return connect

    def write_rejected_rows_to_db(self):
        '''
        Write data from dictionary into database
        '''
        data = copy.deepcopy(self.rejected_data)
        # write to db
        db_con = self.get_db_connection()
        db_name = db_con[self.config['output']['db']['name']]
        coll_name = db_name[self.transform_name+'_rejected']
        if coll_name.estimated_document_count() > 0:
            coll_name.drop()
        coll_name.insert_one(data)
        del data

    def transform_data_expansion(self):
        '''
        Replace data with expanded data if setup
        '''
        for field, field_exp in self.config['output']['field_expansion'].items():
            for row, data in self.transformed_data.items():
                if self.transformed_data[row][field] in field_exp.keys():
                    self.transformed_data[row][field] = field_exp[data[field]]

    def transform(self):
        '''
        Transform source data
        Steps:
            Data validations
            Date field validations
            Float field validations
            Digit field validations
        '''
        self.run_preprocess()

        for row in [k for (k,v) in self.transformed_data.items() if v['is_valid'] is False]:
            self.rejected_data[row] = self.source_data[row]
            self.rejected_data[row]['col_count'] = self.transformed_data[row]['col_count']
            self.rejected_data[row]['err_msg'] = self.transformed_data[row]['err_msg']
            logging.warning("Row %s rejected: %s", row, self.source_data[row])
            del self.transformed_data[row]
            logging.debug('Removed row %s', row)

        logging.info('%s rows processed', len(self.transformed_data))

        if len(self.rejected_data) > 0:
            logging.warning( "%s row(s) rejected", len(self.rejected_data))
            self.write_rejected_rows_to_db()

        if 'field_expansion' in self.config['output'].keys():
            self.transform_data_expansion()

    # pylint: disable=too-many-locals
    # 9 is reasonable in this case
    def gen_output(self):
        '''
        Generates output as per configuration
        '''
        intermediate_data = {}
        result = {}
        group_fields = self.config['output']['group_fields']
        leaf_fields = self.config['output']['leaf_fields']
        for row_data in self.transformed_data.values():
            leaf_data = []
            group_key=tuple(i for i in [row_data[field] for field in group_fields])
            leaf_data = {v[0]: row_data[k] for k,v in leaf_fields.items()}

            # Add the row if key not already present
            if group_key not in intermediate_data.keys():
                intermediate_data[group_key] = []
                intermediate_data[group_key].append(leaf_data)
                continue

            # If key exists, add row only if lower level of keys do not exists
            # Else perform the aggregation and update the lower level of key rows
            curr_data = []
            curr_data = intermediate_data[group_key].copy()
            new_data_non_calc = {}
            new_data_non_calc = {v[0]: row_data[k] for k,v in leaf_fields.items() if v[1]==''}
            i = 0
            found_match=0
            for curr in curr_data:
                curr_data_non_calc = {}
                curr_data_non_calc = {v[0]: curr[v[0]] for v in leaf_fields.values() if v[1]==''}
                if curr_data_non_calc == new_data_non_calc:
                    found_match = 1
                    for leaf_field in leaf_fields.values():
                        if leaf_field[1] == 'sum':
                            curr[leaf_field[0]] += leaf_data[leaf_field[0]]
                            curr[leaf_field[0]] = round(curr[leaf_field[0]],2)
                        elif leaf_field[1] == 'avg':
                            curr[leaf_field[0]] += leaf_data[leaf_field[0]]
                            curr[leaf_field[0]] /= 2
                            curr[leaf_field[0]] = round(curr[leaf_field[0]],2)
                curr_data[i] = curr
                i = i + 1
            if found_match == 1:
                intermediate_data[group_key] = curr_data
            else:
                intermediate_data[group_key].append(leaf_data)
        for key, data in intermediate_data.items():
            utils.merge_dicts(target=result, source=utils.get_data_by_group(list(key), data))
        return result

    def write_json(self, data):
        '''
        Write data from dictionary to json file
        '''
        try:
            with open( self.output_file, 'w' ) as json_file:
                json_file.write( json.dumps( data, indent=4, sort_keys=True ) )
        except FileNotFoundError:
            logging.error('Error writing to file - \'%s\'. Validate path.', self.output_file)
        else:
            logging.info('Data written to file - \'%s\'', self.output_file)

    def write_to_db(self, data):
        '''
        Write data from dictionary into database
        '''
        # write to db
        db_con = self.get_db_connection()
        db_name = db_con[self.config['output']['db']['name']]
        coll_name = db_name[self.config['output']['db']['collection']]
        if coll_name.count() > 0:
            coll_name.drop()
        coll_name.insert_one(data)
