'''
simple program to perform ETL process based on config
'''
import argparse
import logging as log
import sys
import yaml
from pipeline import Pipeline, Extract, Transform

def setup_logging(app_config):
    '''
    Setup logging
    '''
    log_filename=app_config['logging']['log_file']
    log_level=app_config['logging']['log_level']
    log_format='%(asctime)s.%(msecs)03d %(module)-10s(%(funcName)-20s) %(levelname)-8s %(message)s'
    date_format='%Y-%m-%d %H:%M:%S'
    log.basicConfig(filename=log_filename,level=log_level,format=log_format,datefmt=date_format)

def main(transform_name):
    '''
    Main program to run required ETL pipeline
    '''
    log.info('----- Program started -----')
    log.info('Transformation name --> %s', transform_name)
    pipeline = Pipeline(transform_name)
    pipeline.get_config()
    pipeline.configure_preprocess_checks()
    extract = Extract(pipeline)
    extract.extract()
    transform = Transform(pipeline,extract)
    transform.transform()
    data = transform.gen_output()
    transform.write_json(data)
    transform.write_to_db(data)
    log.info( "----- Program complete -----\n\n" )

if __name__ == "__main__":
    # Parse input arguments
    argsp = argparse.ArgumentParser()
    argsp.add_argument( '-n', '--name',type=str, required=True, help='Transformation name')
    argsp.add_argument( '-c', '--config', type=str, required=True, help='Application config file')
    args = argsp.parse_args()
    TRANSFORM_NAME = str(vars(args)['name'])
    APP_CFG_FILE = str(vars(args)['config'])
    APP_CONFIG = {}
    try:
        with open(APP_CFG_FILE, "r") as file:
            APP_CONFIG = yaml.safe_load(file)
    except FileNotFoundError:
        print('{} not found'.format(APP_CFG_FILE))
        sys.exit(1)
    else:
        # setup logging and kickoff transformation process
        setup_logging(APP_CONFIG)
        main(TRANSFORM_NAME)
