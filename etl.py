'''
simple program to perform ETL process based on config
'''
import argparse
import logging
import sys
import yaml
from pipeline import Pipeline, Extract, Transform

def setup_logging(app_config):
    '''
    Setup logging
    '''
    logging.basicConfig(
        filename=app_config['logging']['log_file'],
        level=app_config['logging']['log_level'],
        format='%(asctime)s.%(msecs)03d %(module)-10s(%(funcName)-20s) %(levelname)-8s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
        )

def main(transform_name, source_file):
    '''
    Main program to run required ETL pipeline
    '''
    logging.info('----- Program started -----')
    logging.info('Transformation name --> %s', transform_name)
    logging.info('Source file --> %s', source_file)
    pipeline = Pipeline(transform_name, source_file)
    pipeline.get_config()
    pipeline.get_preprocess_tasks(pipeline.config)
    extract = Extract(pipeline)
    extract.extract()
    transform = Transform(pipeline,extract)
    transform.run_preprocess_tasks()
    #logging.info(transform.transformed_data)
    #logging.info(yaml.dump(transform.transformed_data))
    transform.transform()
    if transform_name == "sales-summary":
        data = transform.gen_sales_summary()

    if transform_name == "sales-aggregate":
        data = transform.gen_sales_aggregate()

    transform.write_json(data, transform.output_file)
    transform.write_to_db(data)
    logging.info( "----- Program complete -----\n\n" )

if __name__ == "__main__":
    # Parse input arguments
    argsp = argparse.ArgumentParser()
    argsp.add_argument( '-n', '--name',type=str, required=True, help='Transformation name')
    argsp.add_argument( '-f', '--source',type=str, required=True, help='Source data file')
    argsp.add_argument( '-c', '--config', type=str, required=True, help='Application config file')
    args = argsp.parse_args()
    TRANSFORM_NAME = str(vars(args)['name'])
    SOURCE_FILE = str(vars(args)['source'])
    APP_CONFIG = {}
    try:
        with open(str(vars(args)['config']), "r") as file:
            APP_CONFIG = yaml.safe_load(file)
    except Exception as excp:
        print("Error: {} - {}".format(str(excp),repr(excp)))
        sys.exit(1)
    setup_logging(APP_CONFIG)
    main(TRANSFORM_NAME, SOURCE_FILE)
