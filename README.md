# About
Simple command line ETL utility/tool for processing of sales data (csv) and transformation into summarized and aggregated view


# Setup

_Python 3.8 or higher_

1. Application setup
    
    a.  Download code locally, say in folder APP_DIR and extract all files.
    
        Directory structure should look like this
    
        <APP_DIR>
        └input
            ├sales-records.csv
        └log
        └output
        └transforms
            ├sales-aggregate.yaml
            ├sales-summary.yaml
        ├app-test.yaml
        ├app-prod.yaml
        ├etl.py
        ├pipeline.py
        ├requirements.txt
        ├utils.py
    
    b.  Create link APP_DIR/app.yaml pointing to APP_DIR/app-\<env\>.yaml depending on environment to run applicaiton on.

2. Install requirements

        pip install -r requirements.txt

# Execution
    
    python etl.py -n "sales-summary" -c "<APP_DIR_PATH>/app.yaml" 
    python etl.py -n "sales-aggregate" -c "<APP_DIR_PATH>/app.yaml"
    
   where,
    
   -n NAME, --name NAME        Transformation name

   -f SOURCE, --source SOURCE  Source data file

   -c CONFIG, --config CONFIG  Application config file

   Ensure there is corresponding config file in folder **tranforms** for the transformation required.
       
   For transformation 'sales-summary', config file 'sales-summary.yaml' should be present.
        
   For transformation 'sales-aggregate', config file 'sales-aggregate.yaml' should be present.
       
