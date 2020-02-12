# radar-bigquery python

A collection of python scripts for processing data from big query.

## scripts/get_list_of_participants_with_issues.py

This script is used for finding out the participants which have been logged-out/uninstalled the application and have not yet re-enrolled.
The output is of the structure - `output/{start-date}-{end-date}/{study-name}.csv`

### Usage

1. First install all the requirements using 
    ```shell script
     pip install -r requirements.txt
    ```
   
2. Add your Google cloud credentials file path as environment variable `GOOGLE_APPLICATION_CREDENTIALS`. This can be done using `export` command on linux.
For more information, see the [official docs]().

3. You can see the usage of the script using 
    ```shell script
     python3 scripts/get_list_of_participants_with_issues.py --help
    ```