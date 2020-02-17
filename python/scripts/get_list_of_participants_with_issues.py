#!/usr/bin/env python3
import os
from datetime import datetime, timedelta

import pandas as pd
from google.cloud import bigquery


def get_query(table_name: str, start_date: str, end_date: str) -> str:
    return f"""
        SELECT event_date, event_timestamp, event_name, user_pseudo_id, user_prop.key, user_prop.value.string_value
        FROM `{table_name}` as T, UNNEST(user_properties) AS user_prop
        WHERE _TABLE_SUFFIX BETWEEN "{start_date}" AND "{end_date}"
            AND (event_name = "app_reset" OR event_name = "app_remove" OR event_name = "sign_up" )
            AND ( user_prop.key = "subjectId" OR user_prop.key = "projectId")
    """


def get_list_of_studies(query_result: pd.DataFrame):
    return query_result[query_result['key'] == 'projectId'][['user_pseudo_id', 'string_value']]


def write_to_csv_file(filename: str, filepath: str, data: pd.DataFrame):
    os.makedirs(filepath, exist_ok=True)
    with open(os.path.join(filepath, filename), 'w') as f:
        f.write(data.to_csv(index_label='index'))
    print('Written to File successfully.')


def format_datetime(obj: datetime) -> str:
    return obj.strftime('%Y%m%d')


def pre_process_data(data: pd.DataFrame) -> [pd.DataFrame, pd.DataFrame]:
    studies: pd.DataFrame = get_list_of_studies(data)
    # Drop duplicate rows (unnesting resulted in two row for each record one containing subject id and other project id)
    data = data[data['key'] != 'projectId']
    # Add project Id as column
    data['projectId'] = data['user_pseudo_id'].apply(
        lambda x: studies[studies['user_pseudo_id'] == x]['string_value'].values[0])
    # Rename the existing string_value as subjectId since we have removed all rows with Project ID and added as a column
    data.rename({'string_value': 'subjectId'}, axis=1, inplace=True)
    # drop key column as subjectId and projectId columns have been created using it.
    data = data.drop(axis=1, columns=['key'])
    return data, studies


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("-t", "--table", help="The table name in BigQuery. Supports globbing.",
                        default='radar-armt-notification.analytics_180955751.events_*')
    parser.add_argument("-d", "--days",
                        help="The number of days to process the data from. Example if 60 days, then start date will be"
                             " (today - 60 days)", type=int, default=60)
    parser.add_argument("-o", "--output", help="The output path where to store the data.", default="output")
    args = parser.parse_args()
    days_to_subtract = args.days
    start = format_datetime(datetime.today() - timedelta(days=days_to_subtract))
    end = format_datetime(datetime.today())
    table = args.table
    print('Start Date:' + start)
    print('End Date:' + end)

    # Construct a BigQuery client object.
    client = bigquery.Client()

    result: bigquery.QueryJob = client.query(get_query(table, start, end))
    result_df: pd.DataFrame = result.to_dataframe()

    [result_df, studies_list] = pre_process_data(result_df)
    print(result_df.info())
    sorted_df = result_df.sort_values(by='event_timestamp')
    csv_headers = ['event_timestamp', 'projectId', 'subjectId', 'event_names']
    for study in studies_list['string_value'].unique():
        print(f'Processing data for {study}')
        study_issues = list()
        for subjectId in sorted_df[sorted_df['projectId'] == study]['subjectId'].unique():
            subject_details = sorted_df[sorted_df['subjectId'] == subjectId]
            other_events = subject_details[subject_details['event_name'] != 'sign_up']
            other_event_time = other_events['event_timestamp'].max()
            if not other_events.empty:
                if 'sign_up' in subject_details['event_name'].values:
                    sign_up_time = subject_details[subject_details['event_name'] == 'sign_up']['event_timestamp'].max()
                    if sign_up_time >= other_event_time:
                        # Don't include the event as user has signed back again (re-enrolled)
                        print(f'{subjectId} signed back up at: {sign_up_time}')
                    else:
                        # report the user has an issue with event type.
                        study_issues.append([other_event_time, study, subjectId,
                                             other_events[other_events['event_timestamp'] == other_event_time][
                                                 'event_name'].values])
                else:
                    # report the user has an issue with event type.
                    study_issues.append([other_event_time, study, subjectId,
                                         other_events[other_events['event_timestamp'] == other_event_time][
                                             'event_name'].values])
        # Save result of each study to a file. The directory is constructed using output path, start date and end date.
        write_to_csv_file(filename=study + '.csv', filepath=f'{args.output}/' + f'{start}' + f'-{end}',
                          data=pd.DataFrame(study_issues, columns=csv_headers))
