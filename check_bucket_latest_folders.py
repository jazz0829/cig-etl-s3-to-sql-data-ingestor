import boto
import boto.s3
from boto.s3.connection import S3Connection
from boto.s3.prefix import Prefix
from boto.s3.key import Key
import pandas
from pandas.io.json import json_normalize
import json
import os
from datetime import datetime, timedelta

from SlackNotifier import SlackNotifier

import logging
import argparse

def list_directories(bucket, prefix='', max_keys=100):
    """Get all items in a directory.
    Returns a dictionary with keys: 'files' and 'directories'
    """
    # strip leading '/' from the prefix
    prefix = prefix.lstrip('/')
    if prefix != '' and prefix[-1] != '/':
        prefix = prefix + '/'

    # a list of keys/prefixes
    all_keys = bucket.get_all_keys(
        max_keys=max_keys, delimiter='/', prefix=prefix
    )
    out = {
        'prefix': prefix,
        'directories': []
    }
    for key in all_keys:
        if isinstance(key, Prefix):
            out['directories'].append(key.name)
    return out

def get_subfolders_recursive(bucket, prefix='', max_keys=100):
    folder_structure = list_directories(bucket, prefix, max_keys)

    for i, d in enumerate(folder_structure['directories']):
        if i > max_keys:
            break
        folder_structure['directories'][i] = get_subfolders_recursive(bucket, d, max_keys)

    if not len(folder_structure['directories']):
        folder_structure.pop('directories')

    return folder_structure

def get_date_from_folder_structure(folder_structure, result=''):
    folders = folder_structure.get('directories', [])
    if(len(folders) == 0):
        return result.strip('-')

    latest_folder = folders[-1]

    text = latest_folder['prefix'].strip('/').split('/')[-1]

    result += text + "-"
    return get_date_from_folder_structure(latest_folder, result)


def send_notification(slack_notifier, not_up_to_date_entites, wrong_structure_folders, environments, bucket_address):
    header = "Check : Data in source bucket\nFor the following Environments: " + ", ".join(environments)
    body = ""
    skipped_tables_warining = 'Note that this script (`check_bucket_latest_folders.py`) has a hardcoded list of exceptions including static data tables like CompanySizes and rarely changing tables like ItemFeatureSets. They got filtered out in order to not create noise in this message. However, this filtering can skip notification of some missing rearly changing data.'
    footer = "The bucket: https://s3.console.aws.amazon.com/s3/buckets/{}/\n\n{}\n\n\n{}"\
        .format(bucket_address, skipped_tables_warining, "----------------------------------------------------------------")
    if(len(not_up_to_date_entites) > 0):
        header = ":warning:\n" + header
        
        df = json_normalize(not_up_to_date_entites)
        df = df[["environment", "source_name", "latest_date"]]
        table = str(df.iloc[:])

        text = "Not all the entity sources containing data for today from Cumulus."
        body = "{}\n\n{}\n".format(text, table)
    else:
        header = ":heavy_check_mark:\n" + header
        body = '\nAll entity sources has data from Cumulus for today.\n'+skipped_tables_warining
    
    if(len(wrong_structure_folders) > 0):
        wrong_folders = "\n".join(wrong_structure_folders)
        body = "{}\nWarining. Following folders have wrong structure or just missing in the bucket:\n{}\n".format(body, wrong_folders)
    
    message = "{}\n{}\n{}".format(header, body, footer)
    slack_notifier.notify_with_message(message)

def find_not_up_to_date_folders(folders_state):
    now = datetime.now().date()
    not_up_to_date_entites = [f for f in folders_state if f['latest_date'] < now]
    
    BOID = datetime(2019, 3, 19)# The begginig of ingestions date
    static_tables = {
       'ActivitySectors': {
           "NL": BOID, "BE": BOID, "UK": BOID, "US": BOID, "FR": BOID, "ES": BOID, "DE": BOID},
        'BusinessTypes': {
           "NL": BOID, "BE": BOID, "UK": BOID, "US": BOID, "FR": BOID, "ES": BOID, "DE": BOID},
        'Classifications': {
           "NL": datetime(2019, 3, 29), "BE": datetime(2019, 4, 7), "UK": BOID, "US": BOID, "FR": datetime(2019, 4, 6), "ES": BOID, "DE": BOID},
        'CompanySizes':  {
           "NL": BOID, "BE": BOID, "UK": BOID, "US": BOID, "FR": BOID, "ES": BOID, "DE": BOID},
        'ContractEvents': {
           "NL": BOID, "BE": BOID, "UK": BOID, "US": BOID, "FR": BOID, "ES": BOID, "DE": BOID},
        'CreditManagementStatus': {
           "NL": BOID, "BE": datetime(2019, 5, 12), "UK": BOID, "US": BOID, "FR": BOID, "ES": BOID, "DE": BOID},
        'ExchangeRateTypes': {
           "NL": BOID, "BE": BOID, "UK": BOID, "US": BOID, "FR": BOID, "ES": BOID, "DE": BOID},
        'ItemClasses': {
           "NL": BOID, "BE": BOID, "UK": BOID, "US": BOID, "FR": BOID, "ES": BOID, "DE": BOID},
        'ItemRelationRelations': {
           "NL": datetime(2019, 5, 15), "BE": BOID, "UK": BOID, "US": BOID, "FR": BOID, "ES": BOID, "DE": BOID},
        'LeadSources': {
           "NL": datetime(2019, 3, 26), "BE": BOID, "UK": BOID, "US": BOID, "FR": BOID, "ES": BOID, "DE": BOID},
        'OpportunityStages': {
           "NL": BOID, "BE": BOID, "UK": BOID, "US": BOID, "FR": BOID, "ES": BOID, "DE": BOID},
        'UserTypes':{
           "NL": datetime(2019, 5, 15), "BE": BOID, "UK": BOID, "US": BOID, "FR": BOID, "ES": BOID, "DE": BOID},

        # More tricky cases
        'ConversionStatus':{
           "UK": BOID, "FR": BOID, "DE": BOID},
        'DivisionTypes':{
           "UK": BOID, "US": BOID, "FR": BOID, "ES": BOID},
        'ItemRelations': {
            "UK": BOID, "US": BOID, "ES": BOID},
        'Items': {
            "US": BOID, "ES": BOID, "DE": BOID},
        'Projects':{
            "UK": BOID, "US": BOID, "DE": BOID},
        "SurveyResults":{
            "US": BOID},
        'SubscriptionQuotations': {
            "UK":datetime(2019, 3, 20), "ES": BOID, "DE": BOID},
        'TimeCostTransactions': {
            "UK": BOID, "US": BOID, "DE": BOID},
        'UsageEntitlements': {
            "UK": BOID, "US": BOID, "DE": BOID}
    }

    few_days_delay_tables = {
        #'DivisionStaistics':                ["ES","US","UK"],
        'DivisionTypes':                    ["NL","BE"],
        'DivisionDivisionTypes':            ["FR","ES"],
        'SubscriptionQuotations':           ["NL","BE"],
        'OAuthClientUsers':                 ["FR","US","DE"],
        'TimeCostTransactions':             ["NL","BE"],
        'UserDivisionHistory':              ["FR"],
        'UserUserTypes':                    ["US"]
    }

    weekly_chainging_tables = {
        'Accounts':                         ["US"],
        'ConversionStatus':                 ["NL","BE","US","ES","DE"],
        'DivisionTypes':                    ["NL","BE"],
        'ItemFeatureSets':                  ["NL","BE"],
        'ItemRelations':                    ["NL"],
        'Items':                            ["NL","DE"],
        'OAuthClients':                     ["NL","BE","UK","DE"],
        'DivisionDivisionTypes':            ["DE"],
        'FirstAppUsage':                    ["UK","FR","ES","DE"],
        'Opportunities':                    ["UK","FR","ES","DE"],
        'PaymentTerms':                     ["US"],
        'CIG_Requests':                     ["UK","US","DE"],
        'CustomerSubscriptionStatistics':   ["UK","FR","ES","DE"],
        'SurveyResults':                    ["UK","FR","DE"],
        'UserDivisionHistory':              ["US"],
        "Persons":                          ["US"]
    }

    once_in_2_weeks_chainging_tables = {
        'Items':                            ["BE","FR"],
        'ItemFeatureSets':                  ["UK","FR"],
        'ItemRelations':                    ["BE"],
        'OAuthClients':                     ["FR"],
        'SurveyResults':                    ["FR"],
        'UsageTransactions':                ["UK"],
    }

    monthly_chainging_tables = {
        'CustomerSubscriptionStatistics':   ["US"],
        'DivisionDivisionTypes':            ["US"],
        'FirstAppUsage':                    ["US"],
        'Items':                            ["UK"],
        'ItemFeatureSets':                  ["ES","US","DE"],
        'OAuthClients':                     ["ES","US"],
        'Opportunities':                    ["US"],
        'Projects':                         ["FR","ES","DE"],
        'SubscriptionQuotations':           ["FR"]
    }

    once_in_2_months_chainging_tables = {
        'DivisionTypes':                    ["DE"],
        'ItemRelations':                    ["FR","DE"],
        'TimeCostTransactions':             ["FR","ES"],
        'UsageEntitlements':                ["FR"],        
        'UsageTransactions':                ["US"]
    }

    static_entities_to_skip = [e for e in not_up_to_date_entites
        if (e['source_name'] in static_tables and 
            e['environment'] in static_tables[e['source_name']] and 
            e['latest_date'] >= static_tables[e['source_name']][e['environment']].date())]

    few_days_delay_entities_to_skip = get_tables_to_skip(
        not_up_to_date_entites, few_days_delay_tables, 2)

    weakly_entities_to_skip = get_tables_to_skip(
        not_up_to_date_entites, weekly_chainging_tables, 7)

    per_2_weeks_entities_to_skip = get_tables_to_skip(
        not_up_to_date_entites, once_in_2_weeks_chainging_tables, 14)

    monthly_entities_to_skip = get_tables_to_skip(
        not_up_to_date_entites, monthly_chainging_tables, 30)

    per_2_months_entities_to_skip = get_tables_to_skip(
        not_up_to_date_entites, once_in_2_months_chainging_tables, 60)

    not_up_to_date_entites = [e for e in not_up_to_date_entites
        if not e in (
            static_entities_to_skip +
            few_days_delay_entities_to_skip + 
            weakly_entities_to_skip + 
            per_2_weeks_entities_to_skip + 
            monthly_entities_to_skip + 
            per_2_months_entities_to_skip)]

    return not_up_to_date_entites

def get_tables_to_skip(all_tables, exceptional_tables, exception_period_in_days):
    now = datetime.now().date()
    return [e for e in all_tables
        if (e['source_name'] in exceptional_tables and 
            e['environment'] in exceptional_tables[e['source_name']] and 
            e['latest_date'] >= now - timedelta(days = exception_period_in_days))]

def read_json(file_path):
    if file_path is None or not os.path.isfile(file_path):
        raise FileNotFoundError(file_path)
    data = {}
    with open(file_path) as file:
        data = json.load(file)
    return data

def create_logger(file_path):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s || %(levelname)s %(message)s")
    file_handler = logging.FileHandler(file_path)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)

    return logger

if __name__ == "__main__":
    ingestion_config_filename = 'ingestion_config.json'
    parser = argparse.ArgumentParser()
    parser.add_argument('--ingestion_config_filename', help='Specify filename of ingestion configuration (Default="ingestion_config.json")')    
    args = parser.parse_args()
    if not args.ingestion_config_filename is None and len(args.ingestion_config_filename) > 0:
        ingestion_config_filename = args.ingestion_config_filename

    currentFolder = os.path.dirname(__file__)
    config = read_json(os.path.join(currentFolder, ingestion_config_filename))

    now = datetime.now()
    logger = create_logger("{}\\eol_hosting_bucket_data_check.{}.log".format(config['logs_folder'], now.strftime('%Y-%m')))
    logger.info("============================================================\nStarting Bucket check: EOL Hosting Data")    

    environments = config['environments_to_check']
    bucket_address = config['ingest_from']

    tables = read_json(os.path.join(currentFolder, config['tables_to_download_config_file']))
    entity_folders = [t['source'] for t in tables]
    
    slack_notifier = SlackNotifier()
    wrong_structure_folders = []
    try:
        conn = boto.s3.connect_to_region('eu-west-1')
        bucket = conn.get_bucket(bucket_address)
        logger.info("AWS connection established.")

        folders_state = []
        for env in environments:
            for source in entity_folders:
                folders = get_subfolders_recursive(bucket, "environment={}/{}/".format(env, source))
                latest_date_str = get_date_from_folder_structure(folders)
                if(len(latest_date_str) < 1):
                    wrong_structure_folders.append("{} {} {}".format(env, source, latest_date_str))
                    continue
                latest_date = datetime.strptime(latest_date_str, '%Y-%m-%d').date()
                folders_state.append({'environment': env, 'source_name': source, 'latest_date': latest_date})
        
        logger.info("Summarizing the bucket folders state...")
        not_up_to_date_entites = find_not_up_to_date_folders(folders_state)
        logger.info("Sending Slack notification...")
        send_notification(slack_notifier, not_up_to_date_entites, wrong_structure_folders, environments, bucket_address)
    except Exception as e:
        logger.fatal(e, exc_info=True)
        slack_notifier.notify_with_message(":rage4:\nEOL Hosting bucket data check failed\n{}".format(e))
    logger.info("Check completed.")