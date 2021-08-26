from datetime import datetime
import pandas
import json

import luigi
import urllib

import os
from itertools import groupby
from SlackNotifier import SlackNotifier

from ParquetFileInsertion import IngestParquetFile
from LoggerFactory import LoggerFactory

import argparse
import logging
import traceback

class RunAllEolHostingMailboxIngestions(luigi.WrapperTask):
    data_folder = luigi.Parameter()
    eol_tables_config_path = luigi.Parameter()
    data_sources_to_ingest = luigi.ListParameter()
    ingest_to = luigi.Parameter()
    ingestion_date = luigi.DateParameter()
    logs_file_path = luigi.Parameter()

    # The logic of starting multiple tasks basing on our files/partitioning structure
    # Warning! Dependent workers (for IngestParquetFile) will run in a separate process. Unfortunatelly, that complicates troubleshooting and logging.
    # To debug code logic, change luigi.cfg [core] workers = 1. Don't forget to undo this change before commiting.
    # Note, that for multiprocess behaviour number of workers should be > 1.
    # To troubleshoot multiprocess issues use logging since debugging will be unable.
    def requires(self):
        # Logger for this task somehow breaking the multi-thread execution.
        # Therefore, no logger is used for this task.
        eol_tables_config = self.read_tables_configuration(self.eol_tables_config_path)

        configured_files_to_ingest = self.aggregate_files_configured_to_ingest(self.data_folder, eol_tables_config)
        ### For debugging a specific parquet file
        #configured_files_to_ingest = [f for f in configured_files_to_ingest if f.FileName=='33413888-000000-0000.parquet']

        for datasource, files_to_ingest_for_datasource in groupby(configured_files_to_ingest, lambda x: x.DataSource):
            if (not datasource in self.data_sources_to_ingest):
                continue
            for file_to_ingest in files_to_ingest_for_datasource:
                # Skip not enabled tables and files for not today
                if(file_to_ingest.Date < self.ingestion_date or
                        not file_to_ingest.IsEnabled):
                    continue
                
                yield IngestParquetFile(
                    ingest_to = self.make_db_connection_string(),

                    ingestion_date = self.ingestion_date,
                    # environment will be get from data source
                    # example for data source: NL_Hosting_Mailbox
                    environment = file_to_ingest.DataSource.split("_")[0],

                    parquet_source = file_to_ingest.FileName,
                    parquet_source_location = file_to_ingest.FullPath,
                    backup_date = file_to_ingest.Date,
                    table = file_to_ingest.TargetTable,
                    column_names = file_to_ingest.TargetColumns,
                    
                    logs_file_path = self.logs_file_path
                )

    def make_db_connection_string(self):
        connection_string = "Driver={SQL Server};Server=" + self.ingest_to + ";Database=CustomerIntelligence;UID=;PWD="
        connection_string = urllib.parse.quote_plus(connection_string) 
        connection_string = "mssql+pyodbc:///?odbc_connect=%s" % connection_string
        return connection_string

    def read_tables_configuration(self, config_file_path):
        if config_file_path is None or not os.path.isfile(config_file_path):
            raise FileNotFoundError(config_file_path)
        tables = pandas.read_json(config_file_path)
        if len(tables) < 1:
            raise Exception("List of tables to ingest is empty at: {}".format(config_file_path))
        return json.loads(tables.to_json(orient='records'))

    def aggregate_files_configured_to_ingest(self, root_folder, eol_tables_config):
        local_files = self.list_all_files_of_eol_hosting_structure(root_folder)
        files_to_ingest = [f for f in local_files 
                                if any(table['source'] == f.EntityName for table in eol_tables_config)]
        for file in files_to_ingest:
            config = ([c for c in eol_tables_config if c['source'] == file.EntityName])[0]
            file.ExtendWithIngestionConfig(config)
        return files_to_ingest

    def check_path(self, file_path):
        # path structure: data folder path\\NL_Hosting_Mailbox\\Tables\\2019\\12\\31
        return len(file_path.split('\\')[-5:][0].split('_')) > 1

    def list_all_files_of_eol_hosting_structure(self, folder_path):
        paths = []
        for path, subdirs, files in os.walk(folder_path):
            if not self.check_path(path):
                continue
            for name in files:
                paths.append(os.path.join(path, name))
        cig_files = [CigIngestionFile(p) for p in paths]
        return cig_files

class CigIngestionFile():
    def __init__(self, file_path):
        if file_path is None:
            raise FileNotFoundError(file_path)
        # path structure: data folder path\\NL_Mailbox_Hosting\\table\\2019\\12\\31\\file.parquet
        splitted_path = file_path.split('\\')
        splitted_path = splitted_path[-6:]

        if len(splitted_path) < 1:
            raise FileNotFoundError(file_path)

        datasource = splitted_path[0]
        date_string = "{}-{}-{}".format(splitted_path[2], splitted_path[3], splitted_path[4])

        self.FullPath = file_path
        self.FileName = splitted_path[5]
        self.DataSource = datasource
        self.Date = datetime.strptime(date_string, "%Y-%m-%d").date()
        self.EntityName = splitted_path[1]
    
    def ExtendWithIngestionConfig(self, ingestion_config):
        self.TargetTable = ingestion_config['target_name']
        self.TargetColumns = ingestion_config['columns']
        self.IsEnabled = ingestion_config['is_enabled']

def read_ingestion_config(ingestion_config_filename):
    config = {}
    currentFolder = os.path.dirname(__file__)
    with open(os.path.join(currentFolder, ingestion_config_filename)) as file: 
        config = json.load(file)
    return config

def send_execution_summary(execution_summary, environments, db_server_name, ingestion_date):
    icon = ":heavy_check_mark:" if execution_summary.worker.run_succeeded else ":fish_cake:"
    luigi_page = 'http://nlc1prodci02:8082/static/visualiser/index.html'

    header = "{}\n EOL Hosting ETL : Ingestion summary\n\nFor the following Environments: {}\nIngestion date (CIGCopyTime): {}"\
        .format(icon, ", ".join(environments), ingestion_date)
    body = "\nRun visualization page: {}\n\n{}\n".format(luigi_page, execution_summary.summary_text)
    footer = "The SQL Server DB: {}\n{}".format(db_server_name, "----------------------------------------------------------------")
    message = "{}\n{}\n{}".format(header, body, footer)
    SlackNotifier().notify_with_message(message)

# The entry point
# Before starting it luigi server must be started
# Via cmd : luigid
#      Or : python start_luigi_server.py
if __name__ == "__main__":
    try:
        ingestion_config_filename = 'ingestion_mailbox_config.json'
        parser = argparse.ArgumentParser()
        parser.add_argument('--ingestion_config_filename', help='Specify filename of ingestion configuration (Default="ingestion_config.json")')    
        args = parser.parse_args()
        if not args.ingestion_config_filename is None and len(args.ingestion_config_filename) > 0:
            ingestion_config_filename = args.ingestion_config_filename

        now = datetime.now()
        config = read_ingestion_config(ingestion_config_filename)
        data_folder = config['data_folder']+'\\'
        data_sources_to_ingest = config['data_sources']
        ingest_to = config['ingest_to']
        ingestion_date = datetime.strptime(config['ingestion_date'], '%Y-%m-%d') if len(config['ingestion_date']) > 0 else now
        currentFolder = os.path.dirname(__file__)
        tables_to_upload_config_file = os.path.join(currentFolder, config['tables_to_upload_config_file'])
        logs_file_path = "{}\luigi.{}.log".format(config['logs_folder'], now.strftime('%Y-%m'))

        logger = LoggerFactory.create_logger_for_multiprocess_use(logs_file_path, logging.INFO, 'main')
        logger.info("The ingestion is configured and started.")

        execution_summary = luigi.build(
            [RunAllEolHostingMailboxIngestions(
                eol_tables_config_path = tables_to_upload_config_file,
                data_folder = data_folder,
                data_sources_to_ingest = data_sources_to_ingest,
                ingest_to = ingest_to,
                ingestion_date = ingestion_date,
                logs_file_path = logs_file_path
                )],
            detailed_summary=True)

        logger.info(execution_summary.summary_text)

        if config['is_execution_summary_via_slack_enabled'] and (
            len(execution_summary.worker._scheduled_tasks) > 1 or
            not execution_summary.worker.run_succeeded):
                send_execution_summary(execution_summary, data_sources_to_ingest, ingest_to, ingestion_date)

    except Exception as e:
        message = ":rage4: \n EOL Mailbox Hosting ETL : Unexpected failure \nError:\n{}\n{}"\
            .format(e, traceback.format_exc())
        SlackNotifier().notify_with_message(message)
        logger = LoggerFactory.create_logger_for_multiprocess_use(logs_file_path, logging.ERROR, 'main-crashed')
        logger.fatal(e, exc_info=True)