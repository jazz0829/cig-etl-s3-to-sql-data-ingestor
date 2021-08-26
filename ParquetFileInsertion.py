
from datetime import datetime
import json

import luigi
from luigi.contrib import sqla
import urllib
import sqlalchemy

from CustomMarkerTable import SQLAlchemyWithCigMarkerTarget
from CigEolHostingIngestionLogic import EolHostingIngestionService

import logging
from LoggerFactory import LoggerFactory

# Parent class: https://github.com/spotify/luigi/blob/eb1a2f3a52d84979d9435510b83d975f42c37405/luigi/contrib/sqla.py
# Here we need to just override few methods and properties
class IngestParquetFile(sqla.CopyToTable):
    schema = "raw"
    reflect = True
    retry_count = 3

    ingestion_date = luigi.DateParameter()
    backup_date = luigi.DateParameter()
    environment = luigi.Parameter()
    parquet_source = luigi.Parameter()
    parquet_source_location = luigi.Parameter()
    table = luigi.Parameter()
    logs_file_path = luigi.Parameter()
    # ! Important: order of columns is crucial! Must match the order of columns of table in SQL
    column_names = luigi.ListParameter()

    ingest_to = luigi.Parameter()

    # Implemented via property due to the specifics of sqla.CopyToTable
    @property
    def connection_string(self):
        return self.ingest_to

    def __init__(self, *args, **kwargs):
        super(IngestParquetFile, self).__init__(*args, **kwargs)
        columns = [([c, sqlalchemy.String()], {}) for c in self.column_names]

    def rows(self):
        result_dataframe = EolHostingIngestionService(self.table_bound)\
            .get_cig_dataframe_from_parquet_file(
                self.parquet_source_location,
                self.environment,
                self.ingestion_date)
        result_dataframe = result_dataframe[list(self.column_names)]
        for row in result_dataframe.iterrows():
            yield row[1]

    def run(self):
        # This way logger works from multiple threads and processes
        self._logger = LoggerFactory.create_logger_for_multiprocess_use(self.logs_file_path, logging.ERROR, 'luigi-interface')
        super(IngestParquetFile, self).run()

    # Overriden parent method. With only one change for fixing None values.
    def copy(self, conn, ins_rows, table_bound):
        bound_cols = dict((c, sqlalchemy.bindparam("_" + c.key)) for c in table_bound.columns)
        ins = table_bound.insert().values(bound_cols)  
        # The fix:
        cleaned_rows_to_insert = self.fix_none_values(ins_rows)
        conn.execute(ins, cleaned_rows_to_insert)
        
    # ! Important: so far it is the most straight forward way to fix the issue of inserting nullable guids
    def fix_none_values(self, rows):
        cleaned_rows = []
        for row in rows:
            cleaned_row = row
            for key, value in row.items():
                cleaned_row[key] = value if value != "None" else None
            cleaned_rows.append(cleaned_row)
        return cleaned_rows

    # Here we specify our custom marker table to be used
    def output(self):
        return SQLAlchemyWithCigMarkerTarget(
            environment = self.environment,
            parquet_source = self.parquet_source,
            date = self.backup_date,

            connection_string=self.connection_string,
            target_table=self.table,
            update_id=self.update_id(),
            connect_args=self.connect_args,
            echo=self.echo)