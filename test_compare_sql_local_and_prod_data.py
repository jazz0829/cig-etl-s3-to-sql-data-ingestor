from approvaltests.approvals import verify_file
from approvaltests.reporters.generic_diff_reporter_factory import GenericDiffReporterFactory
from approvaltests.file_approver import FileApprover
import unittest
import tempfile
import pyodbc
import pandas
import os
import json

class CompareTablesData(unittest.TestCase):
    def test_local_and_prod_schemas_match(self):
        # Arrange
        local_db = ".\SQLEXPRESS"
        prod_db = "NLC1PRODCI01"
        ingestion_date = "2019-07-03"

        tables = self.read_tables_configuration("table_under_test.json")
        # Act
        for i, table in enumerate(tables):
            table_name = table['target_name']
            print(i, table_name)

            columns_to_select = table['columns']
            columns_to_skip = ['CIGCopyTime', 'CIGProcessed']
            for c in columns_to_skip:
                columns_to_select.remove(c)
            
            columns_to_select = ",".join(columns_to_select)
            
            # columns_to_select = "ID, sysmodified, syscreated"
            query = "SELECT {} FROM raw.{} WHERE CAST(sysmodified AS date) = CAST('{}' AS date) AND UPPER(Environment)='NL' Order By sysmodified, syscreated"\
                .format(columns_to_select, table_name, ingestion_date)

            # query = "SELECT DISTINCT ID FROM raw.{} WHERE CAST(sysmodified AS date) = CAST('{}' AS date) AND UPPER(Environment)='NL' Order By ID"\
            #     .format(table_name, ingestion_date)

            # query = "SELECT DISTINCT ID FROM raw.{} WHERE UPPER(Environment)='NL' Order By ID"\
            #     .format(table_name)

            # query = "SELECT {} FROM raw.{} WHERE UPPER(Environment)='NL' Order By ID"\
            #     .format(columns_to_select, table_name)
            
            prod_df = self.execute_sql_query(prod_db, query)
            local_df = self.execute_sql_query(local_db, query)
        # Assert
            print('Columns assertion...')
            self.assert_data_frames(local_df, prod_df, table_name)
        # Test completed

    def read_tables_configuration(self, config_file_path):
        if config_file_path is None or not os.path.isfile(config_file_path):
            raise FileNotFoundError(config_file_path)
        tables = pandas.read_json(config_file_path)
        return json.loads(tables.to_json(orient='records'))

    def execute_sql_query(self, db_name, sql_query):
        print('Establishing SQL connection')
        sql_conn = pyodbc.connect('Driver={SQL Server};'
                        'Server={'+db_name+'};'
                        'Database=CustomerIntelligence;'
                        'Trusted_Connection=yes;')
        print('Querying SQL...')
        df = pandas.read_sql(sql_query, sql_conn)
        sql_conn.close()
        print('SQL connection closed')
        return df

    def assert_data_frames(self, local_data, expected_prod_data, table_name):
        if len(expected_prod_data.index) == 0:
            raise AssertionError("Prod {} had no records".format(table_name))
        if len(local_data.index) == 0:
            raise AssertionError("Local {} had no records".format(table_name))

        reporter = GenericDiffReporterFactory().get_first_working()
        approver = FileApprover()       

        for col in expected_prod_data:
            expected_prod_data[col] = expected_prod_data[col].astype(str)
        for col in local_data:
            local_data[col] = local_data[col].astype(str)

        expected_data_columns = expected_prod_data.apply('\n'.join, axis=0)
        actual_columns = local_data.apply('\n'.join, axis=0)

        for expected_column_name in expected_data_columns._info_axis:
            print(expected_column_name)
            expected_column_values = expected_data_columns[expected_column_name]
            expected_file = "{}\Prod - {} {}".format(tempfile.gettempdir(), table_name, expected_column_name)
            with open(expected_file, 'w', encoding='utf-8') as f:
                f.write(expected_column_values)
            
            actual_column_name = expected_column_name
            actual_column_values = actual_columns[actual_column_name]
            actual_file = "{}\Local - {} {}".format(tempfile.gettempdir(), table_name, actual_column_name)
            with open(actual_file, 'w', encoding='utf-8') as f:
                f.write(actual_column_values)

            approver.verify_files(expected_file, actual_file, reporter)

if __name__ == "__main__":
    unittest.main()