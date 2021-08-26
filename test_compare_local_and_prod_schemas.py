from approvaltests.approvals import verify_file
from approvaltests.reporters.generic_diff_reporter_factory import GenericDiffReporterFactory
from approvaltests.file_approver import FileApprover
from approvaltests.string_writer import StringWriter
import unittest
import urllib
import sqlalchemy
import tempfile

class CompareTableSchemas(unittest.TestCase):
    def test_local_and_prod_schemas_match(self):
        # Arrange
        table_name = "DivisionStatistics"
        local_db = ".\SQLEXPRESS"
        prod_db = "NLC1PRODCI01"
        # Act
        local_schema = self.get_columns(local_db, table_name)
        prod_schema = self.get_columns(prod_db, table_name)
        # Assret

        reporter = GenericDiffReporterFactory().get_first_working()
        approver = FileApprover()
        writer = StringWriter("\n".join(prod_schema))
        expected_file = tempfile.gettempdir() + '\PROD - ' + table_name
        writer.write_received_file(expected_file)
        
        writer = StringWriter("\n".join(local_schema))
        actual_file = tempfile.gettempdir() + '\Local - ' + table_name
        writer.write_received_file(actual_file)

        approver.verify_files(expected_file, actual_file, reporter)
        # Test completed

    def get_columns(self, db_name, table_name):
        connection_string = "Driver={SQL Server};Server={"+db_name+"};Database=CustomerIntelligence;UID=;PWD="
        connection_string = urllib.parse.quote_plus(connection_string) 
        connection_string = "mssql+pyodbc:///?odbc_connect=%s" % connection_string

        cig_host_prefix = 'HOST_CIG_'
        table_name = cig_host_prefix + table_name
        schema = 'raw'
        column_names = []
        engine = sqlalchemy.create_engine(connection_string, echo = False)
        with engine.begin() as con:
            inspector = sqlalchemy.inspect(engine)        
            columns = inspector.get_columns(table_name  = table_name, schema = schema)
            column_names = [c['name'] for c in columns]
        return column_names

if __name__ == "__main__":
    unittest.main()