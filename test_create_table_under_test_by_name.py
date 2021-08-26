import sqlalchemy
import urllib
import json

if __name__ == '__main__':
    connection_string = "Driver={SQL Server};Server=NLC1PRODCI01;Database=CustomerIntelligence;UID=;PWD="
    connection_string = urllib.parse.quote_plus(connection_string) 
    connection_string = "mssql+pyodbc:///?odbc_connect=%s" % connection_string

    cig_host_prefix = 'HOST_CIG_'
    table_name = cig_host_prefix + 'DivisionStatistics'
    schema = 'raw'
    result_file_name = 'table_under_test.json'

    print('Inspecting', table_name)
    engine = sqlalchemy.create_engine(connection_string, echo = True)
    with engine.begin() as con:
        inspector = sqlalchemy.inspect(engine)
        columns = inspector.get_columns(table_name = table_name, schema = schema)
        column_names = [c['name'] for c in columns]
        s3_name = table_name[len(cig_host_prefix):]
        table_config = [{
            "target_name": table_name,
            "source": s3_name,
            "is_enabled": True,
            "columns": column_names
        }]

        with open(result_file_name, 'w') as outfile:
            json.dump(table_config, outfile, indent=2)
    print(result_file_name, "is successfully updated")
