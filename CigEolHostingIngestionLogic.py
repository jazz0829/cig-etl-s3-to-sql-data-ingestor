import pandas
import pyarrow.parquet as pq
from datetime import datetime
import numpy as np

class EolHostingIngestionService():
    def __init__(self, table_bound):
      self.table_bound = table_bound

    def get_cig_dataframe_from_parquet_file(self, parquet_source_location, environment, ingestion_date):
        dataset = pq.ParquetDataset(parquet_source_location)
        table = dataset.read()

        df = table.to_pandas()

        if environment.length > 2:
            df['Environment'] = environment.split('_')[0]
        else:
            df['Environment'] = environment

        df['CIGCopyTime'] = ingestion_date.strftime("%Y-%m-%d")
        df['CIGProcessed'] = "0"

        # This syntax replaces only the cell with values equals to "NaT" or "nan". 
        # Bigger strings (like "nanarnia" or "NaTali") are not getting affected
        df = df.replace("NaT", "None")
        df = df.replace("nan", "None")

        df = df.replace("True", "1")
        df = df.replace("False", "0")

        # The order of calls here is important
        df = self.default_missing_columns(df)
        df = self.handle_nullable_ints(df)
        df = self.handle_nullable_bigints(df)
        df = self.handle_not_nullable_columns(df)
        df = self.handle_dates_too_long_milliseconds(df)
        df = self.handle_nvarchar_max_columns(df)
        # TODO: Remove when switch is made and Geolocation and Logo columns are deleted from DB
        df = self.handle_odd_columns(df)

        return df

    def handle_nvarchar_max_columns(self, df):
        ### Addresses Error: 
        ### [HY000] [Microsoft][ODBC SQL Server Driver]
        ### Warning: Partial insert/update. The insert/update of a text or image column(s) did not succeed. (0) (SQLPutData); 
        ### The text, ntext, or image pointer value conflicts with the column name specified. (7125)')
        nvarxhar_max_columns = []
        for c in self.table_bound.columns:
            try:
                if c.type.python_type == str and c.type.length is None:
                    nvarxhar_max_columns.append(c.name)
            except:
                pass
        nvarxhar_max_limit = 100000
        for column in nvarxhar_max_columns:
           df[column] = df[column].str[:nvarxhar_max_limit]
        return df

    def handle_nullable_ints(self, df):
        int_columns = []
        for c in self.table_bound.columns:
            try:
                if c.type.python_type == int and c.nullable:
                    int_columns.append(c.name)
            except:
                pass
        
        for column in int_columns:
            df[column] = df[column].apply(
                lambda x : x.replace('.0', '') if x.endswith('.0') else x)
        return df

    def handle_nullable_bigints(self, df):
        ### To deal with values like: 1.801439850948301e+16
        int_columns = []
        for c in self.table_bound.columns:
            try:
                if c.type.python_type == int and c.nullable:
                    int_columns.append(c.name)
            except:
                pass

        for column in int_columns:
            if (df[column].str.contains('e-', regex=False)).sum() > 0 or (df[column].str.contains('e+', regex=False)).sum() > 0:
                df[column] = df[column].apply(lambda x : np.nan if x == "None" else x)
                df[column] = df[column].apply(float)
                df[column] = df[column].replace(np.nan, "None")
        return df

    def handle_dates_too_long_milliseconds(self, df):
        date_columns = []
        for c in self.table_bound.columns:
            try:
                if c.type.python_type == datetime:
                    date_columns.append(c.name)
            except:
                pass
        for column in date_columns:
            max_length = (df[column].map(str).apply(len)).max()
            if max_length > 23:
                df[column] = df[column].str[:23]
        return df

    def handle_not_nullable_columns(self, df):
        not_nullable_columns = [c.name for c in self.table_bound.columns if not c.nullable]
        for column in not_nullable_columns:
            if not column in df.columns:
                df[column] = ''
            df[column] = df[column].str.replace("None", '')
        return df

    def default_missing_columns(self, df):
        missing_columns = [c.name for c in self.table_bound.columns if not c.name in df.columns]
        for column in missing_columns:
            df[column] = "None"
        return df

    def handle_odd_columns(self, df):
        if "Geolocation" in df.columns:
            df["Geolocation"] = "POINT (0 0)"
            #df["Geolocation"] = np.where(df["Geolocation"] != "None", df["Geolocation"], "POINT (0 0)")
        if "Logo" in df.columns:
            df["Logo"] = "None"
        if "Picture" in df.columns:
            df["Picture"] = "None"
        return df