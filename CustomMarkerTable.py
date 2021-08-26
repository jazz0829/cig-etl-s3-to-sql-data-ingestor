from datetime import datetime

import luigi
from luigi.contrib import sqla
import sqlalchemy

#
# Overriden for marker table customization
#
class SQLAlchemyWithCigMarkerTarget(sqla.SQLAlchemyTarget):
    def __init__(self, parquet_source, environment, date, connection_string, target_table, update_id, echo=False, connect_args=None):
        super().__init__(connection_string, target_table, update_id, echo, connect_args)
        self.parquet_source=parquet_source
        self.environment=environment
        self.backup_date=date

    def touch(self):
        """
        Mark this update as complete.
        """
        if self.marker_table_bound is None:
            self.create_marker_table()

        table = self.marker_table_bound
        id_exists = self.exists()
        with self.engine.begin() as conn:
            if not id_exists:
                ins = table.insert().values(
                    ParquetSource=self.parquet_source,
                    Environment=self.environment,
                    BackupDate=self.backup_date,
                    TargetTable=self.target_table,
                    InsertedDate=datetime.now())
            else:
                ins = table.update().where(sqlalchemy.and_(
                        table.c.ParquetSource == self.parquet_source,
                        table.c.Environment == self.environment,
                        table.c.TargetTable == self.target_table)).\
                    values(ParquetSource=self.parquet_source,
                            Environment=self.environment,
                            BackupDate=self.backup_date,
                            TargetTable=self.target_table,
                            InsertedDate=datetime.now())
            conn.execute(ins)
        assert self.exists()

    def exists(self):
        row = None
        if self.marker_table_bound is None:
            self.create_marker_table()
        with self.engine.begin() as conn:
            table = self.marker_table_bound
            s = sqlalchemy.select([table])\
                .where(sqlalchemy.and_(
                    table.c.ParquetSource == self.parquet_source,
                    table.c.Environment == self.environment,
                    table.c.TargetTable == self.target_table)).limit(1)
            row = conn.execute(s).fetchone()
        return row is not None

    def create_marker_table(self):
        """
        Create marker table if it doesn't exist.
        Using a separate connection since the transaction might have to be reset.
        """
        if self.marker_table is None:
            self.marker_table = luigi.configuration.get_config().get('sqlalchemy', 'marker-table', 'table_updates')

        engine = self.engine

        with engine.begin() as con:
            metadata = sqlalchemy.MetaData()
            if not con.dialect.has_table(con, self.marker_table):
                self.marker_table_bound = sqlalchemy.Table(
                    self.marker_table, metadata,
                    sqlalchemy.Column("ParquetSource", sqlalchemy.String(128), primary_key=True),
                    sqlalchemy.Column("TargetTable", sqlalchemy.String(128)),
                    sqlalchemy.Column("Environment", sqlalchemy.String(128)),
                    sqlalchemy.Column("BackupDate", sqlalchemy.DateTime),
                    sqlalchemy.Column("InsertedDate", sqlalchemy.DateTime, default=datetime.now()))
                metadata.create_all(engine)
            else:
                metadata.reflect(only=[self.marker_table], bind=engine)
                self.marker_table_bound = metadata.tables[self.marker_table]