import luigi
import luigi.server

from LoggerFactory import LoggerFactory
from datetime import datetime
import logging
import os

if __name__ == "__main__":
    # Display last run details for several hours on UI
    hour = 3600
    scheduler = luigi.scheduler.Scheduler(remove_delay = 16 * hour)

    now = datetime.now()
    logs_folder = "C:\\Logs\\etl-s3-to-sql-prod"
    if not os.path.exists(logs_folder):
        os.makedirs(logs_folder)
    logs_file_path = "{}\luigi.server.{}.log".format(logs_folder, now.strftime('%Y-%m'))
    # Just initialization of loggers is enough here. They will be picked up by Luigi automatically via logger names
    LoggerFactory.create_logger_for_multiprocess_use(logs_file_path, logging.INFO, 'luigi.server')
    LoggerFactory.create_logger_for_multiprocess_use(logs_file_path, logging.INFO, 'luigi.scheduler')
    
    server = luigi.server.run(scheduler=scheduler)