import logging
import os


class LoggerFactory:
    def create_logger_for_multiprocess_use(logs_file_name, log_level = logging.DEBUG, logger_id = None):
        ''' Be careful with creating loggers of the same id within one process\n
            Creating multiple loggers of the same id in one process using this method can cause adding of multiple log handlers to single logger.\n
            Which results in logs duplication and sometimes in multithreading errors from Python'''
        if logs_file_name is None:
            raise Exception("Please specify logs_file_name")
        
        logs_folder = os.path.dirname(os.path.abspath(logs_file_name))
        if not os.path.exists(logs_folder):
            os.makedirs(logs_folder)

        logger = logging.getLogger(logger_id)
        logger.setLevel(log_level)
        formatter = logging.Formatter("%(asctime)s.%(msecs)03d %(name)-12s %(levelname)-8s [thread %(thread)d] [pid %(process)d] %(message)s")
        file_handler = logging.FileHandler(logs_file_name)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        return logger