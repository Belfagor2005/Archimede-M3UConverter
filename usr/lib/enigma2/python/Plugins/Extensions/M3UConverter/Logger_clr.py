# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function

"""
#########################################################
#                                                       #
#  Archimede Universal Converter Plugin                 #
#  Version: 1.7                                         #
#  Created by Lululla (https://github.com/Belfagor2005) #
#  License: CC BY-NC-SA 4.0                             #
#  https://creativecommons.org/licenses/by-nc-sa/4.0    #
#  Last Modified: "21:50 - 20250527"                    #
#                                                       #
#  Credits:                                             #
#  - Original concept by Lululla                        #
#  Usage of this code without proper attribution        #
#  is strictly prohibited.                              #
#  For modifications and redistribution,                #
#  please maintain this credit header.                  #
#########################################################
"""
__author__ = "Lululla"

from os import remove
from os.path import join
from threading import Lock
from time import strftime
import logging
from logging.handlers import RotatingFileHandler

# add lululla for debug
class ColoredLogger:
    LEVELS = {
        "DEBUG": ("\033[92m", "[DEBUG]"),       # green
        "INFO": ("\033[97m", "[INFO] "),        # white
        "WARNING": ("\033[93m", "[WARN] "),     # yellow
        "ERROR": ("\033[91m", "[ERROR]"),       # red
    }
    END = "\033[0m"
    _instance = None
    _lock = Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, log_file=None, clear_on_start=True):
        if not hasattr(self, '_initialized'):
            self.log_file = log_file
            if self.log_file and clear_on_start:
                try:
                    remove(self.log_file)
                except Exception:
                    pass
            self._initialized = True

    def log(self, level, message):
        """Base logging method"""
        prefix, label = self.LEVELS.get(level.upper(), ("", "[LOG] "))
        timestamp = strftime("%Y-%m-%d %H:%M:%S")

        # Console output with colors
        console_msg = f"{timestamp} {label} {prefix}{message}{self.END}"
        print(console_msg)

        # File output without colors
        if self.log_file:
            try:
                with open(self.log_file, "a") as f:
                    f.write(f"{timestamp} {label} {message}\n")
            except Exception:
                print(f"{timestamp} [ERROR] Failed to write to log file")

    # Standard logging methods
    def debug(self, message, *args):
        self.log("DEBUG", message % args if args else message)

    def info(self, message, *args):  # <-- METODO AGGIUNTO
        self.log("INFO", message % args if args else message)

    def warning(self, message, *args):
        self.log("WARNING", message % args if args else message)

    def error(self, message, *args):
        self.log("ERROR", message % args if args else message)

    def critical(self, message, *args):
        self.log("ERROR", "CRITICAL: " + (message % args if args else message))

    def exception(self, message, *args):
        exc_info = self._get_exception_info()
        self.log("ERROR", f"EXCEPTION: {message % args if args else message}\n{exc_info}")

    def _get_exception_info(self):
        """Get formatted exception info"""
        import sys
        import traceback
        exc_type, exc_value, exc_traceback = sys.exc_info()
        return ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))


# Global singleton Instance
logger = ColoredLogger(log_file=join("/tmp", "archimede_converter", "m3u_converter.log"))

# Test
# if __name__ == "__main__":
    # logger.debug("Test debug message")
    # logger.info("Test info message")
    # logger.warning("Test warning message")
    # logger.error("Test error message")
    # try:
        # 1 / 0
    # except Exception as e:
        # logger.exception(f"Divisione per zero {str(e)}")

# print(f"Logger type: {type(logger)}")  # Debug
# print(f"Has debug method: {hasattr(logger, 'debug')}")  # Debug
# logger.log("INFO", f"START PLUGIN {str(title_plug)}")
