# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function

"""
#########################################################
#                                                       #
#  Archimede Universal Converter Plugin                 #
#  Version: 1.1                                         #
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
# import logging
# from logging.handlers import RotatingFileHandler


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

    def __init__(self, log_file=None, clear_on_start=True, secondary_log=None):  # Aggiunto secondary_log
        if not hasattr(self, '_initialized'):
            self.log_file = log_file
            self.secondary_log = secondary_log  # Aggiungi questa linea
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
        log_message = f"{timestamp} {label} {message}"

        print(f"{timestamp} {label} {prefix}{message}{self.END}")

        if self.log_file:
            self._write_to_file(self.log_file, log_message)
        
        if hasattr(self, 'secondary_log') and self.secondary_log:
            self._write_to_file(self.secondary_log, log_message)

    def show_message(self, text, timeout=5):
        from Screens.MessageBox import MessageBox
        self.session.openWithCallback(
            self.log_message_closed,
            MessageBox,
            text=text,
            type=MessageBox.TYPE_INFO,
            timeout=timeout
        )
    
    def log_message_closed(self, ret=None):
        self.log("DEBUG", "MessageBox closed")

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

    def _write_to_file(self, filepath, message):
        """Helper method per scrivere su file (versione ottimizzata)"""
        try:
            from functools import partial
            from threading import Timer
            import os
            
            timeout = 2
            
            def write_operation(f, msg):
                try:
                    f.write(msg + "\n")
                    f.flush()
                    os.fsync(f.fileno())
                except Exception as e:
                    raise IOError(f"Write failed: {str(e)}")
            
            try:
                with open(filepath, "a") as f:
                    timer = Timer(timeout, partial(os._exit, 1))
                    timer.start()
                    try:
                        write_operation(f, message)
                    finally:
                        timer.cancel()
            except Exception as e:
                print(f"[LOG CRITICAL] File write timeout to {filepath} Error {str(e)}")

        except Exception as e:
            print(f"[LOG ERROR] File access error: {str(e)}")

    def rotate_logs(self):
        """Perform log rotation"""
        try:
            import os
            from glob import glob
            
            for log_path in [self.log_file, self.secondary_log]:
                if not log_path:
                    continue
                    
                if os.path.exists(log_path) and os.path.getsize(log_path) > 1 * 1024 * 1024:
                    base, ext = os.path.splitext(log_path)
                    backups = sorted(glob(f"{base}.*{ext}"), reverse=True)
                    
                    for old in backups[2:]:
                        os.remove(old)
                    
                    for i in range(min(len(backups), 2), 0, -1):
                        os.rename(
                            f"{base}.{i - 1}{ext}" if i > 1 else log_path,
                            f"{base}.{i}{ext}"
                        )
        except Exception as e:
            self.log("ERROR", f"Log rotation failed: {str(e)}")


# Global singleton Instance
logger = ColoredLogger(log_file=join("/tmp", "archimede_converter", "m3u_converter.log"))

# Test
if __name__ == "__main__":
    print("\n" + "=" * 50)
    print("FULL COLOREDLOGGER TEST".center(50))
    print("=" * 50 + "\n")
    
    # Basic configuration test
    print("[CONFIG] Logger initialized with:")
    print(" - Main file: %s" % logger.log_file)
    print(" - Secondary file: %s" % getattr(logger, "secondary_log", "None"))
    print("-" * 50 + "\n")
    
    # Log level test
    print("[TEST] Log level verification:")
    logger.debug("Debug message (lowest level)")
    logger.info("Informational message")
    logger.warning("Warning message")
    logger.error("Error message")
    logger.critical("CRITICAL message")
    
    # Exception test
    print("\n[TEST] Exception logging verification:")
    try:
        1 / 0
    except Exception as e:
        logger.exception("Caught exception: Division by zero - %s" % str(e))
    
    # File writing test
    print("\n[TEST] Log file writing verification:")
    import os
    if logger.log_file and os.path.exists(logger.log_file):
        print("Main log content (last 5 lines):")
        with open(logger.log_file, "r") as f:
            lines = f.readlines()[-5:]
            print("".join(lines).strip())
    
    if hasattr(logger, "secondary_log") and logger.secondary_log and os.path.exists(logger.secondary_log):
        print("\nSecondary log content (last 5 lines):")
        with open(logger.secondary_log, "r") as f:
            lines = f.readlines()[-5:]
            print("".join(lines).strip())
    
    # Log rotation test
    print("\n[TEST] Log rotation verification:")
    try:
        print("Forcing log rotation...")
        logger.rotate_logs()
        print("Rotation completed successfully")
    except Exception as e:
        logger.error("Error during rotation: %s" % str(e))
    
    # Available methods check
    print("\n[TEST] Logger interface verification:")
    required_methods = ["debug", "info", "warning", "error", "critical", "exception"]
    missing = [m for m in required_methods if not hasattr(logger, m)]
    if not missing:
        print("✔ All required methods are present")
    else:
        print("✖ Missing methods: %s" % ", ".join(missing))
    
    print("\n" + "=" * 50)
    print("TEST COMPLETED".center(50))
    print("=" * 50 + "\n")
    
    # Additional test for title_plug (only if it exists)
    if "title_plug" in globals():
        logger.log("INFO", "PLUGIN TEST: ColoredLogger")
