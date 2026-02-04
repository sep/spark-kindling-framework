import logging
import re
import traceback
from datetime import datetime

from kindling.injection import *
from kindling.spark_config import *
from kindling.spark_session import *


class SparkLogger:
    def __init__(self, name: str, baselogger=None, session=None, config=None):
        self.name = name
        self.pattern = "%p: (%c) %m [trace_id=%x{trace_id} span_id=%x{span_id} component=%x{component} operation=%x{operation}]"
        self.spark = session or get_or_create_spark_session()
        self.config = config or {}
        self.log_level = self.config.get("log_level", "").lower()

        try:
            baselogger = baselogger or self.spark._jvm.org.apache.log4j.LogManager.getLogger(name)
        except:
            baselogger = None

        self.logger = baselogger or logging.getLogger(name)

        # Log level hierarchy (lower numbers = higher priority)
        self._level_hierarchy = {"error": 0, "warn": 1, "info": 2, "debug": 3}

        # Cache log level methods for efficiency
        self._log_methods = {
            "debug": self.logger.debug,
            "info": self.logger.info,
            "warn": self.logger.warn,
            "error": self.logger.error,
        }

    def should_print(self):
        return self.config.get("print_logging", False)

    def debug(self, msg: str, exc_info: bool = False):
        if exc_info:
            msg = f"{msg}\n{traceback.format_exc()}"
        self._log("debug", msg)

    def info(self, msg: str, exc_info: bool = False):
        if exc_info:
            msg = f"{msg}\n{traceback.format_exc()}"
        self._log("info", msg)

    def warn(self, msg: str, exc_info: bool = False):
        if exc_info:
            msg = f"{msg}\n{traceback.format_exc()}"
        self._log("warn", msg)

    def warning(self, msg: str, exc_info: bool = False):
        if exc_info:
            msg = f"{msg}\n{traceback.format_exc()}"
        self._log("warn", msg)

    def error(self, msg: str, exc_info: bool = False):
        if exc_info:
            msg = f"{msg}\n{traceback.format_exc()}"
        self._log("error", msg)

    def _is_level_enabled(self, level: str) -> bool:
        """Check if the given log level should be logged based on current logger level"""
        # Get the effective level (handles inheritance from parent loggers)
        current_level_str = "info"

        if self.log_level != "":
            current_level_str = self.log_level
        else:
            current_level = self.logger.getEffectiveLevel()
            current_level_str = str(current_level).lower()

        # If current level is not in our hierarchy, default to allowing all
        if current_level_str not in self._level_hierarchy:
            return True

        return self._level_hierarchy[level] <= self._level_hierarchy[current_level_str]

    def _log(self, level: str, msg: str):
        """Central logging method that respects log levels and eliminates redundancy"""
        if self._is_level_enabled(level):
            formatted_msg = self._format_msg(msg, level)
            self._log_methods[level](formatted_msg)

            if self.should_print():
                print(formatted_msg)

    def with_pattern(self, pattern: str):
        self.pattern = (
            pattern
            + "%ntrace_id=%x{trace_id} span_id=%x{span_id} component=%x{component} operation=%x{operation}"
        )
        return self

    def _format_msg(self, msg: str, level: str) -> str:
        """
        Format a log message according to the specified pattern

        Supported patterns:
        %d{format} - Date (uses datetime strftime format)
        %p - Log level
        %c - Logger name
        %c{n} - Logger name truncated to last n components
        %F - File name
        %L - Line number
        %M - Method name
        %m - Message
        %n - Newline
        %t - Thread name
        %x{key} - MDC value for key
        """
        result = self.pattern

        # Handle date/time
        date_matches = re.findall(r"%d(?:{([^}]+)})?", result)
        if date_matches:  # Only compute datetime if needed
            now = datetime.now()
            for date_format in date_matches:
                if date_format:
                    date_str = now.strftime(date_format)[:-3]
                    result = result.replace(f"%d{{{date_format}}}", date_str)
                else:
                    date_str = now.isoformat()[:-3]
                    result = result.replace("%d", date_str)

        # Handle logger name with optional truncation
        logger_matches = re.findall(r"%c(?:{(\d+)})?", result)
        for num_components in logger_matches:
            if num_components:
                components = self.name.split(".")
                truncated = components[-int(num_components) :]
                name = ".".join(truncated)
                result = result.replace(f"%c{{{num_components}}}", name)
            else:
                result = result.replace("%c", self.name)

        # Handle MDC values
        mdc_matches = re.findall(r"%x{([^}]+)}", result)
        for mdc_key in mdc_matches:
            mdc_value = None
            try:
                mdc_value = self.spark.sparkContext.getLocalProperty("mdc." + mdc_key)
            except:
                mdc_value = None

            result = result.replace(f"%x{{{mdc_key}}}", mdc_value or "n/a")

        # Handle simple replacements
        replacements = {"%p": level.upper(), "%m": msg, "%n": "\n"}

        for pattern, value in replacements.items():
            result = result.replace(pattern, value)

        return result
