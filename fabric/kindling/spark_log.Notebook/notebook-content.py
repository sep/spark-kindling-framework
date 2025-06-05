# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

import re
from datetime import datetime
 
notebook_import(".spark_session")
 
class SparkLogger:
    def __init__(self, name: str):
        spark = get_or_create_spark_session()
        self.name = name
        self.logger = spark._jvm.org.apache.log4j.LogManager.getLogger(name)
        self.pattern = "%m%ntrace_id=%x{trace_id} span_id=%x{span_id} component=%x{component} operation=%x{operation}"
        self.spark = spark

    def debug(self, msg: str):
        print(f"Logger: {self._format_msg(msg, 'debug')}")
        self.logger.debug(self._format_msg(msg, "debug"))
        print(self._format_msg(msg, "debug"))
    
    def info(self, msg: str):
        print(f"Logger: {self._format_msg(msg, 'info')}")
        self.logger.info(self._format_msg(msg, "info"))
        print(self._format_msg(msg, "info"))

    def warn(self, msg: str):
        self.logger.warn(self._format_msg(msg, "warn"))
        print(self._format_msg(msg, "warn"))
    
    def error(self, msg: str):
        self.logger.error(self._format_msg(msg, "error")) 
        print(self._format_msg(msg, "error"))
    
    def with_pattern(self, pattern: str):
        self.pattern = pattern + "%ntrace_id=%x{trace_id} span_id=%x{span_id} component=%x{component} operation=%x{operation}"
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
        date_matches = re.findall(r'%d(?:{([^}]+)})?', result)
        now = datetime.now()
        for date_format in date_matches:
            if date_format:
                date_str = now.strftime(date_format)[:-3]
                result = result.replace(f'%d{{{date_format}}}', date_str)
            else:
                date_str = now.isoformat()[:-3]
                result = result.replace('%d', date_str)
                
        # Handle logger name with optional truncation
        logger_matches = re.findall(r'%c(?:{(\d+)})?', result)
        for num_components in logger_matches:
            if num_components:
                components = self.name.split('.')
                truncated = components[-int(num_components):]
                name = '.'.join(truncated)
                result = result.replace(f'%c{{{num_components}}}', name)
            else:
                result = result.replace('%c', self.name)
                
        # Handle MDC values 
        mdc_matches = re.findall(r'%x{([^}]+)}', result)
        for mdc_key in mdc_matches:
            mdc_value = self.spark.sparkContext.getLocalProperty("mdc." + mdc_key)  
            if mdc_value:
                result = result.replace(f'%x{{{mdc_key}}}', mdc_value)
            else:
                result = result.replace(f'%x{{{mdc_key}}}', "n/a")
        
        # Handle simple replacements
        replacements = {
            '%p': level.upper(),
            '%m': msg,
            '%n': '\n'
        }
        
        for pattern, value in replacements.items():
            result = result.replace(pattern, value)

        return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
