import time
import pytz
from datetime import datetime, timedelta
import random

class TimestampedDataDTO():
    # In case data_value cant be indexed, it will force the indexer to use str_for_index as base for index
    def __init__(self, source_id, timestamp, data_name, data_value, str_for_index=None):
        self.source_id = source_id
        self.timestamp = timestamp
        self.data_name = data_name
        self.data_value = data_value            # Will be put as data payload
        self.str_for_index = str_for_index      # Will be used as base for index if set

    @staticmethod
    def generate_source_id(namespace, uid):
        return namespace + '.' + uid

    def get_row_key_for_latest(self):
        return self.source_id

    def get_row_key_for_hourly(self):
        time_part = self.timestamp_as_utc().strftime('%Y%m%d%H')
        return str(self.source_id+'-'+self.data_name+'-'+time_part)

    # A nice hash for the data would be better
    def get_row_key_for_blob_data(self):
        time_part = str(self.timestamp_as_unix_time_millis(self.timestamp_as_utc()))
        return str(self.source_id+'-'+self.data_name+'-'+time_part)

    def timestamp_as_utc(self):
        if self.timestamp.tzinfo:
            return self.timestamp.astimezone (pytz.utc)
        else:
            return self.timestamp

    def timestamp_as_unix_time_millis(self, dt=None):
        if not dt:
            dt = self.timestamp_as_utc()
        return long(time.mktime(dt.timetuple())*1e3 + dt.microsecond/1e3)

    def __unicode__(self):
        return u'%s from %s : %s=%s' % (self.timestamp, self.source_id, self.data_name, self.data_value)

class BlobIndexDTO():
    def __init__(self, source_id, data_name, free_text, timestamp, blob_data_row_key):
        self.source_id = source_id
        self.data_name = data_name
        self.free_text = free_text
        self.timestamp = timestamp
        self.blob_data_row_key = blob_data_row_key

    def get_row_key(self):
        u = u'%s-%s-%s' % (self.source_id, self.data_name, self.free_text.decode('utf-8'))
        return u.encode('utf-8')

    def timestamp_as_utc(self):
        if self.timestamp.tzinfo:
            return self.timestamp.astimezone (pytz.utc)
        else:
            return self.timestamp

    def __unicode__(self):
        return u'%s => %s' % (self.get_row_key(), self.blob_data_row_key)
