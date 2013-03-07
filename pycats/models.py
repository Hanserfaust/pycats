import time
import pytz

class TimestampedDataDTO():
    def __init__(self, source_id, timestamp, data_name, data_value):
        self.source_id = source_id
        self.timestamp = timestamp
        self.data_name = data_name
        self.data_value = data_value

    def get_pycassa_hourly_insert_tuple(self, dto):
        row_key = self.get_row_key_for_hourly()
        col_name = self.timestamp_as_utc()
        col_value = self.data_value
        return (row_key, {col_name : col_value})

    def get_pycassa_blob_insert_tuple(self, dto):
        row_key = self.get_row_key_for_blob_data()
        col_name = self.timestamp_as_utc()
        col_value = self.data_value
        return (row_key, {col_name : col_value})

    def get_row_key_for_hourly(self):
        time_part = self.timestamp_as_utc().strftime('%Y%m%d%H')
        return str(self.source_id+'-'+self.data_name+'-'+time_part)

    # No need for a busness-key. a hash of the data could work just as well (better?)
    def get_row_key_for_blob_data(self):
        time_part = str(self.timestamp_as_unix_time_millis(self.timestamp_as_utc()))
        return str(self.source_id+'-'+self.data_name+'-'+time_part)

    def timestamp_as_utc(self):
        if self.timestamp.tzinfo:
            return self.timestamp.astimezone (pytz.utc)
        else:
            return self.timestamp

    def timestamp_as_unix_time_millis(self, dt):
        return long(time.mktime(dt.timetuple())*1e3 + dt.microsecond/1e3)

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
