# -*- coding: utf-8 -*-
import pycassa
from pycassa.cassandra.ttypes import NotFoundException
import pytz
import time
from datetime import datetime, timedelta
from logging import debug, error, info
import re
import string
from models import TimestampedDataDTO, BlobIndexDTO
import random
from collections import OrderedDict


MAX_COLUMNS = 60*60*24
CACHE_TTL = 8*60*60 # 8 hours
# Sorry we are not Y10K compatible, just need something surely beyond anything reasonable
MAX_TIME = datetime.strptime('2900-01-01T01:59:59', '%Y-%m-%dT%H:%M:%S')

#
#
# Known limitations:
#    Timestamps as column-names-problem:
#       Since the time series are stored in shards of one hour, the implementation
#       uses the timestamp as column name (yes sort of wierd), and in Cassandra
#       dates are treated as unsigned long (64-bit) number representing the number
#       of milliseconds since "the epoch". We can store 1000 values per second assuming
#       the values don't share timetamp for each row-key. But what if they do? what if the same source
#       want to store several values for the same timestamp. The problem arises when
#       the source provides a timestamp to the data with low precision (ie no milliseconds
#       supplied in the timestamp), in that situation and there may
#       arise the situation where several data-posts share timestamp. In common data
#       storage solution (SQL), the values would be added in order, but in this
#       hourly-shard model employed in this solution, for one row, there can not be
#       two columns with the same name (the timestamp is the column name). If they
#       share name, the last will overwrite the earlier value.
#
#       (Note that the colission risk is only when two posts share the same exact row
#       key, ie the combination of source_id, data_name and hour-shard)
#
#       A solution is to say we only provide second-resolution and uses the millisecond
#       value as a mean to avoid the collision described. For that to work, we
#       would like to make sure two data posts never share the millisecond-figure.
#
#       In a single-deployment system, this can be made quite robust, but in a multi-tier
#       system, this can prove to be a challenge, there is always a risk that the
#       different systems chooses the same millisecond-figure.
#
#       Implement your strategy in the method __add_millis_to_timestamp(), i provide
#       a simple randomizer that does the job in the small to medium scale.
#
#       Other solution could be to not use a date since epoch as column name, but
#       instead calculate nanoseconds since the shard-hour-start. Upon data-load, you
#       would need to convert the each nanosecond-offset to the correct datetime.
#
#       Actually, an hour of pico-seconds fits into a 64-bit long!
#
# See tests.py for details on how to setup the required keyspace and ColumnFamilies
#
####################################################################################
class TimeSeriesCassandraDao():
    #  CREATE COLUMNFAMILY HourlyTimestampedData (KEY ascii PRIMARY KEY) WITH comparator=timestamp;
    HOURLY_DATA_COLUMN_FAMILY_NAME = 'HourlyTimestampedData'

    # CREATE COLUMNFAMILY BlobData (KEY ascii PRIMARY KEY) WITH comparator=timestamp;
    BLOB_DATA_COLUMN_FAMILY_NAME = 'BlobData'

    # CREATE COLUMNFAMILY BlobDataIndex (KEY text PRIMARY KEY) WITH comparator=timestamp AND default_validation=text;
    BLOB_DATA_INDEX_COLUMN_FAMILY_NAME = 'BlobDataIndex'

    # Maybe the dao should be un-aware of the indexer, break out and make it cleaner?
    blob_indexer = None

    def __init__(self, cassandra_hosts, key_space, cache=None, warm_up_cache_shards=0, disable_high_res_column_name_randomization=False):
        self.__cassandra_hosts = cassandra_hosts
        self.__key_space = key_space
        self.__pool = pycassa.ConnectionPool(self.__key_space, self.__cassandra_hosts)
        self.cache = cache
        self.cache_hits = 0
        self.daily_gets = 0
        self.millis = 0
        self.__warm_up_cache_shards = warm_up_cache_shards
        self.blob_indexer = StringIndexer()
        self.disable_high_res_column_name_randomization = disable_high_res_column_name_randomization

    def __get_hourly_data_cf(self):
        return pycassa.ColumnFamily(self.__pool, self.HOURLY_DATA_COLUMN_FAMILY_NAME, )

    def __get_blob_data_cf(self):
        return pycassa.ColumnFamily(self.__pool, self.BLOB_DATA_COLUMN_FAMILY_NAME)

    def __get_blob_data_index_cf(self):
        return pycassa.ColumnFamily(self.__pool, self.BLOB_DATA_INDEX_COLUMN_FAMILY_NAME)

    # Convert datetime object to millisecond precision unix epoch
    def __unix_time_millis(self, dt):
        return long(time.mktime(dt.timetuple())*1e3 + dt.microsecond/1e3)

    def __datetime_to_utc(self, a_datetime):
        if a_datetime.tzinfo:
            return a_datetime.astimezone (pytz.utc)
        else:
            return a_datetime

    def __from_unix_time_millis(self, unix_time_millis):
        seconds = unix_time_millis / 1000
        millis = unix_time_millis - seconds
        the_datetime = datetime.utcfromtimestamp(seconds)
        the_datetime + timedelta(milliseconds=millis)
        return the_datetime

    # TODO: bleeeeh.. how ugly, can some just optimize this???
    def __datetime_is_in_now_hour(self, a_datetime):
        now = datetime.utcnow()
        if now.year != a_datetime.year:
            return False
        if now.month != a_datetime.month:
            return False
        if now.day != a_datetime.day:
            return False
        if now.hour != a_datetime.hour:
            return False
        return True

    # TODO: dumb stuff, use get_slice of the pycassa library instead FFS!!!
    def __get_slice_of_shard(self, shard, start_datetime, end_datetime):
        truncated_shard = list()

        if not start_datetime:
            start_datetime = self.__from_unix_time_millis(0)

        if not end_datetime:
            end_datetime = MAX_TIME

        for tuple in shard:
            the_datetime = self.highres_to_utc_datetime(tuple[0])
            if the_datetime >= start_datetime and the_datetime <=end_datetime:
                truncated_shard.append((the_datetime, tuple[1]))
        return truncated_shard

    def __get_picoseconds_since_start_of_hour(self, timestamp):
        # This is as accurate as a python datetime object can be
        micros_since_start_of_hour = ((timestamp.minute * 60 * 10**6) + (timestamp.second * 10**6) + (timestamp.microsecond))
        picos_since_start_of_hour = (micros_since_start_of_hour * 10**6)
        return picos_since_start_of_hour

    # Call with exact=True from load-part of code
    def get_high_res_column_name(self, timestamp, exact=False):
        if self.disable_high_res_column_name_randomization:
            return self.__get_picoseconds_since_start_of_hour(timestamp)
        else:
            # Note that the randomization is beyond micro-second precision, so it wont affect the timetamp upon load of data
            # Only the value used as column name
            return self.__get_picoseconds_since_start_of_hour(timestamp) + random.randint(0, 10**6)

    def highres_to_utc_datetime(self, timestamp_for_start_of_hour, picos_since_start_of_hour):
        micros = (picos_since_start_of_hour / 10**6)
        return timestamp_for_start_of_hour + timedelta(microseconds=micros)

    def floor_timestamp_to_hour(self, timestamp):
        return timestamp - timedelta(minutes=timestamp.minute) - timedelta(seconds=timestamp.second) - timedelta(microseconds=timestamp.microsecond)

    ##
    ## Data insertion
    ##
    ######################################################

    # Convenience method to insert a blob that can be auto-indexed, data is typically text
    # If not suitable, just store the blob, and insert indexes manually (create your own suitable indexes, ie based on tags)
    def insert_indexable_text_as_blob_data_and_insert_index(self, ts_data_dto, ttl=None):
        # 1 Store in timeseries shard
        self.insert_timestamped_data(ts_data_dto, ttl)

        # 2 Insert as Blob
        blob_data_row_key = self.insert_blob_data(ts_data_dto, ttl)

        # 3 Create and insert indexes for Blob
        list_of_blob_index_dtos = self.blob_indexer.build_indexes_from_timstamped_dto(ts_data_dto, blob_data_row_key)

        # 4 Batch insert indexes
        self.batch_insert_indexes(list_of_blob_index_dtos, ttl)

    def batch_insert_indexable_text_as_blob_data_and_insert_indexes(self, input_list_of_ts_data_dtos, ttl=None):
        list_of_ts_data_dtos = list()
        # 0 filter out unsupported types
        for obj in input_list_of_ts_data_dtos:
            if obj is None:
                continue
            else:
               list_of_ts_data_dtos.append(obj)
        if len(list_of_ts_data_dtos) == 0:
            # Nothing to do
            return

        # 1 Batch hit DB
        self.batch_insert_timestamped_data(list_of_ts_data_dtos, ttl)

        # 2
        self.batch_insert_blob_data(list_of_ts_data_dtos, ttl)

        # 3 No DB-hit here, only local work
        list_of_blob_index_dtos = list()
        for dto in list_of_ts_data_dtos:
            # TODO: Hmm.. why pass object and a value that can be fetched from the object.. must be legacy design, rethink?
            list_of_blob_index_dtos.extend(self.blob_indexer.build_indexes_from_timstamped_dto(dto, dto.get_row_key_for_blob_data()))

        # 4 batch insert the indexes
        self.batch_insert_indexes(list_of_blob_index_dtos, ttl)

    def insert_timestamped_data(self, ts_data_dto, ttl=None):
        # UTF-8 encode?
        column_name = self.get_high_res_column_name(ts_data_dto.timestamp_as_utc())
        result = self.__get_hourly_data_cf().insert(ts_data_dto.get_row_key_for_hourly(), {column_name : ts_data_dto.data_value}, ttl=ttl)
        return result

    # Will only insert into the shards
    def batch_insert_timestamped_data(self, list_of_timestamped_data_dtos, ttl=None):
        insert_tuples = dict()

        for dto in list_of_timestamped_data_dtos:
            horuly_shard_row_key = dto.get_row_key_for_hourly()
            col_name_value_pairs = insert_tuples.get(horuly_shard_row_key, None)
            if not col_name_value_pairs:
                col_name_value_pairs = dict()
            column_name = self.get_high_res_column_name(dto.timestamp_as_utc())
            col_name_value_pairs[column_name] = dto.data_value
            insert_tuples[horuly_shard_row_key] = col_name_value_pairs

        self.__get_hourly_data_cf().batch_insert(insert_tuples, ttl=ttl)

    def insert_blob_data(self, blob_data_dto, ttl=None):
        row_key = blob_data_dto.get_row_key_for_blob_data()
        self.__get_blob_data_cf().insert(row_key, {blob_data_dto.timestamp_as_utc() : blob_data_dto.data_value}, ttl=ttl)
        return row_key

    def batch_insert_blob_data(self, list_of_blobs, ttl=None):
        insert_tuples = dict()

        for dto in list_of_blobs:
            blob_data_row_key = dto.get_row_key_for_blob_data()
            col_name_value_pairs = insert_tuples.get(blob_data_row_key, None)
            if not col_name_value_pairs:
                col_name_value_pairs = dict()
            col_name_value_pairs[dto.timestamp_as_utc()] = dto.data_value
            insert_tuples[blob_data_row_key] = col_name_value_pairs

        self.__get_blob_data_cf().batch_insert(insert_tuples, ttl=ttl)

    def batch_insert_indexes(self, index_dtos, ttl=None):
        insert_tuples = dict()

        for dto in index_dtos:
            insert_tuples[dto.get_row_key()] = {dto.timestamp_as_utc() : dto.blob_data_row_key}

        # And perform the insertion
        self.__get_blob_data_index_cf().batch_insert(insert_tuples, ttl=ttl)

    ##
    ## Data loading
    ##
    ######################################################

    # Given a list of data_names search for same string in them
    def get_blobs_multi_data_by_free_text_index(self, source_id, data_names, free_text, start_date=None, end_date=None, to_list_of_tuples=True):
        blob_index_rows = list()

        for data_name in data_names:
            blob_index_rows.append(self.get_blob_index_row(source_id, data_name, free_text, start_date, end_date))

        return self.get_blobs_by_kyes(blob_index_rows, to_list_of_tuples)

    def get_blob_index_row(self, source_id, data_name, free_text, start_date="", end_date=""):
        scrubbed_free_text = self.blob_indexer.strip_and_lower(free_text)
        # We dont need the DTO, Just create one for key generation
        index_row_key = BlobIndexDTO(source_id, data_name, scrubbed_free_text, None, None).get_row_key()
        try:
            # Note, a row contains many keys
            if start_date and end_date:
                blob_index_row = self.__get_blob_data_index_cf().get(index_row_key, column_reversed=False, column_count=MAX_COLUMNS, column_start=start_date, column_finish=end_date).items()
            elif start_date and not end_date:
                blob_index_row = self.__get_blob_data_index_cf().get(index_row_key, column_reversed=False, column_count=MAX_COLUMNS, column_start=start_date, column_finish="").items()
            elif end_date and not start_date:
                blob_index_row = self.__get_blob_data_index_cf().get(index_row_key, column_reversed=False, column_count=MAX_COLUMNS, column_start="", column_finish=end_date).items()
            else:
                blob_index_row = self.__get_blob_data_index_cf().get(index_row_key, column_reversed=False, column_count=MAX_COLUMNS, column_start="", column_finish="").items()

        except NotFoundException as e:
            # Differ between not found in Index and not found in Blob-CF (the load done in get_blobs_by_kyes()) which would be a serious error.
            return []

        return blob_index_row

    def get_blobs_by_free_text_index(self, source_id, data_name, free_text, start_date=None, end_date=None, to_list_of_tuples=True):
        blob_index_row = self.get_blob_index_row(source_id, data_name, free_text, start_date, end_date)
        return self.get_blobs_by_kyes([blob_index_row], to_list_of_tuples)

    def get_blobs_by_kyes(self, blob_index_rows, to_list_of_tuples=True):
        ts_data_row_keys_to_multi_fetch = list()

        for blob_index_row in blob_index_rows:
            for blob_index in blob_index_row:
                #utc_timestamp = blob_index_result[0]
                ts_data_row_key = blob_index[1]
                ts_data_row_keys_to_multi_fetch.append(ts_data_row_key)

        # Drop the key from the result by calling .values()
        list_of_ordered_dicts = self.__get_blob_data_cf().multiget(ts_data_row_keys_to_multi_fetch).values()

        if to_list_of_tuples:
            list_of_tuples = list()
            for ordered_dict in list_of_ordered_dicts:
                item = ordered_dict.items()[0]
                list_of_tuples.append((item[0], item[1]))
            return list_of_tuples
        else:
            return list_of_ordered_dicts

    def __load_shard(self, row_key, from_datetime=None, to_datetime=None):
        # Special case, we say we want data from a shard but range is 0, return empty then

        if from_datetime and to_datetime:
            if from_datetime == to_datetime:
                #print u'skipping load'
                return []
            #print u'parital load: %s - %s' % (from_datetime, to_datetime)
            column_start = self.get_high_res_column_name(from_datetime, True)
            column_finish = self.get_high_res_column_name(to_datetime, True)
        else:
            #print u'loading all'
            column_start = ""
            column_finish = ""
        try:
            result = self.__get_hourly_data_cf().get(row_key, column_reversed=False, column_count=MAX_COLUMNS, column_start=column_start, column_finish=column_finish)
            return (row_key, result)
        except NotFoundException:
            return None

    # Load data for given metric_name, a start and end datetime and source_id
    def get_timetamped_data_range(self, source_id, metric_name, start_datetime, end_datetime):

        # TODO: check requested range, or check how many shards we will request
        # should probably put a limit here

        datetimes = list()

        # TODO: check out column slice usage

        curr = self.floor_timestamp_to_hour(start_datetime)
        last = self.floor_timestamp_to_hour(end_datetime)
        while curr <= last:
            datetimes.append(self.floor_timestamp_to_hour(curr))
            curr += timedelta(hours=1)

        # load the hourly shards
        result = list()
        shards = list()
        keys_to_full_shards = list()
        first_shard = []
        last_shard = []

        if len(datetimes) == 0:
            return []
        if len(datetimes) == 1:
            row_key = TimestampedDataDTO( source_id, datetimes[0], metric_name, None).get_row_key_for_hourly()
            shard = self.__load_shard(row_key, start_datetime, end_datetime)
            shards.append(shard)
        if len(datetimes) > 1:
            for i in range(0, len(datetimes)):
                row_key = TimestampedDataDTO( source_id, datetimes[i], metric_name, None).get_row_key_for_hourly()
                if i==0:
                    first_shard = self.__load_shard(row_key, start_datetime, datetimes[i+1]-timedelta(microseconds=1))
                elif i==len(datetimes)-1:
                    last_shard = self.__load_shard(row_key, datetimes[i], end_datetime+timedelta(microseconds=1))
                else:
                    # Store key for multi-get below
                    keys_to_full_shards.append(row_key)

        if first_shard:
            shards.append(first_shard)

        if len(keys_to_full_shards) > 0:
            multi_get_result = self.__get_hourly_data_cf().multiget(keys_to_full_shards, column_reversed=False, column_count=MAX_COLUMNS)
            shards.extend(multi_get_result.items())

        if last_shard:
            shards.append(last_shard)

        # Shards contain hourly data.. need to straighten it out and convert the high-res timestamp
        for shard in shards:
            row_key = shard[0]
            # Restore date from rowkey and column name (which is pico-time offset).. this is wierd... but it works
            floored_datetime_from_key = row_key.split('-')[-1]
            floored_datetime = datetime.strptime(floored_datetime_from_key, '%Y%m%d%H')
            datetimes_and_values = shard[1].items()
            for tuple in datetimes_and_values:
                the_datetime = self.highres_to_utc_datetime(floored_datetime, tuple[0])
                result.append((the_datetime, tuple[1]))

        return result

# Can index a blob that is a valid utf-8 string. If blob is not a valid utf-8 (ie. a png-imgage, skip
# this and create a few tags manually)
class StringIndexer():
    white_list = string.letters + string.digits + ' '
    index_depth = 5
    #default_ttl = 60*60*24*30 # 30 days TTL as default

    # Takes a string and returns a clean string of lower-case words only
    # Makes a good base to create index from
    def strip_and_lower(self, string):
        # Split only on a couple of separators an do lower
        r1 = re.sub('[,\.\-=!@#$\(\)<>_\[\]\'\"\´\:]', ' ', string.lower())
        r2 = ' '.join(r1.split())
        return r2.encode('utf-8')

        #pattern = re.compile('([^\s\w]|_)+')
        #strippedList = pattern.sub(' ', string)
        #return strippedList.lower().strip(' \t\n\r')


    # Will split a sting and return the permutations given depth
    # made for storing short scentences too use as index
    #
    # Example, given 'hello indexed words' and depth = 2
    # Will return: ['hello', 'indexed', 'words', 'hello indexed', 'indexed words']
    # ie depth equals the maximum number of words in a substring
    #
    # Assume string can be split on space, depth is an integer >= 1
    def _build_substrings(self, string, depth):
        result = set()
        words = string.split()

        for d in range(0, depth):
            for i in range(0, len(words)):
                if i+d+1 > len(words):
                    continue
                current_words = words[i:i+d+1]
                result.add(' '.join(current_words))
        return result

    # idea: could add flag to run the loop again but with ommited source_id and/or dataname to
    # return double and tripple amount of keys to make a global search available
    def build_indexes_from_timstamped_dto(self, dto, blob_data_row_key):
        if dto.str_for_index:
            indexable_string = self.strip_and_lower(dto.str_for_index)
        else:
            indexable_string = self.strip_and_lower(dto.data_value)

        substrings = self._build_substrings(indexable_string, self.index_depth)

        index_dtos = []
        for substring in substrings:
            index_dto = BlobIndexDTO(dto.source_id, dto.data_name, substring, dto.timestamp, blob_data_row_key)
            index_dtos.append(index_dto)

        return index_dtos

    def __datetime_to_utc(self, a_datetime):
        if a_datetime.tzinfo:
            # Convert to UTC if timezone info
            return a_datetime.astimezone (pytz.utc)
        else:
            # Assume datetime was in UTC if no timezone info exists
            return a_datetime

    def __build_row_key_for_timestamped_string(self, source_id, utc_datetime):
#        time_part = utc_datetime.strftime('%Y%m%d')
#        return str(source_id+'-'+time_part)
        return source_id
