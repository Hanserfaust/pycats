# -*- coding: utf-8 -*-
import pycassa
import pytz
import time
from datetime import datetime, timedelta
from logging import debug, error, info
import re
import string
from models import TimestampedDataDTO, BlobIndexDTO

#
# Create the DAO using a List of cassandra hosts and a KeySpace
# The KeySpace must exist on the cassandra cluster.
# A cache instance can optionally be supplied
#
# The KeySpace must have a column family created as follows:
#
# CREATE COLUMNFAMILY HourlyFloat (KEY ascii PRIMARY KEY) WITH comparator=timestamp AND default_validation=blob;
#
####################################################################################

MAX_COLUMNS = 60*60*24
CACHE_TTL = 8*60*60 # 8 hours
# Sorry we are not Y10K compatible, just need something surely beyond anything reasonable
MAX_TIME = datetime.strptime('2900-01-01T01:59:59', '%Y-%m-%dT%H:%M:%S')

class TimeSeriesCassandraDao():
    #  CREATE COLUMNFAMILY HourlyTimestampedData (KEY ascii PRIMARY KEY) WITH comparator=timestamp AND default_validation=blob;
    HOURLY_DATA_COLUMN_FAMILY_NAME = 'HourlyTimestampedData'

    # CREATE COLUMNFAMILY BlobData (KEY ascii PRIMARY KEY) WITH comparator=timestamp AND default_validation=blob;
    BLOB_DATA_COLUMN_FAMILY_NAME = 'BlobData'

    # CREATE COLUMNFAMILY BlobDataIndex (KEY text PRIMARY KEY) WITH comparator=timestamp AND default_validation=text;
    BLOB_DATA_INDEX_COLUMN_FAMILY_NAME = 'BlobDataIndex'

    string_indexer = None

    def __init__(self, cassandra_hosts, key_space, cache=None, warm_up_cache_shards=0):
        self.__cassandra_hosts = cassandra_hosts
        self.__key_space = key_space
        self.__pool = pycassa.ConnectionPool(self.__key_space, self.__cassandra_hosts)
        self.cache = cache
        self.cache_hits = 0
        self.daily_gets = 0
        self.__warm_up_cache_shards = warm_up_cache_shards
        self.string_indexer = StringIndexer()

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

    def __get_hourly_data_cached(self, row_key, a_datetime):
        if not self.__datetime_is_in_now_hour(a_datetime) and self.cache:
            hourly_data = self.cache.get(row_key)
            if hourly_data:
                # Cache hit:
                self.cache_hits+=1
                return hourly_data

        # Requested time was for the current hour shard (or no cache is used), go fetch then
        self.daily_gets+=1
        # TODO: improve, use pycassa.multiget() if row_key argument is a list !!!
        hourly_data = self.__get_hourly_data_cf().get(row_key, column_reversed=False, column_count=MAX_COLUMNS).items()
        if self.cache:
            # add means, insert only if not existing in cache
            self.cache.add(row_key, hourly_data, CACHE_TTL)
        return hourly_data

    # TODO: dumb stuff, use get_slice of the pycassa library instead FFS!!!
    def __get_slice_of_shard(self, shard, start_datetime, end_datetime):
        truncated_shard = list()

        if not start_datetime:
            start_datetime = self.__from_unix_time_millis(0)

        if not end_datetime:
            end_datetime = MAX_TIME

        for tuple in shard:
            if tuple[0] >= start_datetime and tuple[0] <=end_datetime:
                truncated_shard.append((tuple[0], tuple[1]))
        return truncated_shard

    def __floor_datetime_to_start_of_hour(self, tm):
        tm = tm - timedelta(minutes=tm.minute,
            seconds=tm.second,
            microseconds=tm.microsecond)
        return tm

    ##
    ## Data insertion
    ##
    ######################################################
    def insert_timestamped_data(self, ts_data_dto):
        # UTF-8 encode?
        result = self.__get_hourly_data_cf().insert(ts_data_dto.get_row_key_for_hourly(), {ts_data_dto.timestamp_as_utc() : ts_data_dto.data_value})
        return result

    # Will only insert into the buckets
    def batch_insert_timestamped_data(self, list_of_timestamped_data_dtos):
        insert_tuples = dict()

        for dto in list_of_timestamped_data_dtos:
            horuly_bucket_row_key = dto.get_row_key_for_hourly()
            col_name_value_pairs = insert_tuples.get(horuly_bucket_row_key, None)
            if not col_name_value_pairs:
                col_name_value_pairs = dict()
            col_name_value_pairs[dto.timestamp_as_utc()] = dto.data_value
            insert_tuples[horuly_bucket_row_key] = col_name_value_pairs

        self.__get_hourly_data_cf().batch_insert(insert_tuples)

    # Convenience method to insert a blob that can be auto-indexed, data is typically text
    # If not suitable, just store the blob, and insert indexes manually (create your own suitable indexes, such as tags)
    def insert_indexable_text_as_blob_data_and_insert_index(self, ts_data_dto):
        blob_data_row_key = self.insert_blob_data(ts_data_dto)
        list_of_string_index_dtos = self.string_indexer.build_indexes_from_timstamped_dto(ts_data_dto, blob_data_row_key)
        self.batch_insert_indexes(list_of_string_index_dtos)

    def insert_blob_data(self, blob_data_dto):
        row_key = blob_data_dto.get_row_key_for_blob_data()
        self.__get_blob_data_cf().insert(row_key, {blob_data_dto.timestamp_as_utc() : blob_data_dto.data_value})
        return row_key

    def batch_insert_indexes(self, index_dtos):
        insert_tuples = dict()

        for dto in index_dtos:
            insert_tuples[dto.get_row_key()] = {dto.timestamp_as_utc() : dto.blob_data_row_key}

        # And perform the insertion
        self.__get_blob_data_index_cf().batch_insert(insert_tuples)

    ##
    ## Data loading
    ##
    ######################################################
    def get_blobs_by_free_text_index(self, source_id, data_name, free_text, to_list_of_tuples=True):
        scrubbed_free_text = self.string_indexer.strip_and_lower(free_text)
        # Just create empty index-dto for key generation
        index_row_key = BlobIndexDTO(source_id, data_name, scrubbed_free_text, None, None).get_row_key()
        try:
            result_from_index = self.__get_blob_data_index_cf().get(index_row_key, column_reversed=False, column_count=MAX_COLUMNS).items()
        except Exception as e:
            # Not found mostlikley
            return None

        ts_data_row_keys_to_multi_fetch = list()
        for string_index_result in result_from_index:
            #utc_timestamp = string_index_result[0]
            ts_data_row_key = string_index_result[1]
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

    # Load data for given metric_name, a start and end datetime and source_id
    def get_timetamped_data_range(self, source_id, metric_name, start_datetime, end_datetime):

        # TODO: check requested range, or check how many shards we will request
        # should probably put a limit here

        datetimes = list()

        # TODO: check out column slice usage

        curr = self.__floor_datetime_to_start_of_hour(start_datetime)
        last = self.__floor_datetime_to_start_of_hour(end_datetime)
        while curr <= last:
            datetimes.append(curr)
            curr += timedelta(hours=1)

        # load the hourly shards
        shards = list()
        result = list()

        for a_datetime in datetimes:
            # Build dto without value for key generation
            row_key = TimestampedDataDTO( source_id, a_datetime, metric_name, None).get_row_key_for_hourly()
            try:
                debug('trying %s' % (row_key))
                a_shard = self.__get_hourly_data_cached(row_key, a_datetime)
                shards.append(a_shard)
            except Exception, e:
                # Not found, or other error, try next date
                pass

        if len(shards) == 0:
            return []

        if len(shards) == 1:
            truncated_shard = self.__get_slice_of_shard(shards[0], start_datetime, end_datetime)
            result.extend(truncated_shard)

        if len(shards) == 2:
            first_truncated_shard = self.__get_slice_of_shard(shards[0], start_datetime, None)
            result.extend(first_truncated_shard)
            last_truncated_shard = self.__get_slice_of_shard(shards[1], None, end_datetime)
            result.extend(last_truncated_shard)

        if len(shards) > 2:
            first_truncated_shard = self.__get_slice_of_shard(shards[0], start_datetime, None)
            result.extend(first_truncated_shard)

            middle_chunk = shards[1:len(shards)-1]
            for shard in middle_chunk:
                result.extend(shard)

            last_truncated_shard = self.__get_slice_of_shard(shards[len(shards)-1], None, end_datetime)
            result.extend(last_truncated_shard)

        return result

    def try_warm_up_hourly_timestamped_data_cache(self, source_id, a_datetime, metric_name):
        if self.cache and self.__warm_up_cache_shards > 0:
            for offs in range(1,self.__warm_up_cache_shards+1):
                earlier_datetime = a_datetime-timedelta(hours=offs)
                earlier_key = self.__build_row_key_for_hourly(source_id,metric_name, earlier_datetime)
                self.__get_hourly_data_cached(earlier_key, earlier_datetime)

    def get_timestamped_data(self, source_id, a_datetime, metric_name):
        row_key = self.__build_row_key_for_hourly(source_id,metric_name, a_datetime)

        result = self.__get_hourly_data_cached(row_key, a_datetime)

        return result

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
