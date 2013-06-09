# -*- coding: utf-8 -*-
import pycassa
from pycassa.cassandra.ttypes import NotFoundException
from datetime import datetime, timedelta
from models import TimestampedDataDTO, BlobIndexDTO
import random
import pytz
import indexers
import time

MAX_TIME_SERIES_COLUMN_COUNT = 1000
MAX_INDEX_COLUMN_COUNT = 100
MAX_BLOB_COLUMN_COUNT = 100

CACHE_TTL = 8*60*60 # 8 hours
# Sorry we are not Y10K compatible, just need something surely beyond anything reasonable
MAX_TIME = datetime.strptime('2900-01-01T01:59:59', '%Y-%m-%dT%H:%M:%S')

#
#
# TODO: Insert link to a few figures that explain what this class is up to.
#
# Warning: Code looks overly complex and is in need of refactoring:
#
# Known fuzzyness:
#   - Code has chaned drastically a few times, hence old names may still appear
#   - Code looks overly complex
#   - Caching solution was implemented, but disabled again. Needs to be fixed again.
#   - Variable names related to the DTOs does not have unified names over the code base
#   - Explain why the column names in the time-series ColumnFamily needs pico-second precision
#
# See tests.py for details on how to setup the required keyspace and ColumnFamilies
#
####################################################################################
class TimeSeriesCassandraDao():

    #  CREATE COLUMNFAMILY HourlyTimestampedData (KEY ascii PRIMARY KEY) WITH comparator=timestamp;
    HOURLY_DATA_COLUMN_FAMILY_NAME = 'HourlyTimestampedData'

    #  CREATE COLUMNFAMILY LatestData (KEY ascii PRIMARY KEY) WITH comparator=ascii;
    LATEST_DATA_COLUMN_FAMILY_NAME = 'LatestData'

    # CREATE COLUMNFAMILY BlobData (KEY ascii PRIMARY KEY) WITH comparator=timestamp;
    BLOB_DATA_COLUMN_FAMILY_NAME = 'BlobData'

    # CREATE COLUMNFAMILY BlobDataIndex (KEY text PRIMARY KEY) WITH comparator=timestamp AND default_validation=text;
    BLOB_DATA_INDEX_COLUMN_FAMILY_NAME = 'BlobDataIndex'

    # Maybe the dao should be un-aware of the indexer, break out and make it cleaner?
    blob_indexer = None

    # Important: keep the randomizer on in production environments to avoid collisions (overwrites) in the time-series CF to a minimum
    def __init__(self, cassandra_hosts, key_space, cache=None, warm_up_cache_shards=0, disable_high_res_column_name_randomization=False, index_depth=5, pool_size=5, prefill=True):
        self.__cassandra_hosts = cassandra_hosts
        self.__key_space = key_space
        self.__pool = pycassa.ConnectionPool(self.__key_space, self.__cassandra_hosts, pool_size=0, prefill=False)
        self.cache = cache
        self.cache_hits = 0
        self.daily_gets = 0
        self.millis = 0
        self.__warm_up_cache_shards = warm_up_cache_shards
        self.blob_indexer = indexers.StringIndexer(index_depth)
        self.disable_high_res_column_name_randomization = disable_high_res_column_name_randomization

    def __get_hourly_data_cf(self):
        return pycassa.ColumnFamily(self.__pool, self.HOURLY_DATA_COLUMN_FAMILY_NAME)

    def __get_latest_data_cf(self):
        return pycassa.ColumnFamily(self.__pool, self.LATEST_DATA_COLUMN_FAMILY_NAME)

    def __get_blob_data_cf(self):
        return pycassa.ColumnFamily(self.__pool, self.BLOB_DATA_COLUMN_FAMILY_NAME)

    def __get_blob_data_index_cf(self):
        return pycassa.ColumnFamily(self.__pool, self.BLOB_DATA_INDEX_COLUMN_FAMILY_NAME)

    # Convert datetime object to millisecond precision unix epoch
#    def __unix_time_millis(self, dt):
#        return long(time.mktime(dt.timetuple())*1e3 + dt.microsecond/1e3)
#
#    def __datetime_to_utc(self, a_datetime):
#        if a_datetime.tzinfo:
#            return a_datetime.astimezone (pytz.utc)
#        else:
#            return a_datetime
#
#    def __from_unix_time_millis(self, unix_time_millis):
#        seconds = unix_time_millis / 1000
#        millis = unix_time_millis - seconds
#        the_datetime = datetime.utcfromtimestamp(seconds)
#        the_datetime + timedelta(milliseconds=millis)
#        return the_datetime

    # Legacy for cache solution,
    # alternative: could just compare a floored datetime to a floored "now"
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
            return self.__get_picoseconds_since_start_of_hour(timestamp) + random.randint(1, 10**6)

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

    def create_insert_dict_for_latest_data(self, data_name, data_value, timestamp):
        return  {data_name : data_value, data_name+'-ts' : str(timestamp)}

    def insert_latest_data(self, dto, verify_timestamp=True):
        last_ts = 0
        this_ts = dto.timestamp_as_unix_time_millis()
        if verify_timestamp:
            try:
                last_latest_data = self.load_latest_data(dto.source_id)
                last_ts = int(last_latest_data[dto.data_name+'-ts'])
            except NotFoundException:
                # Source did not store any data before
                pass
            except KeyError:
                # Source stored data, but this data_name is new
                pass
            except Exception:
                # could not parse the timestamp, format may have change?
                last_ts = 0
        if this_ts > last_ts:
            self.__get_latest_data_cf().insert(dto.source_id, self.create_insert_dict_for_latest_data(dto.data_name, dto.data_value, this_ts))

    # Will force insert a dictionary of data using UTC now as timestamp
    def insert_latest_data_by_dict(self, source_id, data_dict):
        now = datetime.utcnow()
        timestamp = long(time.mktime(now.timetuple())*1e3 + now.microsecond/1e3)
        i_dict = dict()
        for data_name in data_dict.keys():
            i_dict.update(self.create_insert_dict_for_latest_data(data_name, data_dict[data_name], timestamp))
        self.__get_latest_data_cf().insert(source_id, i_dict)

    def insert_timestamped_data(self, ts_data_dto, ttl=None, set_latest=False):
        # UTF-8 encode?
        column_name = self.get_high_res_column_name(ts_data_dto.timestamp_as_utc())
        result = self.__get_hourly_data_cf().insert(ts_data_dto.get_row_key_for_hourly(), {column_name : ts_data_dto.data_value}, ttl=ttl)
        if set_latest:
            self.insert_latest_data(ts_data_dto)
        return result

    # Will only insert into the shards
    def batch_insert_timestamped_data(self, list_of_timestamped_data_dtos, ttl=None, set_latest=False):
        hourly_insert_dict = dict()

        for dto in list_of_timestamped_data_dtos:
            hourly_shard_row_key = dto.get_row_key_for_hourly()
            col_name_value_pairs = hourly_insert_dict.get(hourly_shard_row_key, None)
            if not col_name_value_pairs:
                col_name_value_pairs = dict()
            column_name = self.get_high_res_column_name(dto.timestamp_as_utc())
            col_name_value_pairs[column_name] = dto.data_value
            hourly_insert_dict[hourly_shard_row_key] = col_name_value_pairs

        if set_latest:
            for dto in list_of_timestamped_data_dtos:
                self.insert_latest_data(dto)

        self.__get_hourly_data_cf().batch_insert(hourly_insert_dict, ttl=ttl)

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
    def get_blobs_multi_data_by_free_text_index(self, source_id, data_names, free_text, start_date=None, end_date=None, to_list_of_tuples=True, column_count=MAX_INDEX_COLUMN_COUNT):
        blob_index_rows = list()

        for data_name in data_names:
            blob_index_rows.append(self.get_blob_index_row(source_id, data_name, free_text, start_date, end_date, column_count))

        return self.get_blobs_by_keys(blob_index_rows, to_list_of_tuples)

    def get_blob_index_row(self, source_id, data_name, free_text, start_date="", end_date="", column_count=MAX_INDEX_COLUMN_COUNT):
        scrubbed_free_text = self.blob_indexer.strip_and_lower(free_text)
        # We don't need the DTO, Just create one for key generation
        index_row_key = BlobIndexDTO(source_id, data_name, scrubbed_free_text, None, None).get_row_key()
        try:
            # Note, a row contains many keys
            if start_date and end_date:
                blob_index_row = self.__get_blob_data_index_cf().get(index_row_key, column_reversed=False, column_count=column_count, column_start=start_date, column_finish=end_date).items()
            elif start_date and not end_date:
                blob_index_row = self.__get_blob_data_index_cf().get(index_row_key, column_reversed=False, column_count=column_count, column_start=start_date, column_finish="").items()
            elif end_date and not start_date:
                blob_index_row = self.__get_blob_data_index_cf().get(index_row_key, column_reversed=False, column_count=column_count, column_start="", column_finish=end_date).items()
            else:
                blob_index_row = self.__get_blob_data_index_cf().get(index_row_key, column_reversed=False, column_count=column_count, column_start="", column_finish="").items()

        except NotFoundException as e:
            # Differ between not found in Index and not found in Blob-CF (the load done in get_blobs_by_kyes()) which would be a serious error.
            return []

        return blob_index_row

    def get_blobs_by_free_text_index(self, source_id, data_name, free_text, start_date=None, end_date=None, to_list_of_tuples=True, column_count=MAX_INDEX_COLUMN_COUNT):
        blob_index_row = self.get_blob_index_row(source_id, data_name, free_text, start_date, end_date, column_count)
        return self.get_blobs_by_keys([blob_index_row], to_list_of_tuples, column_count)

    def get_blobs_by_keys(self, blob_index_rows, to_list_of_tuples=True, column_count=MAX_BLOB_COLUMN_COUNT):
        ts_data_row_keys_to_multi_fetch = list()

        for blob_index_row in blob_index_rows:
            for blob_index in blob_index_row:
                ts_data_row_key = blob_index[1]
                ts_data_row_keys_to_multi_fetch.append(ts_data_row_key)

        # Drop the key from the result
        list_of_ordered_dicts = self.__get_blob_data_cf().multiget(ts_data_row_keys_to_multi_fetch, column_count=column_count).values()

        if to_list_of_tuples:
            list_of_tuples = list()
            for ordered_dict in list_of_ordered_dicts:
                item = ordered_dict.items()[0]
                list_of_tuples.append((item[0], item[1]))
            return list_of_tuples
        else:
            return list_of_ordered_dicts

    def remove_latest_data(self, source_id):
        self.__get_latest_data_cf().remove(source_id)

    def load_latest_data(self, source_id, data_name=None):
        try:
            latest_data = self.__get_latest_data_cf().get(source_id, super_column=data_name)
        except NotFoundException:
            return {}
        return latest_data

    def multi_load_latest_data(self, source_ids):
        try:
            latest_data = self.__get_latest_data_cf().multiget(source_ids)
        except NotFoundException:
            return []
        return latest_data

    def __load_shard(self, row_key, from_datetime=None, to_datetime=None, column_count=MAX_TIME_SERIES_COLUMN_COUNT, allow_cached_loads=False):
        # Special case, we say we want data from a shard but range is 0, return empty then
        if from_datetime and to_datetime:
            if from_datetime == to_datetime:
                #print u'skipping load'
                return (row_key, {})
            #print u'parital load: %s - %s' % (from_datetime, to_datetime)
            column_start = self.get_high_res_column_name(from_datetime, True)
            column_finish = self.get_high_res_column_name(to_datetime, True)
        else:
            #print u'loading all'
            column_start = ""
            column_finish = ""
        try:
            result = self.__get_hourly_data_cf().get(row_key, column_reversed=False, column_count=column_count, column_start=column_start, column_finish=column_finish)
            return (row_key, result)
        except NotFoundException:
            return (row_key, {})

    # Load data for given metric_name, a start and end datetime and source_id
    #
    # Note that max_count referres to the maximum size of the total result.
    #
    # Never asume the whole range will be fetched. Call will returned when max_count is reached.
    def get_timetamped_data_range(self, source_id, metric_name, start_datetime, end_datetime, max_count=MAX_TIME_SERIES_COLUMN_COUNT, allow_cached_loads=False):

        # TODO: check requested range, or check how many shards we will request
        # should probably put a limit here

        datetimes = list()
        maximum_allowed = max_count

        # TODO: check out column slice usage

        curr = self.floor_timestamp_to_hour(start_datetime)
        last = self.floor_timestamp_to_hour(end_datetime)
        while curr <= last:
            datetimes.append(self.floor_timestamp_to_hour(curr))
            curr += timedelta(hours=1)

        # load the hourly shards
        result = list()
        shards = list()
        key_to_last_shard = None

        if len(datetimes) == 0:
            return []
        if len(datetimes) == 1:
            row_key = TimestampedDataDTO( source_id, datetimes[0], metric_name, None).get_row_key_for_hourly()
            shard = self.__load_shard(row_key, start_datetime, end_datetime)
            shards.append(shard)
        if len(datetimes) > 1:
            for i in range(0, len(datetimes)):
                if maximum_allowed <= 0 :
                    # Cant go on, would be good to explicitly not this upwards?
                    break
                row_key = TimestampedDataDTO( source_id, datetimes[i], metric_name, None).get_row_key_for_hourly()
                if i==0:
                    a_shard = self.__load_shard(row_key, start_datetime, datetimes[i+1]-timedelta(microseconds=1), maximum_allowed, allow_cached_loads)
                elif i > 0 and i < len(datetimes) -1:
                    a_shard =  self.__load_shard(row_key, column_count=maximum_allowed, allow_cached_loads=allow_cached_loads)
                else:
                    a_shard = self.__load_shard(row_key, datetimes[len(datetimes)-1], end_datetime+timedelta(microseconds=1), maximum_allowed, allow_cached_loads)
                maximum_allowed -= len(a_shard[1])
                shards.append(a_shard)

        # Avoid multiget for now due to uncertainty of the column_count meaning. We dont want to fetch the 100 first of many slices,
        # leaving us with holes in the data series we are fetching
        #
        #if len(keys_to_full_shards) > 0:
        #    # TODO: important, should any of the multi-fetched slices be limited
        #    multi_get_result = self.__get_hourly_data_cf().multiget(keys_to_full_shards, column_reversed=False, column_count=maximum_allowed)
        #    # Check if any of the shards was limited
        #    shards.extend(multi_get_result.items())

        # Shards contain hourly data.. need to straighten it out and convert the high-res timestamp
        for shard in shards:
            if not shard:
                continue
            row_key = shard[0]
            # Restore date from rowkey and column name (which is pico-time offset).. this is wierd... but it works
            floored_datetime_from_key = row_key.split('-')[-1]
            floored_datetime = datetime.strptime(floored_datetime_from_key, '%Y%m%d%H')
            datetimes_and_values = shard[1].items()
            for tuple in datetimes_and_values:
                the_datetime = self.highres_to_utc_datetime(floored_datetime, tuple[0])
                result.append((the_datetime, tuple[1]))

        return result
