import pycassa
import time
import pytz
from datetime import datetime, timedelta
from logging import debug, error, info
#
# Create the DAO using a List of cassandra hosts and a KeySpace
# The KeySpace must exist on the cassandra cluster.
# A cache instance can optionally be supplied
#
# The KeySpace must have a column family created as follows:
#
# CREATE COLUMNFAMILY HourlyFloat (KEY ascii PRIMARY KEY) WITH comparator=timestamp AND default_validation=float;
#
####################################################################################

MAX_COLUMNS = 60*60*24
CACHE_TTL = 8*60*60 # 8 hours
# Sorry we are not Y10K compatible, just need something surely beyond anything reasonable
MAX_TIME = datetime.strptime('2100-01-01T01:59:59', '%Y-%m-%dT%H:%M:%S')

class FloatMetricDTO():
    def __init__(self, source_id, timestamp, metric_name, value):
        self.source_id = source_id
        self.timestamp = timestamp
        self.metric_name = metric_name
        self.value = value

class TimeSeriesCassandraDao():
    HOURLY_FLOAT_COLUMN_FAMILY_NAME = 'HourlyFloat'

    def __init__(self, cassandra_hosts, key_space, cache=None, warm_up_cache_shards=0):
        self.__cassandra_hosts = cassandra_hosts
        self.__key_space = key_space
        self.__pool = pycassa.ConnectionPool(self.__key_space, self.__cassandra_hosts)
        self.cache = cache
        self.cache_hits = 0
        self.daily_gets = 0
        self.__warm_up_cache_shards = warm_up_cache_shards

    def __get_byteport_hourly_float_cf(self):
        return pycassa.ColumnFamily(self.__pool, self.HOURLY_FLOAT_COLUMN_FAMILY_NAME)

    def __build_row_key_for_hourly(self, source_id, metric_name, utc_datetime):
        time_part = utc_datetime.strftime('%Y%m%d%H')
        return str(source_id+'-'+metric_name+'-'+time_part)

    # Convert datetime object to millisecond precision unix epoch
    def __unix_time_millis(self, dt):
        return long(time.mktime(dt.timetuple())*1e3 + dt.microsecond/1e3)

    def __from_unix_time_millis(self, unix_time_millis):
        seconds = unix_time_millis / 1000
        millis = unix_time_millis - seconds
        the_datetime = datetime.utcfromtimestamp(seconds)
        the_datetime + timedelta(milliseconds=millis)
        return the_datetime

    def __datetime_to_utc(self, a_datetime):
        if a_datetime.tzinfo:
            # Convert to UTC if timezone info
            return a_datetime.astimezone (pytz.utc)
        else:
            # Assume datetime was in UTC if no timezone info exists
            return a_datetime

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

    def __get_hourly_float_cached(self, row_key, a_datetime):
        if not self.__datetime_is_in_now_hour(a_datetime) and self.cache:
            hourly_data = self.cache.get(row_key)
            if hourly_data:
                # Cache hit:
                self.cache_hits+=1
                return hourly_data

        # Requested time was for the current hour shard (or no cache is used), go fetch then
        self.daily_gets+=1
        # TODO: improve, use pycassa.multiget() if row_key argument is a list !!!
        hourly_data = self.__get_byteport_hourly_float_cf().get(row_key, column_reversed=False, column_count=MAX_COLUMNS).items()
        if self.cache:
            # add means, insert only if not existing in cache
            self.cache.add(row_key, hourly_data, CACHE_TTL)
        return hourly_data

    # TODO: improve, not good to iterate over the dict
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

    def __build_pycassa_insert_tuple(self, float_metric_dto):
        utc_datetime = self.__datetime_to_utc(float_metric_dto.timestamp)

        row_key = self.__build_row_key_for_hourly(float_metric_dto.source_id, float_metric_dto.metric_name, utc_datetime)

        column_name = utc_datetime
        column_value = float(float_metric_dto.value)

        return (row_key, {column_name : column_value})

    ##
    ## Float metric interface
    ##
    ######################################################
    def insert_hourly_float_metric(self, float_metric_dto):
        tuple = self.__build_pycassa_insert_tuple(float_metric_dto)
        return self.__get_byteport_hourly_float_cf().insert(tuple[0], tuple[1])

        # during development only
        #print u'%s : {%s : %s}' % (row_key, column_name, column_value)

    def batch_insert_hourly_float_metric(self, list_of_float_metric_dtos):
        list_of_insert_tuples = dict()

        for float_metric_dto in list_of_float_metric_dtos:
            tuple = self.__build_pycassa_insert_tuple(float_metric_dto)
            row_key = tuple[0]
            col_name_value_pairs = list_of_insert_tuples.get(row_key, None)
            if not col_name_value_pairs:
                col_name_value_pairs = dict()
            col_name_value_pairs.update(tuple[1])
            list_of_insert_tuples[row_key] = col_name_value_pairs

        self.__get_byteport_hourly_float_cf().batch_insert(list_of_insert_tuples)

    # Load data for given metric_name, a start and end datetime and source_id
    def get_hourly_float_data_range(self, source_id, metric_name, start_datetime, end_datetime):

        # TODO: check requested range, or check how many shards we will request
        # should probably put a limit here

        datetimes = list()
        curr_datetime = start_datetime

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
            row_key = self.__build_row_key_for_hourly(source_id, metric_name, a_datetime)
            try:
                debug('trying %s' % (row_key))
                a_shard = self.__get_hourly_float_cached(row_key, a_datetime)
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

    def try_warm_up_hourly_float_metric_cache(self, source_id, a_datetime, metric_name):
        if self.cache and self.__warm_up_cache_shards > 0:
            for offs in range(1,self.__warm_up_cache_shards+1):
                earlier_datetime = a_datetime-timedelta(hours=offs)
                earlier_key = self.__build_row_key_for_hourly(source_id,metric_name, earlier_datetime)
                self.__get_hourly_float_cached(earlier_key, earlier_datetime)

    def get_hourly_float(self, source_id, a_datetime, metric_name):
        row_key = self.__build_row_key_for_hourly(source_id,metric_name, a_datetime)

        result = self.__get_hourly_float_cached(row_key, a_datetime)

        return result
