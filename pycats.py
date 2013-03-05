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

    def __get_hourly_float_cf(self):
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
        hourly_data = self.__get_hourly_float_cf().get(row_key, column_reversed=False, column_count=MAX_COLUMNS).items()
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
        return self.__get_hourly_float_cf().insert(tuple[0], tuple[1])

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

        self.__get_hourly_float_cf().batch_insert(list_of_insert_tuples)

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


# The KeySpace must have a column family created as follows:
#
# CREATE COLUMNFAMILY IndexedStrings (KEY ascii PRIMARY KEY) WITH comparator=timestamp AND default_validation=text;
#
####################################################################################
import re
import string

class TimeStampedStringDTO():
    def __init__(self, source_id, timestamp, string):
        self.source_id = source_id
        self.timestamp = timestamp
        self.string = string

class StringIndexDTO():
    def __init__(self, index_key, timestamp, key_to_string):
        self.row_key = index_key
        self.timestamp = timestamp
        self.key_to_string = key_to_string

    def __unicode__(self):
        return u'%s for %s on time %s' % (self.row_key, self.key_to_string, self.timestamp)

class IndexedStringsCassandraDAO():
    TIME_STAMPED_STRING_COLUMN_FAMILY_NAME = 'TimeStampedString'
    white_list = string.letters + string.digits + ' '
    index_depth = 5
    #default_ttl = 60*60*24*30 # 30 days TTL as default

    def __init__(self, cassandra_hosts, key_space, index_depth=5, ttl=None, cache=None, warm_up_cache_shards=0):
        self.__cassandra_hosts = cassandra_hosts
        self.__key_space = key_space
        self.__pool = pycassa.ConnectionPool(self.__key_space, self.__cassandra_hosts)
        self.index_depth = index_depth
        self.cache = cache
        self.cache_hits = 0
        self.daily_gets = 0
        self.ttl = ttl
        self.__warm_up_cache_shards = warm_up_cache_shards

    def __get_timestamped_string_cf(self):
        return pycassa.ColumnFamily(self.__pool, self.TIME_STAMPED_STRING_COLUMN_FAMILY_NAME)


    # TODO: consider breaking out the indexing methods to separate class
    # to make the string storage be independent of indexing method

    # Takes a string and returns a clean string of lower-case words only
    # Makes a good base to create index from
    def make_indexable_string(self, string):
        pattern = re.compile('([^\s\w]|_)+')
        strippedList = pattern.sub(' ', string)
        return strippedList.lower().strip(' \t\n\r')

    # Will split a sting and return the permutations given depth
    # made for storing short scentences too use as index
    #
    # Example, given 'hello indexed words' and depth = 2
    # Will return: ['hello', 'indexed', 'words', 'hello indexed', 'indexed words']
    # ie depth equals the maximum number of words in a substring
    #
    # Assume string can be split on space, depth is an integer >= 1
    def build_substrings(self, string, depth):
        result = []
        words = string.split()

        for d in range(0, depth):
            for i in range(0, len(words)):
                if i+d+1 > len(words):
                    continue
                current_words = words[i:i+d+1]
                result.append('_'.join(current_words))
        return result

    def build_index_key(self, source_id, substring):
        return source_id + '.' + substring

    def build_indexes_from_string_dto(self, dto, key_to_string_dto):
        indexable_string = self. make_indexable_string(dto.string)

        substrings = self.build_substrings(indexable_string, self.index_depth)

        index_dtos = []
        for substring in substrings:
            index_key = self.build_index_key(dto.source_id, substring)
            index_dto = StringIndexDTO(index_key, dto.timestamp, key_to_string_dto)
            index_dtos.append(index_dto)

        return index_dtos


    def __datetime_to_utc(self, a_datetime):
        if a_datetime.tzinfo:
            # Convert to UTC if timezone info
            return a_datetime.astimezone (pytz.utc)
        else:
            # Assume datetime was in UTC if no timezone info exists
            return a_datetime

    # TODO: consider storing in daily-chunks similar to the Floats in the class above
    def __build_row_key_for_timestamped_string(self, source_id, utc_datetime):
#        time_part = utc_datetime.strftime('%Y%m%d')
#        return str(source_id+'-'+time_part)
        return source_id

    def __build_pycassa_insert_tuple(self, time_stamped_string_dto):
        utc_datetime = self.__datetime_to_utc(time_stamped_string_dto.timestamp)

        row_key = self.__build_row_key_for_timestamped_string(time_stamped_string_dto.source_id, utc_datetime)

        column_name = utc_datetime
        column_value = u'%s' % time_stamped_string_dto.string

        return (row_key, {column_name : column_value})

    def store_timestamped_string_dto(self, dto):
        tuple = self.__build_pycassa_insert_tuple(dto)
        self.__get_timestamped_string_cf().insert(tuple[0], tuple[1])

        # create indexes, note using the datetime as key, upon load, we request the log that has that same timestamp
        index_dtos = self.build_indexes_from_string_dto(dto, dto.timestamp)
