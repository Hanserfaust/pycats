import pycassa
import time
import pytz
from datetime import datetime, timedelta
from logging import debug, error, info
import re
import string

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
MAX_TIME = datetime.strptime('2100-01-01T01:59:59', '%Y-%m-%dT%H:%M:%S')

class TimestampedDataDTO():
    def __init__(self, source_id, timestamp, data_name, data_value):
        self.source_id = source_id
        self.timestamp = timestamp
        self.data_name = data_name
        self.data_value = data_value

class StringIndexDTO():
    def __init__(self, row_key, timestamp, data_value):
        self.row_key = row_key
        self.timestamp = timestamp
        self.data_value = data_value

    def __unicode__(self):
        return u'%s for %s on time %s' % (self.row_key, self.data_value, self.timestamp)

class TimeSeriesCassandraDao():
    HOURLY_DATA_COLUMN_FAMILY_NAME = 'HourlyTimestampedData'
    STRING_INDEX_COLUMN_FAMILY_NAME = 'StringIndex'
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
        return pycassa.ColumnFamily(self.__pool, self.HOURLY_DATA_COLUMN_FAMILY_NAME)

    def __get_indexed_data_cf(self):
        return pycassa.ColumnFamily(self.__pool, self.HOURLY_DATA_COLUMN_FAMILY_NAME)

    def __get_string_index_cf(self):
        return pycassa.ColumnFamily(self.__pool, self.STRING_INDEX_COLUMN_FAMILY_NAME)

    def __build_row_key_for_hourly(self, source_id, data_name, utc_datetime):
        time_part = utc_datetime.strftime('%Y%m%d%H')
        return str(source_id+'-'+data_name+'-'+time_part)

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

    def __build_pycassa_insert_list(self, dto):
        utc_datetime = self.__datetime_to_utc(dto.timestamp)
        row_key = self.__build_row_key_for_hourly(dto.source_id, dto.data_name, utc_datetime)

        column_name = utc_datetime

        # Note: if you dont want to store it as unicode string, cast it to float
        # for example, and make sure to create the column family with 'float' as default validator
        # see http://cassandra.apache.org/doc/cql/CQL.html#CREATECOLUMNFAMILY and
        # http://cassandra.apache.org/doc/cql/CQL.html#storageTypes
        # You need a float/int validator to use conditional queries (ie greater than.. )
        column_value = u'%s' % dto.data_value

        return [row_key, column_name, column_value]

    def __build_pycassa_insert_tuple(self, dto):
        i_list = self.__build_pycassa_insert_list(dto)
        return (i_list[0], {i_list[1] : i_list[2]})

    ##
    ## Data storage interface
    ##
    ######################################################

    def insert_timestamped_data(self, timestamped_data_dto, create_indexes=False):
        i_list = self.__build_pycassa_insert_list(timestamped_data_dto)

        # Insert in hour bucket
        result = self.__get_hourly_data_cf().insert(i_list[0], {i_list[1] : i_list[2]})

        if create_indexes:
            row_key = i_list[0]         # the key to reference

            # Will return list of StringIndexDTO
            list_of_string_index_dtos = self.string_indexer.build_indexes_from_timstamped_dto(timestamped_data_dto)

            # Convert them to list of insertable tuples for batch insertion
            insert_tuples = dict()
            for dto in list_of_string_index_dtos:
                row_key = dto.row_key
                col_name_value_pair = {dto.timestamp : timestamped_data_dto.data_value}    # Note, all index dtos has same timestamp (which is the "column_name" for the timestamped data above)
                insert_tuples[row_key] = col_name_value_pair

            # And perform the insertion
            self.__get_string_index_cf().batch_insert(insert_tuples)

        return result

    def batch_insert_timestamped_data(self, list_of_timestamped_data_dtos):
        insert_tuples = dict()

        for dto in list_of_timestamped_data_dtos:
            tuple = self.__build_pycassa_insert_tuple(dto)
            row_key = tuple[0]
            col_name_value_pairs = insert_tuples.get(row_key, None)
            if not col_name_value_pairs:
                col_name_value_pairs = dict()
            col_name_value_pairs.update(tuple[1])
            insert_tuples[row_key] = col_name_value_pairs

        self.__get_hourly_data_cf().batch_insert(insert_tuples)

    def get_timestamped_data_by_search_string(self, source_id, data_name, search_string):
        substring = '_'.join(self.string_indexer.make_indexable_string(search_string).split())
        return self.get_timestamped_data_by_index(source_id, data_name, substring)

    def get_timestamped_data_by_index(self, source_id, data_name, substring):
        index_row_key = self.string_indexer.build_index_row_key(source_id, data_name, substring)
        return self.__get_string_index_cf().get(index_row_key, column_reversed=False, column_count=MAX_COLUMNS).items()

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
            row_key = self.__build_row_key_for_hourly(source_id, metric_name, a_datetime)
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


# A simple example of an indexer for the bucketed storage above, useful if
# you are storing strings in the buckets and want to make them searchable
#
# Idea: The TimeSeriesCassandraDao class stores any kind of data
# based on a row key that is assembled. Any row represent data during
# a specific hour, as explained above.
#
# The indexer class can then create aditional keys in another column family
# named 'StringIndex', representing references to the data stored in the buckets:
#
# Say we want to store this data, as a TimestampedDataDTO()
#  timestamp = 2012-12-24T18:12:33
#  source_id = 'the_kids'
#  data_name = 'log_info'
#  data_value = 'Santa is comming'
#
# The method __build_row_key_for_hourly() would return a row key for an hourly bucket
#  data_row_key = 2012122418-the_kids-data_name
# The column name is the timestamp and the value is of course the data_value
#
# Now, a typical scenario is to search for 'santa' or 'comming' given
# we want to search for stuff said by 'the_kids' with 'log_info' kind
# of messages. We kan then store a new column in the 'StringIndex'
# column family using the follwing row_key
#
# row_key = 'the_kids.log_info.santa' and add another column and value would be
# column_name = 2012-12-24T18:12:33
# column_value = 2012122418-the_kids-data_name
#
# So, a query for santa would yield all columns on row
#
# Using the column_value(s), we would query the Hourly buckets and ask for
# the data on the corresponding 'column_name' (which is the timestamp).
#
# A caveat: to be able to sarch not only for santa we would also insert
# indexes for 'santa', 'is', 'comming', 'santa is', 'is comming' and 'santa is comming'.
# The work of creating that array of indexable substrings is taken care of
# by the class below
#
#
class StringIndexer():
    white_list = string.letters + string.digits + ' '
    index_depth = 5
    #default_ttl = 60*60*24*30 # 30 days TTL as default

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
    def _build_substrings(self, string, depth):
        result = set()
        words = string.split()

        for d in range(0, depth):
            for i in range(0, len(words)):
                if i+d+1 > len(words):
                    continue
                current_words = words[i:i+d+1]
                result.add('_'.join(current_words))
        return result

    def build_index_row_key(self, source_id, data_name, substring):
        return source_id + '-' + data_name + '-' + substring

    # idea: could add flag to run the loop again but with ommited source_id and/or dataname to
    # return double and tripple amount of keys to make a global search available
    def build_indexes_from_timstamped_dto(self, dto):
        indexable_string = self.make_indexable_string(dto.data_value)

        substrings = self._build_substrings(indexable_string, self.index_depth)

        index_dtos = []
        for substring in substrings:
            index_row_key = self.build_index_row_key(dto.source_id, dto.data_name, substring)
            index_dto = StringIndexDTO(index_row_key, self.__datetime_to_utc(dto.timestamp), dto.data_value)
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
