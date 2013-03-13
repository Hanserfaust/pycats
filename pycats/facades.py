
from models import TimestampedDataDTO

# Thin facade to the pycats dao. Purpose is to show off how to use the TimeSeriesCassandraDao
# for something useful and make a clean API for a common case PyCats was designed for.
#
# The user-stories behind the implementation:
#
# - As a system designer, i want to store messages that occurs around the system. I want to store
#   it with a property identifying the source, a timestamp, a log-level and a message.
# - As a system maintainer, i would like to browse the logs given a time-span, independent of log level
#   or with a specific loglevel. Ie. show all errors in given timespan
# - As a system maintainer, i would like to search in the logs for a given pattern during a given time span
#   and log level
#
#
#
# Public methods is meant to store a message with a specific log level and then fetch them
# either through a timespan or by free-text search. Something that would be trivial in SQL
# but on Cassandra with the big-data hat on, we employ data duplication and de-normalization
# to improve read performance. Write more, read fast..:).
#
# Now, lets improve for
#
class UnsupportedLogLevelException(Exception):
    pass

class LogLoadingArgumentErrorException(Exception):
    pass

class LogMessageDTO():
    def __init__(self, source_context, log_source, timestamp, level, message):
        self.source_context = source_context
        self.log_source = log_source
        self.timestamp = timestamp
        self.level = level
        self.message = message

    def __unicode__(self):
        return u'%s from %s.%s (%s) : %s' % (self.timestamp, self.source_context, self.log_source, self.level, self.message)

GLOBAL_CONTEXT = '__clg_glb__'
ANY_LEVEL = '__clg_any__'

seconds_per_day = 60*60*24

class CassandraLogger():

    supported_log_levels = ['info', 'warn', 'error', 'debug']

    # Filter for the three context. no need to store info and debug on higher context is a good assumption
    levels_for_exact            = ['info', 'warn', 'error', 'debug']
    levels_for_source_context   = ['warn', 'error']
    levels_for_global_context   = ['warn', 'error']

    # Internal hidden data_names, in case the source stores other stuff
    ext_level_to_internal = {'info' : '__clg_info__',
                            'warn' : '__clg_warn__',
                            'error' : '__clg_error__',
                            'debug' : '__clg_debug__',
                            }

    # It is probably a good idea to use a separate DAO in a separate keyspace for the logging purposes
    # One idea could be to use the nice Cassandra feature of TTL on the log messages,
    # # ie. don't keep logs or indexes for more than 90 days or so.
    def __init__(self, pycats_dao, ttl_days_for_exact=90, ttl_days_for_source_context=30, ttl_days_for_global_context=7, levels_for_source=None, levels_for_global=None):
        self.dao = pycats_dao
        self.ttl_secs_for_exact             = seconds_per_day * ttl_days_for_exact
        self.ttl_secs_for_source_context    = seconds_per_day * ttl_days_for_source_context
        self.ttl_secs_for_global_context    = seconds_per_day * ttl_days_for_global_context

        if levels_for_source:
            self.levels_for_source_context = levels_for_source
        if levels_for_global:
            self.levels_for_global_context = levels_for_global

    def _external_to_internal_message(self, source_context, log_source, level, message):
        return u'%s|%s|%s|%s' % (source_context, log_source, level, message)

    def _internal_message_to_list(self, message):
        return message.split('|')

    def _build_pycats_source_id(self, source_context, log_source):
        return source_context+'.'+log_source

    def info(self, source_context, log_source, timestamp, message):
        return self.log(source_context, log_source, timestamp, 'info', message)

    def warn(self, source_context, log_source, timestamp, message):
        return self.log(source_context, log_source, timestamp, 'warn', message)

    def error(self, source_context, log_source, timestamp, message):
        return self.log(source_context, log_source, timestamp, 'error', message)

    def debug(self, source_context, log_source, timestamp, message):
        return self.log(source_context, log_source, timestamp, 'debug', message)

    # source_context offers a higher level of grouping. if you dont need that level
    # just provide a string that all calls shares, such as 'app'. It could be used
    # for concepts such as projects, namespaces, user groups, companys etc
    def log(self, source_context, log_source, timestamp, level, message):

        if level not in self.supported_log_levels:
            raise UnsupportedLogLevelException('Unsupported log level \'%s\'' % level)

        # We do this to keep source_id and level tightly coupled, since we also store on a global-tag
        # and a level-independent tag
        internal_message = self._external_to_internal_message(source_context, log_source, level, message)

        # big-data-weirdness-deluxe you say? No, we quadruple the message for fast key to value lookup
        # for a number of common cases :
        dto_source_and_level    = None
        dto_source_and_any      = None
        dto_context_and_level   = None
        dto_context_and_any     = None
        dto_global_and_level    = None
        dto_global_and_any      = None

        level_as_data_name = self.ext_level_to_internal[level]

        if level in self.levels_for_exact:
            # ... load based on exact source and exact level
            dto_source_and_level = TimestampedDataDTO(self._build_pycats_source_id(source_context,log_source), timestamp, level_as_data_name, internal_message, message)
            # ... load based on any level but exact source
            dto_source_and_any =  TimestampedDataDTO(self._build_pycats_source_id(source_context,log_source), timestamp, ANY_LEVEL, internal_message, message)
        if level in self.levels_for_source_context:
            # ... load logs independently of ID within context, but with exact log level
            dto_context_and_level = TimestampedDataDTO(source_context, timestamp, level_as_data_name, internal_message, message)
            # ... load logs independently of ID within context, and independent of level
            dto_context_and_any =  TimestampedDataDTO(source_context, timestamp, ANY_LEVEL, internal_message, message)
        if level in self.levels_for_global_context:
            # ... load logs globally with specific level
            dto_global_and_level = TimestampedDataDTO(GLOBAL_CONTEXT, timestamp, level_as_data_name, internal_message, message)
            # ... load logs globally independent of level
            dto_global_and_any =  TimestampedDataDTO(GLOBAL_CONTEXT, timestamp, ANY_LEVEL, internal_message, message)

        # If same TTL, store with one call
        if self.ttl_secs_for_exact == self.ttl_secs_for_source_context and self.ttl_secs_for_exact == self.ttl_secs_for_global_context:
            self.dao.batch_insert_indexable_text_as_blob_data_and_insert_indexes([dto_source_and_level, dto_source_and_any, dto_context_and_level, dto_context_and_any, dto_global_and_level, dto_global_and_any], self.ttl_secs_for_exact)
        else:
            # i know.. could check if first pair has same ttl as third, and third as second
            self.dao.batch_insert_indexable_text_as_blob_data_and_insert_indexes([dto_source_and_level, dto_source_and_any], self.ttl_secs_for_exact)
            self.dao.batch_insert_indexable_text_as_blob_data_and_insert_indexes([dto_context_and_level, dto_context_and_any], self.ttl_secs_for_source_context)
            self.dao.batch_insert_indexable_text_as_blob_data_and_insert_indexes([dto_global_and_level, dto_global_and_any], self.ttl_secs_for_global_context)

    # Note, if log_source is provided a source_context must also be provided
    #
    # start_date and end_date can always be provided
    def __load(self, free_text=None, source_context=None, log_source=None, level=None, start_date=None, end_date=None):

        if log_source and not source_context:
            raise LogLoadingArgumentErrorException('If log source is specified, a source context must also be provided.')

        if not free_text and not start_date and not end_date:
            raise LogLoadingArgumentErrorException('Neither free text, nor time-span was supplied.')

        if level:
            data_name = self.ext_level_to_internal[level]
        else:
            data_name = ANY_LEVEL

        if source_context and log_source:
            source_id = self._build_pycats_source_id(source_context,log_source)
        elif source_context:
            source_id = source_context
        else:
            source_id = GLOBAL_CONTEXT

        if free_text:
            list_of_tuples = self.dao.get_blobs_by_free_text_index(source_id, data_name, free_text, start_date, end_date)
        else:
            list_of_tuples = self.dao.get_timetamped_data_range(source_id, data_name, start_date, end_date)

        result = list()

        # Note how we store an internal message string that needs to be split
        for tup in list_of_tuples:
            timestamp = tup[0]
            message = tup[1]
            l = self._internal_message_to_list(message)
            result.append(LogMessageDTO(l[0],l[1],timestamp,l[2],l[3]))

        # Should be ordered by ascending time
        return result

    def free_text_search(self, free_text=None, source_context=None, log_source=None, level=None, start_date=None, end_date=None):
        return self.__load(free_text, source_context, log_source, level, start_date, end_date)

    def load_by_date_range(self, source_context=None, log_source=None, level=None, start_date=None, end_date=None):
        return self.__load(free_text=None, source_context=source_context, log_source=log_source, level=level, start_date=start_date, end_date=end_date)
