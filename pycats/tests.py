# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from pycats.pycats import TimeSeriesCassandraDao, TimestampedDataDTO, BlobIndexDTO
from indexers import StringIndexer
from facades import CassandraLogger
import unittest
import yaml
from profilehooks import profile

import cProfile
# Tips to get started with pycats:
#
# 0) Create test_settings.yaml, it has one line like this: "cassandra_host1 : ec2-somenumbers123.compute.amazonaws.com" without quotes
#    in the same directory as tests.py (this file).
#
# 1) Use a Datastax prepared AMI on Amazon EC2 to get one or several cassandra hosts up and running
#     Follow the guide here for example (be ware of new versions of the doc though):
#     http://www.datastax.com/docs/1.2/install/install_ami
# 2) Make sure you have access to the cassandra Thrift ports on the instance (configure security groups correctly)
# 3) SSH to the cassandra host and execute the CQL needed to prepare the Columnfamilies, ie:
#
# Ref: http://cassandra.apache.org/doc/cql/CQL.html#CREATECOLUMNFAMILY
#
#    ubuntu@ip-10-10-10-2:~$ cqlsh
#    Connected to CassandraTest at localhost:9160.
#    [cqlsh 2.2.0 | Cassandra 1.1.5 | CQL spec 2.0.0 | Thrift protocol 19.32.0]
#    Use HELP for help.
#    cqlsh> CREATE KEYSPACE pycats_test_space WITH strategy_class = 'SimpleStrategy' AND strategy_options:replication_factor = '1';
#    cqlsh> use pycats_test_space;
#    cqlsh:pycats_test_space> CREATE COLUMNFAMILY HourlyTimestampedData (KEY ascii PRIMARY KEY) WITH comparator=bigint;
#    cqlsh:pycats_test_space> CREATE COLUMNFAMILY BlobData (KEY ascii PRIMARY KEY) WITH comparator=timestamp;
#    cqlsh:pycats_test_space> CREATE COLUMNFAMILY BlobDataIndex (KEY text PRIMARY KEY) WITH comparator=timestamp AND default_validation=text;
#    cqlsh:pycats_test_space>
#
# 4) Enter the URLs to your cassandra instances in the file test_settings.yaml. (rememeber not to commit this
# 5) Run the tests, The tests will insert data and then load it in various scenarios.
# 6) You can drop the key space pycats_test_space afterwards.
#    cqlsh> DROP KEYSPACE pycats_test_space;
#
#
class PyCatsIntegrationTestBase(unittest.TestCase):

    #
    def setUp(self):
        # test_settings.yaml has one line like this: "cassandra_host1 : ec2-somenumbers123.compute.amazonaws.com" without quotes
        f = open('test_settings.yaml')
        settings = yaml.load(f)
        f.close()
        cassandra_host1 = settings['cassandra_host1']

        # Test constants
        self.cassandra_hosts = [cassandra_host1]
        self.key_space = 'pycats_test_space'
        # Use a Django cache to test the cache mechanism
        self.cache = None

        # Run tests with disable_high_res_column_name_randomization=True and false, erase HourlyTimestampedData in between
        self.dao = TimeSeriesCassandraDao(self.cassandra_hosts, self.key_space, cache=self.cache, disable_high_res_column_name_randomization=True)
        self.insert_the_test_range_into_live_db = True

class TimeSeriesCassandraDaoIntegrationTest(PyCatsIntegrationTestBase):

    def __insert_range_of_metrics(self, source_id, value_name, start_datetime, end_datetime, batch_insert=False):
        #
        # Insert test metrics over the days specified during setUP
        # one hour apart
        #
        print 'Inserting test data from %s to %s' % (start_datetime, end_datetime)
        curr_datetime = start_datetime

        values_inserted = 0
        value = 0

        # Build list of DTOs
        dtos = list()
        while curr_datetime <= end_datetime:
            if self.insert_the_test_range_into_live_db:
                dto = TimestampedDataDTO(source_id, curr_datetime, value_name, str(value))
                if not batch_insert:
                    self.dao.insert_timestamped_data(dto)

                dtos.append(dto)
            curr_datetime = curr_datetime + timedelta(minutes=20)
            values_inserted += 1
            value += 1

        # And batch insert
        if batch_insert:
            self.dao.batch_insert_timestamped_data(dtos)
        return dtos

    def test_should_pass_a_datetime_through_high_res_column_generator_and_back(self):
        a_datetime = datetime.strptime('1979-06-20T06:06:07.213462', '%Y-%m-%dT%H:%M:%S.%f')
        start_of_hour = datetime.strptime('1979-06-20T06:00:00.0000', '%Y-%m-%dT%H:%M:%S.%f')

        self.assertNotEqual(a_datetime, start_of_hour)

        high_res_column_name = self.dao.get_high_res_column_name(a_datetime)

        resulting_datetime = self.dao.highres_to_utc_datetime(start_of_hour, high_res_column_name)

        self.assertEqual(a_datetime, resulting_datetime)

    def test_should_floor_timestamp_to_hour_correctly(self):

        a_datetime                  = datetime.strptime('1979-06-20T06:06:07.213462', '%Y-%m-%dT%H:%M:%S.%f')
        expected_floored_datetime   = datetime.strptime('1979-06-20T06:00:00.0000', '%Y-%m-%dT%H:%M:%S.%f')

        resulting_datetime = self.dao.floor_timestamp_to_hour(a_datetime)

        self.assertEqual(expected_floored_datetime, resulting_datetime)

    def test_should_load_all_data_for_full_range_using_batch_insert(self):
        source_id = 'unittest1'
        test_metric = 'ramp_height'
        start_datetime = datetime.strptime('1979-12-31T22:00:00', '%Y-%m-%dT%H:%M:%S')
        end_datetime = datetime.strptime('1980-01-01T03:00:00', '%Y-%m-%dT%H:%M:%S')

        values_inserted = self.__insert_range_of_metrics(source_id, test_metric, start_datetime, end_datetime, batch_insert=True)

        # All values should be received for this date range
        result = self.dao.get_timetamped_data_range(source_id, test_metric, start_datetime, end_datetime)

        # First assert length
        self.assertEqual(len(result), len(values_inserted))

        for i in range(0, len(values_inserted)):
            self.assertEqual(result[i][0], values_inserted[i].timestamp)
            self.assertEqual(result[i][1], values_inserted[i].data_value)

    # NOTE: dash (-) in source_id and test_metric to test that it does not disturb pycats row-key model
    def test_should_load_all_data_for_full_range_using_single_insert(self):
        source_id = 'unittest2-'
        test_metric = 'ramp-height'
        start_datetime = datetime.strptime('1979-12-31T22:00:00', '%Y-%m-%dT%H:%M:%S')
        end_datetime = datetime.strptime('1980-01-02T03:00:00', '%Y-%m-%dT%H:%M:%S')

        values_inserted = self.__insert_range_of_metrics(source_id, test_metric, start_datetime, end_datetime, batch_insert=False)

        # Should miss the first and last values
        result = self.dao.get_timetamped_data_range(source_id, test_metric, start_datetime, end_datetime)

        # First assert length
        self.assertEqual(len(result), len(values_inserted))

        for i in range(1, len(values_inserted)):
            self.assertEqual(result[i][0], values_inserted[i].timestamp)
            self.assertEqual(result[i][1], values_inserted[i].data_value)

    def test_should_load_all_data_for_data_range_with_no_data_in_some_hours(self):
        source_id = 'unittest2_4'
        test_metric = 'ramp_height'
        start1 = datetime.strptime('1990-01-01T10:00:00', '%Y-%m-%dT%H:%M:%S')
        end1 = datetime.strptime('1990-01-01T12:10:00', '%Y-%m-%dT%H:%M:%S')
        # Note hole between 3 and 9
        start2 = datetime.strptime('1990-01-01T15:00:00', '%Y-%m-%dT%H:%M:%S')
        end2 = datetime.strptime('1990-01-01T17:20:00', '%Y-%m-%dT%H:%M:%S')

        first_inserted = self.__insert_range_of_metrics(source_id, test_metric, start1, end1, batch_insert=True)
        second_inserted = self.__insert_range_of_metrics(source_id, test_metric, start2, end2, batch_insert=True)

        values_inserted = list()
        values_inserted.extend(first_inserted)
        values_inserted.extend(second_inserted)

        # Should miss the first and last values
        result = self.dao.get_timetamped_data_range(source_id, test_metric, start1, end2)

        # First assert length
        self.assertEqual(len(result), len(values_inserted))

        for i in range(1, len(values_inserted)):
            self.assertEqual(result[i][0], values_inserted[i].timestamp)
            self.assertEqual(result[i][1], values_inserted[i].data_value)

    def test_should_load_correct_data_for_partial_range_using_batch_insert(self):
        source_id = 'unittest3'
        test_metric = 'ramp_height'
        start_datetime = datetime.strptime('1979-12-31T22:00:00', '%Y-%m-%dT%H:%M:%S')
        end_datetime = datetime.strptime('1980-01-02T03:00:00', '%Y-%m-%dT%H:%M:%S')

        values_inserted = self.__insert_range_of_metrics(source_id, test_metric, start_datetime, end_datetime, batch_insert=True)

        # Should miss the first and last values that we just inserted above
        result = self.dao.get_timetamped_data_range(source_id, test_metric, start_datetime+ timedelta(minutes=1), end_datetime-timedelta(minutes=1))

        # First assert length
        self.assertEqual(len(result), len(values_inserted)-2)

        # iterate over result, correct value should be found with offset=1
        for i in range(0, len(result)):
            self.assertEqual(result[i][0], values_inserted[i+1].timestamp)
            self.assertEqual(result[i][1], values_inserted[i+1].data_value)

class IndexedBlobsIntegrationTests(PyCatsIntegrationTestBase):
    def test_should_store_a_unicode_string_and_corresponding_indexes_and_load_by_date_range_and_index(self):
        source_id = 'indexed_test_1'
        data_name = 'evil_text'
        data_value_unicode = u'Woe to you o örth ánd sea. For the devil sends the beast with wrath'
        #data_value_utf8 = data_value_unicode.encode('utf-8')
        beastly_timestamp = datetime.strptime('1982-03-01T06:06:06', '%Y-%m-%dT%H:%M:%S')

        dto = TimestampedDataDTO(source_id, beastly_timestamp, data_name, data_value_unicode)
        self.dao.insert_timestamped_data(dto)
        self.dao.insert_indexable_text_as_blob_data_and_insert_index(dto)

        # All values should be received for this date range
        result = self.dao.get_timetamped_data_range(source_id, data_name, beastly_timestamp- timedelta(minutes=1), beastly_timestamp+timedelta(minutes=1))
        #print result

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], beastly_timestamp)
        self.assertEqual(result[0][1], data_value_unicode)

        # Now make a free text search
        search_string = 'sea'
        result = self.dao.get_blobs_by_free_text_index(source_id, data_name, search_string)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], beastly_timestamp)
        self.assertEqual(result[0][1], data_value_unicode)

    def test_should_store_and_load_a_complex_string_and_corresponding_indexes_and_load_by_index(self):
        source_id = 'indexed_test_2'
        data_name = 'evil_text'
        data_value = u'Tue Mar  5 14:41:33 Hans-Eklunds-MacBook-Pro com.apple.backupd-auto[3780] <Notice>: Not starting scheduled Time Machine backup - time machine destination not resolvable.'
        beastly_timestamp = datetime.strptime('1982-03-01T06:06:06', '%Y-%m-%dT%H:%M:%S')

        dto = TimestampedDataDTO(source_id, beastly_timestamp, data_name, data_value)
        self.dao.insert_timestamped_data(dto)
        self.dao.insert_indexable_text_as_blob_data_and_insert_index(dto)

        # Now make a couple of free text search
        search_string = 'Notice'
        result = self.dao.get_blobs_by_free_text_index(source_id,data_name, search_string)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], beastly_timestamp)
        self.assertEqual(result[0][1], data_value)

        search_string = 'hans eklunds MacBook pro'
        result = self.dao.get_blobs_by_free_text_index(source_id,data_name, search_string)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], beastly_timestamp)
        self.assertEqual(result[0][1], data_value)

        search_string = 'backupd-auto[3780]'
        result = self.dao.get_blobs_by_free_text_index(source_id,data_name, search_string)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], beastly_timestamp)
        self.assertEqual(result[0][1], data_value)

        # Test what happens on no hits
        search_string = 'w000000000t'
        result = self.dao.get_blobs_by_free_text_index(source_id,data_name, search_string)
        self.assertEqual(len(result), 0)

    def test_should_store_muliple_similar_complex_strings_with_different_timestamps_saved_out_of_order_should_be_loaded_in_order(self):
        source_id = 'indexed_test_3'
        data_name = 'evil_text'

        data_value1 = u'Hans-Eklunds-MacBook-Pro com.apple.backupd-auto[3780] <Notice>: Not stârting scheduled Time Machine backup - time machine destination not resolvable.'
        beastly_timestamp1 = datetime.strptime('1982-03-01T06:06:06', '%Y-%m-%dT%H:%M:%S')
        dto1 = TimestampedDataDTO(source_id, beastly_timestamp1, data_name, data_value1)

        data_value2 = u'Hans-Smiths-MacBook-Pro com.apple.backupd-auto[3780] <Notice>: Not starting scheduled Time Machine backup - time machine destination not resolvable.'
        beastly_timestamp2 = datetime.strptime('1982-03-01T06:06:08', '%Y-%m-%dT%H:%M:%S')
        dto2 = TimestampedDataDTO(source_id, beastly_timestamp2, data_name, data_value2)

        data_value3 = u'Hans-Johnssons-MacBook-Pro com.apple.backupd-auto[3780] <Notice>: Not starting scheduled Time Machine backup - time machine destination not resolvable.'
        beastly_timestamp3 = datetime.strptime('1982-03-01T06:06:07', '%Y-%m-%dT%H:%M:%S')
        dto3 = TimestampedDataDTO(source_id, beastly_timestamp3, data_name, data_value3)

        data_value4 = u'time machine destination not recoverable.'
        beastly_timestamp4 = datetime.strptime('1982-03-01T06:06:09', '%Y-%m-%dT%H:%M:%S')
        dto4 = TimestampedDataDTO(source_id, beastly_timestamp4, data_name, data_value4)

        self.dao.insert_timestamped_data(dto1)
        self.dao.insert_indexable_text_as_blob_data_and_insert_index(dto1)
        self.dao.insert_timestamped_data(dto2)
        self.dao.insert_indexable_text_as_blob_data_and_insert_index(dto2)
        self.dao.insert_timestamped_data(dto3)
        self.dao.insert_indexable_text_as_blob_data_and_insert_index(dto3)
        self.dao.insert_timestamped_data(dto4)
        self.dao.insert_indexable_text_as_blob_data_and_insert_index(dto4)

        # Three should be found, the last one should not be found by this search
        search_string = 'Notice'
        results = self.dao.get_blobs_by_free_text_index(source_id, data_name, search_string)
        self.assertEqual(len(results), 3)

        # Assert correct order
        self.assertTrue(results[0][0] < results[1][0])
        self.assertTrue(results[1][0] < results[2][0])

        #for result in results:
            #print result

        # Assure only one hit on this unique name
        search_string = 'Hans-Smiths-MacBook'
        results = self.dao.get_blobs_by_free_text_index(source_id,data_name, search_string)
        self.assertEqual(len(results), 1)

    def test_should_store_arabic_and_store_manual_index_and_load_by_free_text_search(self):
        arabic_text = u'مساعدة في تصليح كود'
        source_id = 'indexed_test_5'
        data_name = 'evil_text2'
        data_value_unicode = arabic_text
        #data_value_utf8 = data_value_unicode.encode('utf-8')
        beastly_timestamp = datetime.strptime('1988-03-01T06:06:11', '%Y-%m-%dT%H:%M:%S')

        dto = TimestampedDataDTO(source_id, beastly_timestamp, data_name, data_value_unicode)

        # Insert into timeseries shard
        self.dao.insert_timestamped_data(dto)

        # No auto-index for this baby, create a manual index
        #self.dao.insert_indexable_text_as_blob_data_and_insert_index(dto)
        blob_row_key_index = self.dao.insert_blob_data(dto)

        manual_indexes = list()

        # Any search-strings in unicode must be converted to UTF-8 to conform with the keys in the index entries in Cassandra
        manual_indexes.append(BlobIndexDTO(source_id, data_name, u'árabic'.encode('utf-8'), beastly_timestamp, blob_row_key_index))
        manual_indexes.append(BlobIndexDTO(source_id, data_name, u'works'.encode('utf-8'), beastly_timestamp, blob_row_key_index))
        self.dao.batch_insert_indexes(manual_indexes)

        # All values should be received for this date range
        result = self.dao.get_timetamped_data_range(source_id, data_name, beastly_timestamp- timedelta(minutes=1), beastly_timestamp+timedelta(minutes=1))
        #print result

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], beastly_timestamp)
        self.assertEqual(result[0][1], data_value_unicode)

        # Now make a free text search
        search_string = u'works'
        result = self.dao.get_blobs_by_free_text_index(source_id, data_name, search_string)

        self.assertIsNotNone(result)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], beastly_timestamp)
        self.assertEqual(result[0][1], data_value_unicode)

    def test_should_store_data_for_several_data_names_and_load_by_multi_data_index_search(self):
        source_id = 'indexed_test_6'
        data_name1 = 'evil3_text'
        data_name2 = 'bad3_text'
        data_name3 = 'nasty3_text'

        # Note, they differ slightly, but has common words so search can hit all of them
        data_value_unicode1 = u'Woe to you o örth ánd sea. For the devil sends the beast with wrath'
        data_value_unicode2 = u'Darn to you o örth ánd sea. For the mother sends the beast with wrath'
        data_value_unicode3 = u'Hey to you o örth ánd sea. For the bushes sends the beast with wrath'
        #data_value_utf8 = data_value_unicode.encode('utf-8')
        beastly_timestamp1 = datetime.strptime('1982-03-01T06:06:06', '%Y-%m-%dT%H:%M:%S')
        beastly_timestamp2 = datetime.strptime('1982-03-01T06:06:07', '%Y-%m-%dT%H:%M:%S')
        beastly_timestamp3 = datetime.strptime('1982-03-01T06:06:08', '%Y-%m-%dT%H:%M:%S')

        dto1 = TimestampedDataDTO(source_id, beastly_timestamp1, data_name1, data_value_unicode1)
        self.dao.insert_indexable_text_as_blob_data_and_insert_index(dto1)
        dto2 = TimestampedDataDTO(source_id, beastly_timestamp2, data_name2, data_value_unicode2)
        self.dao.insert_indexable_text_as_blob_data_and_insert_index(dto2)
        dto3 = TimestampedDataDTO(source_id, beastly_timestamp3, data_name3, data_value_unicode3)
        self.dao.insert_indexable_text_as_blob_data_and_insert_index(dto3)

        # Now make a free text search
        search_string = 'sea'
        result = self.dao.get_blobs_multi_data_by_free_text_index(source_id, [data_name1, data_name2, data_name3], search_string)

        self.assertEqual(len(result), 3)
        self.assertEqual(result[0][0], beastly_timestamp1)
        self.assertEqual(result[0][1], data_value_unicode1)
        self.assertEqual(result[1][0], beastly_timestamp2)
        self.assertEqual(result[1][1], data_value_unicode2)
        self.assertEqual(result[2][0], beastly_timestamp3)
        self.assertEqual(result[2][1], data_value_unicode3)

    def test_should_store_data_for_several_data_names_and_load_by_index_with_date_range(self):
        source_id = 'unittests.indexed_test_7'
        data_name1 = 'evil3_text'
        data_name2 = 'bad3_text'
        data_name3 = 'nasty3_text'

        # Note, they differ slightly, but has common words so search can hit all of them
        data_value_unicode1 = u'Woe to you o örth ánd sea. For the devil sends the beast with wrath'
        data_value_unicode2 = u'Darn to you o örth ánd sea. For the mother sends the beast with wrath'
        data_value_unicode3 = u'Hey to you o örth ánd sea. For the bushes sends the beast with wrath'
        #data_value_utf8 = data_value_unicode.encode('utf-8')
        beastly_timestamp1 = datetime.strptime('1982-03-01T06:06:05', '%Y-%m-%dT%H:%M:%S')
        beastly_timestamp2 = datetime.strptime('1982-03-01T06:07:05', '%Y-%m-%dT%H:%M:%S')
        beastly_timestamp3 = datetime.strptime('1982-03-01T06:08:05', '%Y-%m-%dT%H:%M:%S')

        dto1 = TimestampedDataDTO(source_id, beastly_timestamp1, data_name1, data_value_unicode1)
        self.dao.insert_indexable_text_as_blob_data_and_insert_index(dto1)
        dto2 = TimestampedDataDTO(source_id, beastly_timestamp2, data_name2, data_value_unicode2)
        self.dao.insert_indexable_text_as_blob_data_and_insert_index(dto2)
        dto3 = TimestampedDataDTO(source_id, beastly_timestamp3, data_name3, data_value_unicode3)
        self.dao.insert_indexable_text_as_blob_data_and_insert_index(dto3)

        # Now make a free text search
        search_string = 'sea'
        start_time = datetime.strptime('1982-03-01T06:07:00', '%Y-%m-%dT%H:%M:%S')
        end_time = datetime.strptime('1982-03-01T06:07:10', '%Y-%m-%dT%H:%M:%S')
        result = self.dao.get_blobs_multi_data_by_free_text_index(source_id, [data_name1, data_name2, data_name3], search_string, start_time, end_time)

        # Should only find middle instance for given range
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], beastly_timestamp2)
        self.assertEqual(result[0][1], data_value_unicode2)

        # Now make same free text search with something that should NOT hit
        search_string = 'volvo'
        start_time = datetime.strptime('1982-03-01T06:07:00', '%Y-%m-%dT%H:%M:%S')
        end_time = datetime.strptime('1982-03-01T06:07:10', '%Y-%m-%dT%H:%M:%S')
        result = self.dao.get_blobs_multi_data_by_free_text_index(source_id, [data_name1, data_name2, data_name3], search_string, start_time, end_time)

        # Should only find middle instance for given range
        self.assertEqual(len(result), 0)

class StringIndexerTest(unittest.TestCase):
    test_strings = ['<1921___.bg three cats!Left__home(early)-In.Two.CARS', 'One man left Home early!!', 'two Woman left homE Late?', 'one Car_turned Left at Our HOME']
    string_indxer = None

    def setUp(self):
        self.string_indexer = StringIndexer()

    def test_should_split_simple_string_into_one_indexable_word(self):

        # Given
        test_string = 'sea.'

        # When
        result = self.string_indexer.strip_and_lower(test_string)

        # Then
        expected_result = 'sea'
        self.assertEqual(expected_result, result)

    def test_should_split_complex_string_into_indexable_words(self):

        # Given
        test_string = u'<1921___.bg three cäts!Left__hôme(early)-In.Two.CARS really?'

        # When
        result = self.string_indexer.strip_and_lower(test_string)

        # Then
        expected_result = '1921 bg three c\xc3\xa4ts left h\xc3\xb4me early in two cars really'
        self.assertEqual(expected_result, result)


    def test_should_return_indexable_substrings_given_depth_1(self):

        # Given
        test_string = 'hello indexed words'

        # When
        result = self.string_indexer._build_substrings(test_string, 1)

        # Then
        expected_result = set(['hello', 'indexed', 'words'])
        self.assertEqual(expected_result, result)

    def test_should_return_indexable_substrings_given_depth_2(self):

        # Given
        test_string = 'hello indexed words'

        # When
        result = self.string_indexer._build_substrings(test_string, 2)

        # Then
        expected_result = set(['hello', 'indexed', 'words', 'hello indexed', 'indexed words'])
        self.assertEqual(expected_result, result)

    def test_should_return_indexable_substrings_for_3_word_string_given_depth_5(self):

        # Given
        test_string = 'hello indexed words'

        # When
        result = self.string_indexer._build_substrings(test_string, 5)

        # Then
        expected_result = set(['hello', 'indexed', 'words', 'hello indexed', 'indexed words', 'hello indexed words'])
        self.assertEqual(expected_result, result)

    def test_should_return_indexable_substrings_for_5_word_string_given_depth_3(self):

        # Given
        test_string = 'hello indexed words of yore'

        # When
        result = self.string_indexer._build_substrings(test_string, 3)

        # Then
        expected_result = set(['hello', 'indexed', 'words', 'of', 'yore', 'hello indexed', 'indexed words', 'words of', 'of yore', 'hello indexed words', 'indexed words of', 'words of yore'])
        self.assertEqual(expected_result, result)

    def test_should_only_print_nr_of_words_in_result_to_show_how_many_indexes_are_needed_for_a_large_string_given_a_depth(self):
        # Given
        test_string = "Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum."
        cleaned_test_string = self.string_indexer.strip_and_lower(test_string)

        # When
        depth = 2
        result = self.string_indexer._build_substrings(cleaned_test_string, depth)

        # Then
        print u'A %s word scentence yielded %s indexes given depth %s' % (len(cleaned_test_string.split()), len(result), depth)

    def test_should_return_build_actual_indexes_from_string_dto(self):

        # Given
        test_string = 'hello indexed words of yore'
        test_row_key = 'magic_key_123'
        dto = TimestampedDataDTO('the_kids', datetime.utcnow(), 'log_text', test_string)

        # When
        result = self.string_indexer.build_indexes_from_timstamped_dto(dto, test_row_key)

        # Then
        for index_dto in result:
            print u'Index: %s' % index_dto

    def test_should_store_string_and_load_by_key(self):
        pass

    def test_should_store_a_few_similar_string_and_their_indexes_and_load_all_by_index(self):
        pass

class CassandraLoggerTest(PyCatsIntegrationTestBase):

    def __assert_log_message(self, lm, source_context, log_source, timestamp, level, message):
        self.assertEqual(lm.source_context, source_context)
        self.assertEqual(lm.log_source, log_source)
        self.assertEqual(lm.timestamp, timestamp)
        self.assertEqual(lm.level, level)
        self.assertEqual(lm.message, message)

    def test_should_log_one_row_and_load_using_all_contexts(self):
        # Given
        logger = CassandraLogger(self.dao)

        source_context = 'CassandraLoggerTest1'
        log_source = 'unittest1'
        timestamp = datetime.strptime('1979-06-20T06:06:06.20', '%Y-%m-%dT%H:%M:%S.%f')
        level = 'warn'
        message = u'This is a log message from the erste unit test.'

        # When
        logger.log(source_context, log_source, timestamp, level, message)

        # Then
        free_text = 'erste'

        # 1. Find by exact context, note it returns list of LogMessageDTOs from the facade module
        result = logger.free_text_search(free_text, source_context, log_source, level)
        self.assertEqual(len(result), 1)
        self.__assert_log_message(result[0], source_context, log_source, timestamp, level, message)

        # 2 Find by looser context
        result = logger.free_text_search(free_text, source_context, None, level)
        self.assertEqual(len(result), 1)
        self.__assert_log_message(result[0], source_context, log_source, timestamp, level, message)

        # 3 Find by even more loose context, global but still within the 'info'-level
        result = logger.free_text_search(free_text, None, None, level)
        self.assertEqual(len(result), 1)
        self.__assert_log_message(result[0], source_context, log_source, timestamp, level, message)

        # 4 Find in über-global context
        result = logger.free_text_search(free_text)
        self.assertEqual(len(result), 1)
        self.__assert_log_message(result[0], source_context, log_source, timestamp, level, message)

    #@profile(immediate=True)
    def test_should_log_one_row_for_three_sources_and_three_should_be_found_within_higher_contexts(self):
        # Given
        logger = CassandraLogger(self.dao)

        source_context = 'CassandraLoggerTest2'
        log_source1 = 'unittest1_1'
        log_source2 = 'unittest1_2'
        log_source3 = 'unittest1_3'
        timestamp1 = datetime.strptime('1979-06-20T06:06:06.20', '%Y-%m-%dT%H:%M:%S.%f')
        timestamp2 = datetime.strptime('1979-06-20T06:06:06.21', '%Y-%m-%dT%H:%M:%S.%f')
        timestamp3 = datetime.strptime('1979-06-20T06:06:06.22', '%Y-%m-%dT%H:%M:%S.%f')
        level = 'warn'
        message = u'This is a log message from the second unit test.'

        # When
        logger.log(source_context, log_source1, timestamp1, level, message)
        logger.log(source_context, log_source2, timestamp2, level, message)
        logger.log(source_context, log_source3, timestamp3, level, message)

        # Then...
        free_text = 'second'

        # 1. Find by exact context, should return 1
        result = logger.free_text_search(free_text, source_context, log_source1, level)
        self.assertEqual(len(result), 1)
        self.__assert_log_message(result[0], source_context, log_source1, timestamp1, level, message)

        # 2 Find by looser context, should find all three now
        result = logger.free_text_search(free_text, source_context, None, level)
        self.assertEqual(len(result), 3)
        self.__assert_log_message(result[0], source_context, log_source1, timestamp1, level, message)
        self.__assert_log_message(result[1], source_context, log_source2, timestamp2, level, message)
        self.__assert_log_message(result[2], source_context, log_source3, timestamp3, level, message)

        # 3 Find by even more loose context, global but still within the 'info'-level
        result = logger.free_text_search(free_text, None, None, level)
        self.assertEqual(len(result), 3)
        self.__assert_log_message(result[0], source_context, log_source1, timestamp1, level, message)
        self.__assert_log_message(result[1], source_context, log_source2, timestamp2, level, message)
        self.__assert_log_message(result[2], source_context, log_source3, timestamp3, level, message)

        # 4 Find in über-global context
        result = logger.free_text_search(free_text)
        self.assertEqual(len(result), 3)
        self.__assert_log_message(result[0], source_context, log_source1, timestamp1, level, message)
        self.__assert_log_message(result[1], source_context, log_source2, timestamp2, level, message)
        self.__assert_log_message(result[2], source_context, log_source3, timestamp3, level, message)

    def test_should_log_tree_row_for_three_sources_and_load_by_date_range(self):
        # Given
        logger = CassandraLogger(self.dao)

        source_context = 'CassandraLoggerTest3'
        log_source1 = 'unittest1_1'
        log_source2 = 'unittest1_2'
        log_source3 = 'unittest1_3'

        timestamp1_1 = datetime.strptime('1979-06-20T06:06:06.20', '%Y-%m-%dT%H:%M:%S.%f')
        timestamp1_2 = datetime.strptime('1979-06-20T06:06:06.21', '%Y-%m-%dT%H:%M:%S.%f')
        timestamp1_3 = datetime.strptime('1979-06-20T06:06:06.22', '%Y-%m-%dT%H:%M:%S.%f')
        timestamp2_1 = datetime.strptime('1979-06-20T06:06:06.23', '%Y-%m-%dT%H:%M:%S.%f')
        timestamp2_2 = datetime.strptime('1979-06-20T06:06:06.24', '%Y-%m-%dT%H:%M:%S.%f')
        timestamp2_3 = datetime.strptime('1979-06-20T06:06:06.25', '%Y-%m-%dT%H:%M:%S.%f')
        timestamp3_1 = datetime.strptime('1979-06-20T06:06:06.26', '%Y-%m-%dT%H:%M:%S.%f')
        timestamp3_2 = datetime.strptime('1979-06-20T06:06:06.27', '%Y-%m-%dT%H:%M:%S.%f')
        timestamp3_3 = datetime.strptime('1979-06-20T06:06:06.28', '%Y-%m-%dT%H:%M:%S.%f')
        level = 'warn'
        message = u'In Sweden Strindberg is both known as a novelist and a playwright'

        # When
        logger.log(source_context, log_source1, timestamp1_1, level, message)
        logger.log(source_context, log_source1, timestamp1_2, level, message)
        logger.log(source_context, log_source1, timestamp1_3, level, message)

        logger.log(source_context, log_source2, timestamp2_1, level, message)
        logger.log(source_context, log_source2, timestamp2_2, level, message)
        logger.log(source_context, log_source2, timestamp2_3, level, message)

        logger.log(source_context, log_source3, timestamp3_1, level, message)
        logger.log(source_context, log_source3, timestamp3_2, level, message)
        logger.log(source_context, log_source3, timestamp3_3, level, message)

        # Then...
        free_text = 'Strindberg'

        # 1. Find by exact context, should return 3 in this date span
        # (ie provide source context, log_source and level
        fromdate = datetime.strptime('1979-06-20T06:06:06.19', '%Y-%m-%dT%H:%M:%S.%f')
        todate = datetime.strptime('1979-06-20T06:06:06.23', '%Y-%m-%dT%H:%M:%S.%f')
        result = logger.load_by_date_range(source_context, log_source1, level, fromdate, todate)

        self.assertEqual(len(result), 3)
        self.__assert_log_message(result[0], source_context, log_source1, timestamp1_1, level, message)
        self.__assert_log_message(result[1], source_context, log_source1, timestamp1_2, level, message)
        self.__assert_log_message(result[2], source_context, log_source1, timestamp1_3, level, message)


        # 2. now be less specific, should find all 9
        # (ie provide source context, log_source and level
        fromdate = datetime.strptime('1979-06-20T06:06:06.19', '%Y-%m-%dT%H:%M:%S.%f')
        todate = datetime.strptime('1979-06-20T06:06:06.29', '%Y-%m-%dT%H:%M:%S.%f')
        result = logger.load_by_date_range(source_context, None, level, fromdate, todate)

        self.assertEqual(len(result), 9)

        for lm in result:
            print u'%s' % lm

        # 3. Same as last, but choose a date outside the inserted range, should find 0
        fromdate = datetime.strptime('1939-06-20T06:06:06.19', '%Y-%m-%dT%H:%M:%S.%f')
        todate = datetime.strptime('1939-06-20T06:06:06.29', '%Y-%m-%dT%H:%M:%S.%f')
        result = logger.load_by_date_range(source_context, None, level, fromdate, todate)

        self.assertEqual(len(result), 0)

if __name__ == '__main__':
    unittest.main()
