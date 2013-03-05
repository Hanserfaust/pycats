
from datetime import datetime, timedelta
from pycats import TimeSeriesCassandraDao, TimestampedDataDTO, StringIndexer, StringIndexDTO
import unittest

# Tips to get started with pycats:
#
#
# 1) Use a Datastax prepared AMI on Amazon EC2 to get one or several cassandra hosts up and running
#     Follow the guide here for example (be ware of new versions of the doc though):
#     http://www.datastax.com/docs/1.2/install/install_ami
# 2) Make sure you have access to the cassandra Thrift ports on the instance
# 3) SSH to the cassandra host and execute the CQL needed to prepare the Columnfamily, ie:
#
#    ubuntu@ip-10-10-10-2:~$ cqlsh
#    Connected to CassandraTest at localhost:9160.
#    [cqlsh 2.2.0 | Cassandra 1.1.5 | CQL spec 2.0.0 | Thrift protocol 19.32.0]
#    Use HELP for help.
#    cqlsh> CREATE KEYSPACE pycats_test_space WITH strategy_class = 'SimpleStrategy' AND strategy_options:replication_factor = '1';
#    cqlsh> use pycats_test_space;
#    cqlsh:pycats_test_space> CREATE COLUMNFAMILY HourlyTimestampedData (KEY ascii PRIMARY KEY) WITH comparator=timestamp AND default_validation=blob;
#    cqlsh:pycats_test_space>
#
# 4) Enter the URLs to your cassandra instances in the list 'cassandra_hosts' in the setUp() method below
# 5) Run the tests, The tests will insert data and then load it in various scenarios.
# 6) You can drop the key space pycats_test_space afterwards.
#    cqlsh> DROP KEYSPACE pycats_test_space;
#
#
class TimeSeriesCassandraDaoIntegrationTest(unittest.TestCase):

    # CQL to create the MetricHourlyFloat column family
    #
    # CREATE COLUMNFAMILY HourlyFloat (KEY ascii PRIMARY KEY) WITH comparator=timestamp AND default_validation=float;
    #
    # Ref: http://cassandra.apache.org/doc/cql/CQL.html#CREATECOLUMNFAMILY
    #
    def setUp(self):
        # Test constants
        self.cassandra_hosts = ['ec2-176-34-195-104.eu-west-1.compute.amazonaws.com']
        self.key_space = 'pycats_test_space'
        # Use a Django cache to test the cache mechanism
        self.cache = None

        self.dao = TimeSeriesCassandraDao(self.cassandra_hosts, self.key_space, cache=self.cache)
        self.insert_the_test_range_into_live_db = True

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
                dto = TimestampedDataDTO(source_id, curr_datetime, value_name, u'%s' % value)
                if not batch_insert:
                    self.dao.insert_timestamped_data(dto)

                dtos.append(dto)
            curr_datetime = curr_datetime + timedelta(minutes=10)
            values_inserted += 1
            value += 1

        # And batch insert
        if batch_insert:
            self.dao.batch_insert_timestamped_data(dtos)
        return dtos

    def test_should_load_all_data_for_full_range_using_batch_insert(self):
        source_id = 'unittest'
        test_metric = 'ramp_height'
        start_datetime = datetime.strptime('1979-12-31T22:00:00', '%Y-%m-%dT%H:%M:%S')
        end_datetime = datetime.strptime('1980-01-02T03:00:00', '%Y-%m-%dT%H:%M:%S')

        values_inserted = self.__insert_range_of_metrics(source_id, test_metric, start_datetime, end_datetime, batch_insert=True)

        # All values should be received for this date range
        result = self.dao.get_timetamped_data_range(source_id, test_metric, start_datetime, end_datetime)

        # First assert length
        self.assertEqual(len(result), len(values_inserted))

        for i in range(0, len(values_inserted)):
            self.assertEqual(result[i][0], values_inserted[i].timestamp)
            self.assertEqual(result[i][1], values_inserted[i].data_value)

    def test_should_load_all_data_for_full_range_using_single_insert(self):
        source_id = 'unittest'
        test_metric = 'ramp_height'
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

    def test_should_load_correct_data_for_partial_range_using_batch_insert(self):
        source_id = 'unittest'
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

    def test_should_store_and_load_a_simple_string_and_corresponding_indexes_and_load_by_index(self):
        source_id = 'indexed_test_1'
        data_name = 'evil_text'
        data_value = u'Woe to you o earth and sea. For the devil sends the beast with wrath'
        beastly_timestamp = datetime.strptime('1982-03-01T06:06:06', '%Y-%m-%dT%H:%M:%S')

        dto = TimestampedDataDTO(source_id, beastly_timestamp, data_name, data_value)
        self.dao.insert_timestamped_data(dto, True)

        # All values should be received for this date range
        result = self.dao.get_timetamped_data_range(source_id, data_name, beastly_timestamp- timedelta(minutes=1), beastly_timestamp+timedelta(minutes=1))
        #print result

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], beastly_timestamp)
        self.assertEqual(result[0][1], data_value)

        # Now make a free text search
        search_string = 'sea. For'
        result = self.dao.get_timestamped_data_by_search_string(source_id,data_name, search_string)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], beastly_timestamp)
        self.assertEqual(result[0][1], data_value)
        #print result

    def test_should_store_and_load_a_complex_string_and_corresponding_indexes_and_load_by_index(self):
        source_id = 'indexed_test_2'
        data_name = 'evil_text'
        data_value = u'Tue Mar  5 14:41:33 Hans-Eklunds-MacBook-Pro com.apple.backupd-auto[3780] <Notice>: Not starting scheduled Time Machine backup - time machine destination not resolvable.'
        beastly_timestamp = datetime.strptime('1982-03-01T06:06:06', '%Y-%m-%dT%H:%M:%S')

        dto = TimestampedDataDTO(source_id, beastly_timestamp, data_name, data_value)
        self.dao.insert_timestamped_data(dto, True)

        # Now make a couple of free text search
        search_string = 'Notice'
        result = self.dao.get_timestamped_data_by_search_string(source_id,data_name, search_string)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], beastly_timestamp)
        self.assertEqual(result[0][1], data_value)

        search_string = 'Hans-Eklunds-MacBook-Pro'
        result = self.dao.get_timestamped_data_by_search_string(source_id,data_name, search_string)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], beastly_timestamp)
        self.assertEqual(result[0][1], data_value)

        search_string = 'backupd-auto[3780]'
        result = self.dao.get_timestamped_data_by_search_string(source_id,data_name, search_string)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], beastly_timestamp)
        self.assertEqual(result[0][1], data_value)

    def test_should_store_muliple_similar_complex_strings_with_different_timestamps_saved_out_of_order_should_be_loaded_in_order(self):
        source_id = 'indexed_test_3'
        data_name = 'evil_text'

        data_value1 = u'Hans-Eklunds-MacBook-Pro com.apple.backupd-auto[3780] <Notice>: Not starting scheduled Time Machine backup - time machine destination not resolvable.'
        beastly_timestamp1 = datetime.strptime('1982-03-01T06:06:06', '%Y-%m-%dT%H:%M:%S')
        dto1 = TimestampedDataDTO(source_id, beastly_timestamp1, data_name, data_value1)

        data_value2 = u'Hans-Smiths-MacBook-Pro com.apple.backupd-auto[3780] <Notice>: Not starting scheduled Time Machine backup - time machine destination not resolvable.'
        beastly_timestamp2 = datetime.strptime('1982-03-01T06:06:08', '%Y-%m-%dT%H:%M:%S')
        dto2 = TimestampedDataDTO(source_id, beastly_timestamp2, data_name, data_value2)

        data_value3 = u'Hans-Johnssons-MacBook-Pro com.apple.backupd-auto[3780] <Notice>: Not starting scheduled Time Machine backup - time machine destination not resolvable.'
        beastly_timestamp3 = datetime.strptime('1982-03-01T06:06:07', '%Y-%m-%dT%H:%M:%S')
        dto3 = TimestampedDataDTO(source_id, beastly_timestamp3, data_name, data_value3)

        data_value4 = u'time machine destination not recoverable.'
        beastly_timestamp4 = datetime.strptime('1982-03-01T06:06:07', '%Y-%m-%dT%H:%M:%S')
        dto4 = TimestampedDataDTO(source_id, beastly_timestamp4, data_name, data_value4)

        self.dao.insert_timestamped_data(dto1, True)
        self.dao.insert_timestamped_data(dto2, True)
        self.dao.insert_timestamped_data(dto3, True)
        self.dao.insert_timestamped_data(dto4, True)

        # Three should be found, the last one should not be found by this search
        search_string = 'Notice'
        results = self.dao.get_timestamped_data_by_search_string(source_id,data_name, search_string)
        self.assertEqual(len(results), 3)

        # Assert correct order
        self.assertTrue(results[0][0] < results[1][0])
        self.assertTrue(results[1][0] < results[2][0])

        #for result in results:
            #print result

        # Assure only one hit on this unique name
        search_string = 'Hans-Smiths-MacBook'
        results = self.dao.get_timestamped_data_by_search_string(source_id,data_name, search_string)
        self.assertEqual(len(results), 1)

# The KeySpace must have a column family created as follows:
#
# CREATE COLUMNFAMILY IndexedStrings (KEY ascii PRIMARY KEY) WITH comparator=timestamp AND default_validation=text;
#
####################################################################################
class StringIndexerTest(unittest.TestCase):
    test_strings = ['<1921___.bg three cats!Left__home(early)-In.Two.CARS', 'One man left Home early!!', 'two Woman left homE Late?', 'one Car_turned Left at Our HOME']
    string_indxer = None

    def setUp(self):
        # Test constants
        self.cassandra_hosts = ['ec2-176-34-195-104.eu-west-1.compute.amazonaws.com']
        self.key_space = 'pycats_test_space'
        # Use a Django cache to test the cache mechanism
        self.cache = None

        self.string_indexer = StringIndexer()
        self.insert_the_test_range_into_live_db = True

    def test_should_split_simple_string_into_one_indexable_word(self):

        # Given
        test_string = 'sea.'

        # When
        result = self.string_indexer.make_indexable_string(test_string)

        # Then
        expected_result = 'sea'
        self.assertEqual(expected_result, result)

    def test_should_split_complex_string_into_indexable_words(self):

        # Given
        test_string = self.test_strings[0]

        # When
        result = self.string_indexer.make_indexable_string(test_string)

        # Then
        expected_result = '1921 bg three cats left home early in two cars'
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
        expected_result = set(['hello', 'indexed', 'words', 'hello_indexed', 'indexed_words'])
        self.assertEqual(expected_result, result)

    def test_should_return_indexable_substrings_for_3_word_string_given_depth_5(self):

        # Given
        test_string = 'hello indexed words'

        # When
        result = self.string_indexer._build_substrings(test_string, 5)

        # Then
        expected_result = set(['hello', 'indexed', 'words', 'hello_indexed', 'indexed_words', 'hello_indexed_words'])
        self.assertEqual(expected_result, result)

    def test_should_return_indexable_substrings_for_5_word_string_given_depth_3(self):

        # Given
        test_string = 'hello indexed words of yore'

        # When
        result = self.string_indexer._build_substrings(test_string, 3)

        # Then
        expected_result = set(['hello', 'indexed', 'words', 'of', 'yore', 'hello_indexed', 'indexed_words', 'words_of', 'of_yore', 'hello_indexed_words', 'indexed_words_of', 'words_of_yore'])
        self.assertEqual(expected_result, result)

    def test_should_only_print_nr_of_words_in_result_to_show_how_many_indexes_are_needed_for_a_large_string_given_a_depth(self):
        # Given
        test_string = "Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum."
        cleaned_test_string = self.string_indexer.make_indexable_string(test_string)

        # When
        depth = 2
        result = self.string_indexer._build_substrings(cleaned_test_string, depth)

        # Then
        print u'A %s word scentence yielded %s indexes given depth %s' % (len(cleaned_test_string.split()), len(result), depth)

    def test_should_return_build_actual_indexes_from_string_dto(self):

        # Given
        test_string = 'hello indexed words of yore'
        dto = TimestampedDataDTO('the_kids', datetime.utcnow(), 'log_text', test_string)

        # When
        result = self.string_indexer.build_indexes_from_timstamped_dto(dto)

        # Then
        for index_dto in result:
            print u'Index: %s' % index_dto

    def test_should_store_string_and_load_by_key(self):
        pass

    def test_should_store_a_few_similar_string_and_their_indexes_and_load_all_by_index(self):
        pass

if __name__ == '__main__':
    unittest.main()
