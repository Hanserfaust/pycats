
from datetime import datetime, timedelta
from pycats import TimeSeriesCassandraDao, FloatMetricDTO, IndexedStringsCassandraDAO, TimeStampedStringDTO
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
#    cqlsh:pycats_test_space> CREATE COLUMNFAMILY HourlyFloat (KEY ascii PRIMARY KEY) WITH comparator=timestamp AND default_validation=float;
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
                dto = FloatMetricDTO(source_id, curr_datetime, value_name, float(value))
                if not batch_insert:
                    self.dao.insert_hourly_float_metric(dto)

                dtos.append(dto)
            curr_datetime = curr_datetime + timedelta(minutes=10)
            values_inserted += 1
            value += 1

        # And batch insert
        if batch_insert:
            self.dao.batch_insert_hourly_float_metric(dtos)
        return dtos

    def test_should_load_all_data_for_full_range_using_batch_insert(self):
        source_id = 'batch_insert'
        test_metric = 'ramp_height'
        start_datetime = datetime.strptime('1979-12-31T22:00:00', '%Y-%m-%dT%H:%M:%S')
        end_datetime = datetime.strptime('1980-01-02T03:00:00', '%Y-%m-%dT%H:%M:%S')

        values_inserted = self.__insert_range_of_metrics(source_id, test_metric, start_datetime, end_datetime, batch_insert=True)

        # All values should be received for this date range
        result = self.dao.get_hourly_float_data_range(source_id, test_metric, start_datetime, end_datetime)

        # First assert length
        self.assertEqual(len(result), len(values_inserted))

        for i in range(0, len(values_inserted)):
            self.assertEqual(result[i][0], values_inserted[i].timestamp)
            self.assertEqual(result[i][1], values_inserted[i].value)

    def test_should_load_all_data_for_full_range_using_single_insert(self):
        source_id = 'single_insert'
        test_metric = 'ramp_height'
        start_datetime = datetime.strptime('1979-12-31T22:00:00', '%Y-%m-%dT%H:%M:%S')
        end_datetime = datetime.strptime('1980-01-02T03:00:00', '%Y-%m-%dT%H:%M:%S')

        values_inserted = self.__insert_range_of_metrics(source_id, test_metric, start_datetime, end_datetime, batch_insert=False)

        # Should miss the first and last values
        result = self.dao.get_hourly_float_data_range(source_id, test_metric, start_datetime, end_datetime)

        # First assert length
        self.assertEqual(len(result), len(values_inserted))

        for i in range(1, len(values_inserted)):
            self.assertEqual(result[i][0], values_inserted[i].timestamp)
            self.assertEqual(result[i][1], values_inserted[i].value)

    def test_should_load_correct_data_for_partial_range_using_batch_insert(self):
        source_id = 'batch_insert'
        test_metric = 'ramp_height'
        start_datetime = datetime.strptime('1979-12-31T22:00:00', '%Y-%m-%dT%H:%M:%S')
        end_datetime = datetime.strptime('1980-01-02T03:00:00', '%Y-%m-%dT%H:%M:%S')

        values_inserted = self.__insert_range_of_metrics(source_id, test_metric, start_datetime, end_datetime, batch_insert=True)

        # Should miss the first and last values that we just inserted above
        result = self.dao.get_hourly_float_data_range(source_id, test_metric, start_datetime+ timedelta(minutes=1), end_datetime-timedelta(minutes=1))

        # First assert length
        self.assertEqual(len(result), len(values_inserted)-2)

        # iterate over result, correct value should be found with offset=1
        for i in range(0, len(result)):
            self.assertEqual(result[i][0], values_inserted[i+1].timestamp)
            self.assertEqual(result[i][1], values_inserted[i+1].value)


# The KeySpace must have a column family created as follows:
#
# CREATE COLUMNFAMILY IndexedStrings (KEY ascii PRIMARY KEY) WITH comparator=timestamp AND default_validation=text;
#
####################################################################################
class IndexedStringsCassandraDaoIntegrationTest(unittest.TestCase):
    test_strings = ['<1921___.bg three cats!Left__home(early)-In.Two.CARS', 'One man left Home early!!', 'two Woman left homE Late?', 'one Car_turned Left at Our HOME']

    def setUp(self):
        # Test constants
        self.cassandra_hosts = ['ec2-176-34-195-104.eu-west-1.compute.amazonaws.com']
        self.key_space = 'pycats_test_space'
        # Use a Django cache to test the cache mechanism
        self.cache = None

        self.dao = IndexedStringsCassandraDAO(self.cassandra_hosts, self.key_space, cache=self.cache)
        self.insert_the_test_range_into_live_db = True

    def test_should_split_complex_string_into_indexable_words(self):

        # Given
        test_string = self.test_strings[0]

        # When
        result = self.dao.make_indexable_string(test_string)

        # Then
        expected_result = '1921 bg three cats left home early in two cars'
        self.assertEqual(expected_result, result)


    def test_should_return_indexable_substrings_given_depth_1(self):

        # Given
        test_string = 'hello indexed words'

        # When
        result = self.dao.build_substrings(test_string, 1)

        # Then
        expected_result = ['hello', 'indexed', 'words']
        self.assertEqual(expected_result, result)

    def test_should_return_indexable_substrings_given_depth_2(self):

        # Given
        test_string = 'hello indexed words'

        # When
        result = self.dao.build_substrings(test_string, 2)

        # Then
        expected_result = ['hello', 'indexed', 'words', 'hello_indexed', 'indexed_words']
        self.assertEqual(expected_result, result)

    def test_should_return_indexable_substrings_for_3_word_string_given_depth_5(self):

        # Given
        test_string = 'hello indexed words'

        # When
        result = self.dao.build_substrings(test_string, 5)

        # Then
        expected_result = ['hello', 'indexed', 'words', 'hello_indexed', 'indexed_words', 'hello_indexed_words']
        self.assertEqual(expected_result, result)

    def test_should_return_indexable_substrings_for_5_word_string_given_depth_3(self):

        # Given
        test_string = 'hello indexed words of yore'

        # When
        result = self.dao.build_substrings(test_string, 3)

        # Then
        expected_result = ['hello', 'indexed', 'words', 'of', 'yore', 'hello_indexed', 'indexed_words', 'words_of', 'of_yore', 'hello_indexed_words', 'indexed_words_of', 'words_of_yore']
        self.assertEqual(expected_result, result)

    def test_should_only_print_nr_of_words_in_result_to_show_how_many_indexes_are_needed_for_a_large_string_given_a_depth(self):
        # Given
        test_string = "Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum."
        cleaned_test_string = self.dao.make_indexable_string(test_string)

        # When
        depth = 2
        result = self.dao.build_substrings(cleaned_test_string, depth)

        # Then
        print u'A %s word scentence yielded %s indexes given depth %s' % (len(cleaned_test_string.split()), len(result), depth)

    def test_should_return_build_actual_index_key_from_string_dto(self):

        # Given
        test_string = 'hello indexed words of yore'
        dto = TimeStampedStringDTO('test.hasse-gw-0', datetime.utcnow(), test_string)
        fake_row_key_for_string_dto = '29gh92hg20jb'

        # When
        result = self.dao.build_indexes_from_string_dto(dto, fake_row_key_for_string_dto)

        # Then
        for index_dto in result:
            print u'Index: %s' % index_dto

    def test_should_store_string_and_load_by_key(self):
        pass

    def test_should_store_a_few_similar_string_and_their_indexes_and_load_all_by_index(self):
        pass

if __name__ == '__main__':
    unittest.main()
