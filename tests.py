
from datetime import datetime, timedelta
from pycats import TimeSeriesCassandraDao, FloatMetricDTO
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
        self.cassandra_hosts = ['cass_host1.myhost.com','cass_host2.myhost.com']
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
            self.assertEqual(result[i][0], values_inserted[i].timestamp)
            self.assertEqual(result[i][1], values_inserted[i+1].value)

if __name__ == '__main__':
    unittest.main()
