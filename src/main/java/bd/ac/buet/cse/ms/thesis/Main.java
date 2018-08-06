package bd.ac.buet.cse.ms.thesis;

import com.datastax.driver.core.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Main {

    private static final String SERVER_IP = "127.0.0.1";
    private static final String KEYSPACE = "cuckoo_test";
    private static final String LOOKUP_QUERY = "SELECT * FROM air_traffic WHERE \"UniqueCarrier\" = ?";
    private static final String LOOKUP_QUERY_MANY_KEYS = "SELECT * FROM air_traffic WHERE \"Id\" = ?";
    private static final String DELETE_QUERY = "DELETE FROM air_traffic WHERE \"UniqueCarrier\" = ?";
    private static final String DELETE_QUERY_MANY_KEYS = "DELETE FROM air_traffic WHERE \"Id\" = ?";

    private static final String[] CARRIERS_HAVING_DATA = new String[]{
            "AQ", "HA", "AS", "F9", "B6", "9E", "CO", "NW", "OH", "FL", "YV", "EV"
    };

    private static final String[] CARRIERS_NOT_HAVING_DATA = new String[]{
            "BB", "CC", "DD", "EE", "FF", "GG", "AB", "AC", "AD", "AE", "AF", "AG"
    };

    private static final int[] IDS_HAVING_DATA = new int[]{
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
    };

    private static final int[] IDS_NOT_HAVING_DATA = new int[]{
            -1, -2, -3, -4, -5, -6, -7, -8, -9, -10
    };

    private static final Integer[] FRACTIONS = new Integer[]{0, 3, 6, 9, 12};

    private static Map<Integer, Double> fractionDurationMap = new LinkedHashMap<Integer, Double>(FRACTIONS.length);
    private static List<Double> durations = new ArrayList<Double>(CARRIERS_HAVING_DATA.length);

    private static final int TEST_LOOKUP_PERFORMANCE_POSITIVE_QUERY_FRACTION_WISE = 1;
    private static final int TEST_LOOKUP_PERFORMANCE_POSITIVE_QUERY_FRACTION_WISE_MANY_KEYS = 2;
    private static final int TEST_LOOKUP_PERFORMANCE_FILTER_LOAD_WISE = 3;
    private static final int TEST_LOOKUP_AFTER_DELETE = 4;

    private static final int CURRENT_TEST = TEST_LOOKUP_PERFORMANCE_FILTER_LOAD_WISE;

    public static void main(String[] args) {
        try {
            Cluster cluster = Cluster.builder()
                    .addContactPoints(SERVER_IP)
                    .build();

            Session session = cluster.connect(KEYSPACE);
            PreparedStatement lookupPreparedStatement = session.prepare(CURRENT_TEST == TEST_LOOKUP_PERFORMANCE_POSITIVE_QUERY_FRACTION_WISE_MANY_KEYS ? LOOKUP_QUERY_MANY_KEYS : LOOKUP_QUERY).enableTracing();
            PreparedStatement lookupManyKeysPreparedStatement = session.prepare(CURRENT_TEST == TEST_LOOKUP_PERFORMANCE_POSITIVE_QUERY_FRACTION_WISE_MANY_KEYS ? LOOKUP_QUERY_MANY_KEYS : LOOKUP_QUERY).enableTracing();
            PreparedStatement deletePreparedStatement = session.prepare(CURRENT_TEST == TEST_LOOKUP_PERFORMANCE_POSITIVE_QUERY_FRACTION_WISE_MANY_KEYS ? DELETE_QUERY_MANY_KEYS : DELETE_QUERY).enableTracing();

            switch (CURRENT_TEST) {
                case TEST_LOOKUP_PERFORMANCE_POSITIVE_QUERY_FRACTION_WISE:
                    runLookupPerformanceTestPositiveQueryWise(session, lookupPreparedStatement);
                    break;
                case TEST_LOOKUP_PERFORMANCE_POSITIVE_QUERY_FRACTION_WISE_MANY_KEYS:
                    runLookupPerformanceTestPositiveQueryWiseManyKeys(session, lookupManyKeysPreparedStatement);
                    break;
                case TEST_LOOKUP_AFTER_DELETE:
                    runLookupAfterDeleteTest(session, lookupPreparedStatement, deletePreparedStatement);
                    break;
                case TEST_LOOKUP_PERFORMANCE_FILTER_LOAD_WISE:
                    runLookupPerformanceTestFilterLoadWise(session, lookupPreparedStatement);
                    break;
                default:
                    throw new RuntimeException("Unknown value for CURRENT_TEST: " + CURRENT_TEST);
            }

            session.close();
            cluster.close();
        } finally {
            SoundUtils.tone(100, 250);
        }
    }

    private static void runLookupPerformanceTestPositiveQueryWise(Session session, PreparedStatement preparedStatement) {
        for (Integer fraction : FRACTIONS) {
            long start = System.currentTimeMillis();

            for (int j = 0; j < fraction; j++) {
                BoundStatement statement = preparedStatement.bind(CARRIERS_HAVING_DATA[j]);
                executeQuery(fraction, "HAS_DATA", CARRIERS_HAVING_DATA[j], session, statement);
            }

            for (int j = fraction; j < CARRIERS_NOT_HAVING_DATA.length; j++) {
                BoundStatement statement = preparedStatement.bind(CARRIERS_NOT_HAVING_DATA[j]);
                executeQuery(fraction, "_NO_DATA", CARRIERS_NOT_HAVING_DATA[j], session, statement);
            }

            long end = System.currentTimeMillis();

            double duration = (end - start) / 1000.0;

            fractionDurationMap.put(fraction, duration);
        }

        for (Map.Entry<Integer, Double> entry : fractionDurationMap.entrySet()) {
            System.out.println(entry.getValue());
        }
    }

    private static void runLookupPerformanceTestPositiveQueryWiseManyKeys(Session session, PreparedStatement preparedStatement) {
        for (Integer fraction : FRACTIONS) {
            long start = System.currentTimeMillis();

            for (int j = 0; j < fraction; j++) {
                BoundStatement statement = preparedStatement.bind(IDS_HAVING_DATA[j]);
                executeQuery(fraction, "HAS_DATA", Integer.toString(IDS_HAVING_DATA[j]), session, statement);
            }

            for (int j = fraction; j < IDS_NOT_HAVING_DATA.length; j++) {
                BoundStatement statement = preparedStatement.bind(IDS_NOT_HAVING_DATA[j]);
                executeQuery(fraction, "_NO_DATA", Integer.toString(IDS_NOT_HAVING_DATA[j]), session, statement);
            }

            long end = System.currentTimeMillis();

            double duration = (end - start) / 1000.0;

            fractionDurationMap.put(fraction, duration);
        }

        for (Map.Entry<Integer, Double> entry : fractionDurationMap.entrySet()) {
            System.out.println(entry.getValue());
        }
    }

    private static void runLookupPerformanceTestFilterLoadWise(Session session, PreparedStatement preparedStatement) {
        for (String key : CARRIERS_HAVING_DATA) {
            BoundStatement boundStatement = preparedStatement.bind(key);

            long start = System.currentTimeMillis();

            executeQuery(-1, null, key, session, boundStatement);

            long end = System.currentTimeMillis();

            double duration = (end - start) / 1000.0;

            durations.add(duration);
        }

        for (Double duration : durations.toArray(new Double[]{})) {
            System.out.println(duration);
        }
    }

    private static void runLookupAfterDeleteTest(Session session, PreparedStatement lookupPreparedStatement,
                                                 PreparedStatement deletePreparedStatement) {
        for (String key : CARRIERS_HAVING_DATA) {
            BoundStatement deleteBoundStatement = deletePreparedStatement.bind(key);
            BoundStatement lookupBoundStatement = lookupPreparedStatement.bind(key);

            executeQuery(-1, null, key, session, deleteBoundStatement);
            executeQuery(-1, null, key, session, lookupBoundStatement);


            long start = System.currentTimeMillis();

            executeQuery(-1, null, key, session, lookupBoundStatement);

            long end = System.currentTimeMillis();

            double duration = (end - start) / 1000.0;

            durations.add(duration);
        }

        for (Double duration : durations.toArray(new Double[]{})) {
            System.out.println(duration);
        }
    }

    private static int executeQuery(int fraction, String segment, String key, Session session, BoundStatement statement) {
        ResultSet resultSet = session.execute(statement);
        int rows = 0;
        while (resultSet.iterator().hasNext()) {
            resultSet.iterator().next();
            rows++;
        }

//        System.out.println("Fraction: " + fraction + ", Segment: " + segment + ", Key: " + key + ", Rows: " + rows);

        return rows;
    }

    private static int getQueryExecutionDuration(com.datastax.driver.core.QueryTrace queryTrace) {
        for (com.datastax.driver.core.QueryTrace.Event event : queryTrace.getEvents()) {
            if (event.getDescription().startsWith(QueryTraceEvent.EVENT_READ)) {
                return event.getSourceElapsedMicros();
            }
        }

        throw new RuntimeException("Event " + QueryTraceEvent.EVENT_READ + " not found in query traces!");
    }

    private static void printDurations(List<Integer> durations) {
        System.out.println("----------------");
        for (int duration : durations) {
            System.out.println(duration);
        }
        System.out.println("----------------");
        System.out.println("Avg. = " + getAverage(durations));
    }

    private static long getAverage(List<Integer> durations) {
        long totalDuration = 0;
        for (int duration : durations) {
            totalDuration += duration;
        }

        return totalDuration / durations.size();
    }
}
