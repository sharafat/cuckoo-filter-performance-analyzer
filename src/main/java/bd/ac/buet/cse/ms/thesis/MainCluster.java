package bd.ac.buet.cse.ms.thesis;

import com.datastax.driver.core.*;
import com.datastax.driver.core.QueryTrace;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MainCluster extends Main {

    private static final int TEST_RUNS = 100;

    // all keys requested from other node, all keys result in positive (unused)
    private static final int TEST_LOOKUP_PERFORMANCE_IN_OTHER_NODE_ALL_POSITIVE = 1;
    // all keys requested from other node, all keys result in negative (unused)
    private static final int TEST_LOOKUP_PERFORMANCE_IN_OTHER_NODE_ALL_NEGATIVE = 2;
    // all keys requested from other node, some keys result in positive, other keys result in negative
    private static final int TEST_LOOKUP_PERFORMANCE_IN_OTHER_NODE_FRACTION_OF_NEG_QUERIES = 3;
    // some keys requested from connected node, others from other node, all keys result in positive
    private static final int TEST_LOOKUP_PERFORMANCE_FOR_FRACTION_OF_QUERIES_IN_OTHER_NODES_ALL_POSITIVE = 4;
    // some keys requested from connected node, others from other node, all keys result in negative
    private static final int TEST_LOOKUP_PERFORMANCE_FOR_FRACTION_OF_QUERIES_IN_OTHER_NODES_ALL_NEGATIVE = 5;
    // some keys requested from connected node, others from other node, all keys result in deleted
    private static final int TEST_LOOKUP_PERFORMANCE_FOR_FRACTION_OF_QUERIES_IN_OTHER_NODES_ALL_DELETED = 6;

    private static final int TEST_ALL = -1;
    private static final int[] TEST_SUIT = new int[]{
            TEST_LOOKUP_PERFORMANCE_IN_OTHER_NODE_ALL_POSITIVE,
            TEST_LOOKUP_PERFORMANCE_IN_OTHER_NODE_ALL_NEGATIVE,
            TEST_LOOKUP_PERFORMANCE_IN_OTHER_NODE_FRACTION_OF_NEG_QUERIES,
            TEST_LOOKUP_PERFORMANCE_FOR_FRACTION_OF_QUERIES_IN_OTHER_NODES_ALL_POSITIVE,
            TEST_LOOKUP_PERFORMANCE_FOR_FRACTION_OF_QUERIES_IN_OTHER_NODES_ALL_NEGATIVE,
            TEST_LOOKUP_PERFORMANCE_FOR_FRACTION_OF_QUERIES_IN_OTHER_NODES_ALL_DELETED,
            TEST_LOOKUP_PERFORMANCE_IN_OTHER_NODE_FRACTION_OF_NEG_QUERIES,
            TEST_LOOKUP_PERFORMANCE_FOR_FRACTION_OF_QUERIES_IN_OTHER_NODES_ALL_POSITIVE,
            TEST_LOOKUP_PERFORMANCE_FOR_FRACTION_OF_QUERIES_IN_OTHER_NODES_ALL_NEGATIVE,
            TEST_LOOKUP_PERFORMANCE_FOR_FRACTION_OF_QUERIES_IN_OTHER_NODES_ALL_DELETED
    };

    private static final String LOOKUP_QUERY = "SELECT * FROM air_traffic WHERE \"FlightNum\" = '%s'";
    private static final String LOOKUP_QUERY_IN_CLAUSE = "SELECT * FROM air_traffic WHERE \"FlightNum\" IN ('%s')";

    private static final int[] TWO_NODES_CLUSTER_NODE_1_KEYS = new int[]{904, 761, 5472, 3724, 3715, 235, 2093, 3768, 2625, 835, 600, 1086};
    private static final int[] TWO_NODES_CLUSTER_NODE_2_KEYS = new int[]{2378, 1179, 3481, 1474, 1197, 1016, 2907, 1403, 980, 1416, 931, 2628};
    private static final int[] TWO_NODES_CLUSTER_NODE_1_KEYS_NON_EXISTENT = new int[]{-1, -2, -4, -5, -7, -10, -11, -12, -15, -16, -17, -21};
    private static final int[] TWO_NODES_CLUSTER_NODE_2_KEYS_NON_EXISTENT = new int[]{-3, -6, -8, -9, -13, -14, -18, -19, -20, -22, -26, -28};
    private static final int[] TWO_NODES_CLUSTER_NODE_1_KEYS_DELETED = new int[]{1294, 2036, 2878, 2713, 1740, 1553, 2271, 2914, 1773, 2380, 1627, 2829};
    private static final int[] TWO_NODES_CLUSTER_NODE_2_KEYS_DELETED = new int[]{630, 155, 2715, 5441, 3398, 732, 922, 850, 963, 2621, 2784, 3763};

    private static final int CURRENT_TEST = TEST_ALL;

    public static void main(String[] args) {
        try {
            Cluster cluster = Cluster.builder()
                    .addContactPoint(SERVER_IP)
                    .withLoadBalancingPolicy(new WhiteListPolicy(DCAwareRoundRobinPolicy.builder().build(),
                            new ArrayList<InetSocketAddress>() {{
                                add(new InetSocketAddress(SERVER_IP, 9042));
                            }}))
                    .build()
                    ;

            Session session = cluster.connect(KEYSPACE);

            switch (CURRENT_TEST) {
                case TEST_ALL:
                    runAllTests(session);
                    break;
                default:
                    runTest(CURRENT_TEST, session);
            }

            session.close();
            cluster.close();
        } finally {
//            SoundUtils.tone(100, 250);
        }
    }

    private static void runAllTests(Session session) {
        for (int test : TEST_SUIT) {
            System.out.println("\nTEST " + test + ":\n");
            runTest(test, session);
        }
    }

    private static void runTest(int test, Session session) {
        switch (test) {
            case TEST_ALL:
                runAllTests(session);
                break;
            case TEST_LOOKUP_PERFORMANCE_IN_OTHER_NODE_ALL_POSITIVE:
                runLookupPerformanceTestInOtherNodeAllPositive(session);
                break;
            case TEST_LOOKUP_PERFORMANCE_IN_OTHER_NODE_ALL_NEGATIVE:
                runLookupPerformanceTestInOtherNodeAllNegative(session);
                break;
            case TEST_LOOKUP_PERFORMANCE_IN_OTHER_NODE_FRACTION_OF_NEG_QUERIES:
                runLookupPerformanceTestFractionOfNegQueriesAllInOtherNodes(session);
                break;
            case TEST_LOOKUP_PERFORMANCE_FOR_FRACTION_OF_QUERIES_IN_OTHER_NODES_ALL_POSITIVE:
                runLookupPerformanceTestFractionOfOtherNodesWise_AllPositive(session);
                break;
            case TEST_LOOKUP_PERFORMANCE_FOR_FRACTION_OF_QUERIES_IN_OTHER_NODES_ALL_NEGATIVE:
                runLookupPerformanceTestFractionOfOtherNodesWise_AllNegative(session);
                break;
            case TEST_LOOKUP_PERFORMANCE_FOR_FRACTION_OF_QUERIES_IN_OTHER_NODES_ALL_DELETED:
                runLookupPerformanceTestFractionOfOtherNodesWise_AllDeleted(session);
                break;
            default:
                throw new RuntimeException("Unknown value for test: " + test);
        }
    }

    private static void runLookupPerformanceTestInOtherNodeAllPositive(Session session) {
        runLookupPerformanceTestInOtherNode(session, TWO_NODES_CLUSTER_NODE_2_KEYS, "OTHER_NODE_ALL_POSITIVE");
    }

    private static void runLookupPerformanceTestInOtherNodeAllNegative(Session session) {
        runLookupPerformanceTestInOtherNode(session, TWO_NODES_CLUSTER_NODE_2_KEYS_NON_EXISTENT, "OTHER_NODE_ALL_NEGATIVE");
    }

    private static void runLookupPerformanceTestInOtherNode(Session session, int[] nodeKeys, String segment) {
        for (int run = 0; run < TEST_RUNS / 10; run++) {
            for (int i = 0; i < 10; i++) {
                String id = Integer.toString(nodeKeys[i]);
                SimpleStatement statement = new SimpleStatement(String.format(LOOKUP_QUERY, id));

                long start = System.currentTimeMillis();

                executeQuery(i, segment, id, session, statement);

                long end = System.currentTimeMillis();

                double duration = (end - start);

                durations.add(duration);
            }
        }

        double total = 0;
        for (Double duration : durations) {
            total += duration;
        }
        System.out.println(total / durations.size());
    }

    private static void runLookupPerformanceTestFractionOfOtherNodesWise_AllPositive(Session session) {
        runLookupPerformanceTestFractionOfOtherNodesWise(session, TWO_NODES_CLUSTER_NODE_1_KEYS,
                TWO_NODES_CLUSTER_NODE_2_KEYS, "SAME_AND_OTHER_NODES_ALL_POSITIVE");
    }

    private static void runLookupPerformanceTestFractionOfOtherNodesWise_AllNegative(Session session) {
        runLookupPerformanceTestFractionOfOtherNodesWise(session, TWO_NODES_CLUSTER_NODE_1_KEYS_NON_EXISTENT,
                TWO_NODES_CLUSTER_NODE_2_KEYS_NON_EXISTENT, "SAME_AND_OTHER_NODES_ALL_NEGATIVE");
    }

    private static void runLookupPerformanceTestFractionOfOtherNodesWise_AllDeleted(Session session) {
        runLookupPerformanceTestFractionOfOtherNodesWise(session, TWO_NODES_CLUSTER_NODE_1_KEYS_DELETED,
                TWO_NODES_CLUSTER_NODE_2_KEYS_DELETED, "SAME_AND_OTHER_NODES_ALL_DELETED");
    }

    private static void runLookupPerformanceTestFractionOfNegQueriesAllInOtherNodes(Session session) {
        runLookupPerformanceTestFractionOfOtherNodesWise(session, TWO_NODES_CLUSTER_NODE_2_KEYS,
                TWO_NODES_CLUSTER_NODE_2_KEYS_NON_EXISTENT, "POS_NEG_ALL_OTHER_NODE");
    }

    private static void runLookupPerformanceTestFractionOfOtherNodesWise(Session session, int[] node1Keys, int[] node2Keys, String segment) {
        Map<Integer, List<Double>> fractionDurationMap = new LinkedHashMap<Integer, List<Double>>();

        for (int run = 0; run < TEST_RUNS; run++) {
            for (Integer fraction : FRACTIONS) {
                List<String> ids = new ArrayList<String>();

                for (int j = 0; j < fraction; j++) {
                    ids.add(Integer.toString(node2Keys[j]));
                }

                for (int j = fraction; j < node1Keys.length; j++) {
                    ids.add(Integer.toString(node1Keys[j]));
                }

                String idsString = ids.toString().substring(1, ids.toString().length() - 1).replace(", ", "', '");
                String query = String.format(LOOKUP_QUERY_IN_CLAUSE, idsString);
                Statement statement = new SimpleStatement(query);

                long start = System.currentTimeMillis();

                executeQuery(fraction, segment, idsString, session, statement);

                long end = System.currentTimeMillis();

                double duration = (end - start);

                List<Double> durations = fractionDurationMap.get(fraction);
                if (durations == null) {
                    durations = new ArrayList<Double>(TEST_RUNS);
                }

                durations.add(duration);

                fractionDurationMap.put(fraction, durations);

//                System.out.println(query);
            }
        }

        for (Map.Entry<Integer, List<Double>> entry : fractionDurationMap.entrySet()) {
            Double total = 0.0;
            for (Double d : entry.getValue()) {
                total += d;
            }
            System.out.println(total / TEST_RUNS);
        }
    }

    private static int executeQuery(int fraction, String segment, String key, Session session, Statement statement) {
        long start = System.currentTimeMillis();

        ResultSet resultSet = session.execute(statement);
        int rows = 0;
        while (resultSet.iterator().hasNext()) {
            Row row = resultSet.iterator().next();
            rows++;
        }

        long end = System.currentTimeMillis();

//        System.out.println("Fraction: " + fraction + ", Segment: " + segment + ", Key: " + key + ", Rows: " + rows
//                + ", Host: " + resultSet.getExecutionInfo().getQueriedHost().getAddress().getHostAddress()
//                + ", duration: " + (end - start) + " ms");

//        printQueryTraceEvents(resultSet);


        return rows;
    }

    private static void printQueryTraceEvents(ResultSet resultSet) {
        List<QueryTrace.Event> events = resultSet.getExecutionInfo().getQueryTrace().getEvents();
        String eventStr = "";
        for (QueryTrace.Event event : events) {
            eventStr += event + "\n";
        }
        System.out.println("\n" + eventStr + "\n");
    }
}
