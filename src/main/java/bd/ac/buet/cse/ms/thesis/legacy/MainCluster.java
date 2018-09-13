package bd.ac.buet.cse.ms.thesis.legacy;

import bd.ac.buet.cse.ms.thesis.legacy.utils.SoundUtils;
import com.datastax.driver.core.*;
import com.datastax.driver.core.QueryTrace;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.*;

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
    // some keys requested from connected node, others from other node, all keys result in positive, huge amount of data1 retrieval
    private static final int TEST_LOOKUP_PERFORMANCE_FOR_FRACTION_OF_QUERIES_IN_OTHER_NODES_ALL_POSITIVE_LARGE_RESULT = 7;

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
    private static final String DELETE_QUERY = "DELETE FROM air_traffic WHERE \"FlightNum\" = '%s'";

    private static int[] KEYS = new int[]{
            2378, 3481, 1474, 761, 3715, 1016, 2907, 1403, 2093, 931, 630, 155,
            1179, 1197, 904, 5472, 3724, 235, 980, 1416, 3768, 2628, 2625, 600,
            2715, 835, 1086, 1294, 3398, 732, 922, 850, 1553, 2271, 2784, 5441,
            3763, 2036, 2878, 2713, 1740, 963, 2621, 2914, 238, 2028, 3157, 1627,
            87, 1321, 2721, 1446, 1053, 1521, 3855, 1727, 161, 78, 461, 2816, 2735,
            99, 439, 1201, 3860, 1269, 5459, 3746, 1668, 464, 3138, 1418, 1759, 1841
    };

    private static int[] TWO_NODES_CLUSTER_NODE_1_KEYS = new int[]{3481, 1474, 1016, 2907, 2093, 155, 3724, 235, 3768, 600, 835, 1294};
    private static int[] TWO_NODES_CLUSTER_NODE_2_KEYS = new int[]{2378, 761, 3715, 1403, 931, 630, 1179, 1197, 904, 5472, 980, 1416};
    private static int[] TWO_NODES_CLUSTER_NODE_1_KEYS_NON_EXISTENT = new int[]{-3, -4, -7, -8, -10, -15, -19, -23, -28, -29, -30, -31};
    private static int[] TWO_NODES_CLUSTER_NODE_2_KEYS_NON_EXISTENT = new int[]{-1, -5, -6, -9, -11, -12, -13, -16, -17, -18, -20, -25};
    private static int[] TWO_NODES_CLUSTER_NODE_1_KEYS_DELETED = new int[]{3398, 732, 1553, 2271, 5441, 2878, 963, 2621, 2914, 1627, 1321, 2721};
    private static int[] TWO_NODES_CLUSTER_NODE_2_KEYS_DELETED = new int[]{2628, 2625, 2715, 1086, 922, 850, 2784, 3763, 2036, 2713, 1740, 238};

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

            assignKeysToNodes(session);

            switch (CURRENT_TEST) {
                case TEST_ALL:
                    runAllTests(session);
                    break;
                case TEST_LOOKUP_PERFORMANCE_FOR_FRACTION_OF_QUERIES_IN_OTHER_NODES_ALL_POSITIVE_LARGE_RESULT:
                    runSpecialTest(session);
                default:
                    runTest(CURRENT_TEST, session);
            }

            session.close();
            cluster.close();
        } finally {
            SoundUtils.tone(100, 250);
        }
    }

    private static void assignKeysToNodes(Session session) {
        int[] node1Array = TWO_NODES_CLUSTER_NODE_1_KEYS;
        int[] node2Array = TWO_NODES_CLUSTER_NODE_2_KEYS;

        int node1Index = 0;
        int node2Index = 0;

        boolean node1Done = false;
        boolean node2Done = false;

        String node1 = "192.168.88.11";
        String node2 = "192.168.88.16";

        Runtime runtime = Runtime.getRuntime();

        for (int key : KEYS) {
            String[] commands  = {"/Users/sharafat/Documents/cassandra/bin/nodetool", "getendpoints", "cuckoo_test", "air_traffic",
                    Integer.toString(key)};
            Process process = null;
            try {
                process = runtime.exec(commands);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            BufferedReader lineReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String node = lineReader.lines().findFirst().get();

            if (node1.equals(node)) {
                if (!node1Done) {
                    node1Array[node1Index++] = key;
                }
            } else if (node2.equals(node)) {
                if (!node2Done) {
                    node2Array[node2Index++] = key;
                }
            } else {
                throw new RuntimeException("Unknown IP: " + node);
            }

            if (node1Index == 12) {
                if (node1Array == TWO_NODES_CLUSTER_NODE_1_KEYS) {
                    node1Array = TWO_NODES_CLUSTER_NODE_1_KEYS_DELETED;
                    node1Index = 0;
                } else if (node1Array == TWO_NODES_CLUSTER_NODE_1_KEYS_DELETED) {
                    node1Done = true;
                }
            }
            if (node2Index == 12) {
                if (node2Array == TWO_NODES_CLUSTER_NODE_2_KEYS) {
                    node2Array = TWO_NODES_CLUSTER_NODE_2_KEYS_DELETED;
                    node2Index = 0;
                } else if (node2Array == TWO_NODES_CLUSTER_NODE_2_KEYS_DELETED) {
                    node2Done = true;
                }
            }
            if (node1Done && node2Done) {
                break;
            }
        }

        if (!node1Done || !node2Done) {
            throw new RuntimeException("Keys exhausted before assigning 12 keys to each node");
        }

        node1Array = TWO_NODES_CLUSTER_NODE_1_KEYS_NON_EXISTENT;
        node2Array = TWO_NODES_CLUSTER_NODE_2_KEYS_NON_EXISTENT;
        node1Index = 0;
        node2Index = 0;
        node1Done = false;
        node2Done = false;
        for (int key = -1; key >= -100; key--) {
            String[] commands  = {"/Users/sharafat/Documents/cassandra/bin/nodetool", "getendpoints", "cuckoo_test", "air_traffic",
                    Integer.toString(key)};
            Process process = null;
            try {
                process = runtime.exec(commands);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            BufferedReader lineReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String node = lineReader.lines().findFirst().get();

            if (node1.equals(node)) {
                if (!node1Done) {
                    node1Array[node1Index++] = key;
                }
            } else if (node2.equals(node)) {
                if (!node2Done) {
                    node2Array[node2Index++] = key;
                }
            } else {
                throw new RuntimeException("Unknown IP: " + node);
            }

            if (node1Index == 12) {
                node1Done = true;
            }
            if (node2Index == 12) {
                node2Done = true;
            }
            if (node1Done && node2Done) {
                break;
            }
        }

        if (!node1Done || !node2Done) {
            throw new RuntimeException("Negative Keys exhausted before assigning 12 keys to each node");
        }

        printKeyNodeAssignments();

        deleteRows(session);
    }

    private static void printKeyNodeAssignments() {
        System.out.println("TWO_NODES_CLUSTER_NODE_1_KEYS: " + Arrays.toString(TWO_NODES_CLUSTER_NODE_1_KEYS));
        System.out.println("TWO_NODES_CLUSTER_NODE_2_KEYS: " + Arrays.toString(TWO_NODES_CLUSTER_NODE_2_KEYS));
        System.out.println("TWO_NODES_CLUSTER_NODE_1_KEYS_NON_EXISTENT: " + Arrays.toString(TWO_NODES_CLUSTER_NODE_1_KEYS_NON_EXISTENT));
        System.out.println("TWO_NODES_CLUSTER_NODE_2_KEYS_NON_EXISTENT: " + Arrays.toString(TWO_NODES_CLUSTER_NODE_2_KEYS_NON_EXISTENT));
        System.out.println("TWO_NODES_CLUSTER_NODE_1_KEYS_DELETED: " + Arrays.toString(TWO_NODES_CLUSTER_NODE_1_KEYS_DELETED));
        System.out.println("TWO_NODES_CLUSTER_NODE_2_KEYS_DELETED: " + Arrays.toString(TWO_NODES_CLUSTER_NODE_2_KEYS_DELETED));
    }

    private static void deleteRows(Session session) {
        for (int[] arr : new int[][]{TWO_NODES_CLUSTER_NODE_1_KEYS_DELETED, TWO_NODES_CLUSTER_NODE_2_KEYS_DELETED}) {
            for (int key : arr) {
                SimpleStatement statement = new SimpleStatement(String.format(DELETE_QUERY, key));
                executeQuery(-1, "DELETE", Integer.toString(key), session, statement);
            }
            for (int key : arr) {
                SimpleStatement statement = new SimpleStatement(String.format(LOOKUP_QUERY, key));
                executeQuery(-1, "LOOKUP_AFTER_DELETE", Integer.toString(key), session, statement);
            }
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

    private static void runSpecialTest(Session session) {
        String[] KEYS_CARRIER = new String[] {
                "WN","OO","MQ","US","UA","XE","DL","AA","EV","YV","FL","OH","NW","CO","9E","B6","F9","AS","HA","AQ"
        };
        String[] TWO_NODES_CLUSTER_NODE_1_KEYS_CARRIER = new String[]{"US", "EV", "YV", "OH", "CO", "9E", "F9", "HA"};
        String[] TWO_NODES_CLUSTER_NODE_2_KEYS_CARRIER = new String[]{"XE", "DL", "AA", "FL", "NW", "B6", "AS", "AQ"};

        String[] node1Keys = TWO_NODES_CLUSTER_NODE_1_KEYS_CARRIER;
        String[] node2Keys = TWO_NODES_CLUSTER_NODE_2_KEYS_CARRIER;

        int node1Index = 0;
        int node2Index = 0;

        boolean node1Done = false;
        boolean node2Done = false;

        String node1 = "192.168.88.11";
        String node2 = "192.168.88.16";

        Runtime runtime = Runtime.getRuntime();

        for (String key : KEYS_CARRIER) {
            String[] commands  = {"/Users/sharafat/Documents/cassandra/bin/nodetool", "getendpoints", "cuckoo_test", "air_traffic", key};
            Process process = null;
            try {
                process = runtime.exec(commands);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            BufferedReader lineReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String node = lineReader.lines().findFirst().get();

            if (node1.equals(node)) {
                if (!node1Done) {
                    node1Keys[node1Index++] = key;
                }
            } else if (node2.equals(node)) {
                if (!node2Done) {
                    node2Keys[node2Index++] = key;
                }
            } else {
                throw new RuntimeException("Unknown IP: " + node);
            }

            if (node1Index == 8) {
                node1Done = true;
            }
            if (node2Index == 8) {
                node2Done = true;
            }
            if (node1Done && node2Done) {
                break;
            }
        }

        if (!node1Done || !node2Done) {
            throw new RuntimeException("Keys exhausted before assigning 8 keys to each node");
        }

        Map<Integer, List<Double>> fractionDurationMap = new LinkedHashMap<Integer, List<Double>>();

        for (int run = 0; run < TEST_RUNS; run++) {
            for (Integer fraction : new Integer[]{0, 2, 4, 6, 8}) {
                List<String> ids = new ArrayList<String>();

                for (int j = 0; j < fraction; j++) {
                    ids.add(node2Keys[j]);
                }

                for (int j = fraction; j < node1Keys.length; j++) {
                    ids.add(node1Keys[j]);
                }

                String idsString = ids.toString().substring(1, ids.toString().length() - 1).replace(", ", "', '");
                String query = String.format("SELECT * FROM air_traffic WHERE \"UniqueCarrier\" IN ('%s')", idsString);
                Statement statement = new SimpleStatement(query);

                long start = System.currentTimeMillis();

                executeQuery(fraction, "SPECIAL_TEST", idsString, session, statement);

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
