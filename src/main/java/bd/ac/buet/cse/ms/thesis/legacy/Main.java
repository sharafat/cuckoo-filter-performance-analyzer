package bd.ac.buet.cse.ms.thesis.legacy;

import bd.ac.buet.cse.ms.thesis.legacy.querytrace.QueryTraceEvent;
import com.datastax.driver.core.*;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class Main {

    protected static final String SERVER_IP = "13.58.35.128";
    protected static final String KEYSPACE = "cuckoo_test";
    private static final String LOOKUP_QUERY = "SELECT * FROM amazon_reviews WHERE product_category = ?";
    private static final String LOOKUP_QUERY_MANY_KEYS = "SELECT * FROM air_traffic WHERE \"Id\" = ?";
    private static final String DELETE_QUERY = "DELETE FROM amazon_reviews WHERE product_category = ?";
    protected static final String DELETE_QUERY_MANY_KEYS = "DELETE FROM air_traffic WHERE \"Id\" = ?";
    private static final String INSERT_QUERY = "INSERT INTO amazon_reviews (marketplace, customer_id, review_id, product_id, product_parent, product_title, product_category, star_rating, helpful_votes, total_votes, vine, verified_purchase, review_headline, review_body, review_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String[] CARRIERS_HAVING_DATA = new String[]{
            "Digital_Video_Download",
            "Watches",
            "Automotive",
            "Digital_Music_Purchase",
            "Electronics",
            "Camera",
            "Grocery",
            "Outdoors"
    };

    private static final String[] CARRIERS_NOT_HAVING_DATA = new String[]{
            "BB", "CC", "DD", "EE", "FF", "GG", "AB", "AC"
    };

    private static final int[] IDS_HAVING_DATA = new int[]{
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
    };

    private static final int[] IDS_NOT_HAVING_DATA = new int[]{
            -1, -2, -3, -4, -5, -6, -7, -8, -9, -10, -11, -12
    };

    private static final String[] CARRIERS_HAVING_DATA_DELETED = new String[]{
            "Kitchen",
            "Digital_Software",
            "Toys",
            "Mobile_Electronics",
            "Sports",
            "Digital_Video_Games",
            "Music",
            "Software"
    };

    private static final Map<Integer /* Data size in GB */, String[] /* Key list */> CARRIERS_DATA_DELETED =
            new HashMap<Integer, String[]>() {{
                put( 1, new String[] { "Grocery" });
                put(10, new String[] { "Books", "Home" });
                put(20, new String[] { "Books", "Digital_Ebook_Purchase", "Wireless", "Apparel" });
                put(30, new String[] { "Books", "Digital_Ebook_Purchase", "Wireless", "Apparel", "PC", "Home", "Beauty", "Mobile_Apps", "Grocery" });
                put(40, new String[] { "Books", "Digital_Ebook_Purchase", "Wireless", "Apparel", "PC", "Home", "Beauty", "Mobile_Apps", "Grocery",
                                       "Shoes", "Music", "Sports", "Toys", "Kitchen"});
                put(50, new String[] { "Books", "Digital_Ebook_Purchase", "Wireless", "Apparel", "PC", "Home", "Beauty", "Mobile_Apps", "Grocery",
                                       "Shoes", "Music", "Sports", "Toys", "Kitchen", "Digital_Video_Download", "Automotive", "Electronics",
                                       "Outdoors", "Camera", "Jewelry", "Baby", "Tools", "Digital_Music_Purchase", "Watches", "Furniture"});
            }};

    public static final String[] INPUT_FILE_NAMES = new String[] {
            "amazon_reviews_us_Apparel_v1_00.tsv",
            "amazon_reviews_us_Automotive_v1_00.tsv",
            "amazon_reviews_us_Baby_v1_00.tsv",
            "amazon_reviews_us_Beauty_v1_00.tsv",
            "amazon_reviews_us_Books_v1_00.tsv",
            "amazon_reviews_us_Camera_v1_00.tsv",
            "amazon_reviews_us_Digital_Ebook_Purchase_v1_00.tsv",
            "amazon_reviews_us_Digital_Music_Purchase_v1_00.tsv",
            "amazon_reviews_us_Digital_Software_v1_00.tsv",
            "amazon_reviews_us_Digital_Video_Download_v1_00.tsv",
            "amazon_reviews_us_Digital_Video_Games_v1_00.tsv",
            "amazon_reviews_us_Electronics_v1_00.tsv",
            "amazon_reviews_us_Furniture_v1_00.tsv",
            "amazon_reviews_us_Gift_Card_v1_00.tsv",
            "amazon_reviews_us_Grocery_v1_00.tsv",
            "amazon_reviews_us_Health_Personal_Care_v1_00.tsv",
            "amazon_reviews_us_Home_Entertainment_v1_00.tsv",
            "amazon_reviews_us_Home_Improvement_v1_00.tsv",
            "amazon_reviews_us_Home_v1_00.tsv",
            "amazon_reviews_us_Jewelry_v1_00.tsv",
            "amazon_reviews_us_Kitchen_v1_00.tsv",
            "amazon_reviews_us_Lawn_and_Garden_v1_00.tsv",
            "amazon_reviews_us_Luggage_v1_00.tsv",
            "amazon_reviews_us_Major_Appliances_v1_00.tsv",
            "amazon_reviews_us_Mobile_Apps_v1_00.tsv",
            "amazon_reviews_us_Mobile_Electronics_v1_00.tsv",
            "amazon_reviews_us_Music_v1_00.tsv",
            "amazon_reviews_us_Musical_Instruments_v1_00.tsv",
            "amazon_reviews_us_Office_Products_v1_00.tsv",
            "amazon_reviews_us_Outdoors_v1_00.tsv",
            "amazon_reviews_us_PC_v1_00.tsv",
            "amazon_reviews_us_Personal_Care_Appliances_v1_00.tsv",
            "amazon_reviews_us_Pet_Products_v1_00.tsv",
            "amazon_reviews_us_Shoes_v1_00.tsv",
            "amazon_reviews_us_Software_v1_00.tsv",
            "amazon_reviews_us_Sports_v1_00.tsv",
            "amazon_reviews_us_Tools_v1_00.tsv",
            "amazon_reviews_us_Toys_v1_00.tsv",
            "amazon_reviews_us_Video_DVD_v1_00.tsv",
            "amazon_reviews_us_Video_Games_v1_00.tsv",
            "amazon_reviews_us_Video_v1_00.tsv",
            "amazon_reviews_us_Watches_v1_00.tsv",
            "amazon_reviews_us_Wireless_v1_00.tsv"
    };

    public static final String INPUT_FILE_PATH = "/home/ubuntu/review-data1/";

    protected static final Integer[] FRACTIONS = new Integer[]{0, 2, 4, 6, 8};
    private static final Integer[] FRACTIONS_FOR_DELETION = new Integer[]{0, 2, 4, 6, 8};

    protected static Map<Integer, Double> fractionDurationMap = new LinkedHashMap<Integer, Double>(FRACTIONS.length);
    protected static List<Double> durations = new ArrayList<Double>(CARRIERS_HAVING_DATA.length);

    private static final int TEST_LOOKUP_PERFORMANCE_POSITIVE_QUERY_FRACTION_WISE = 1;
    private static final int TEST_LOOKUP_PERFORMANCE_POSITIVE_QUERY_FRACTION_WISE_MANY_KEYS = 2;
    private static final int TEST_LOOKUP_PERFORMANCE_FILTER_LOAD_WISE = 3;
    private static final int TEST_LOOKUP_AFTER_DELETE = 4;
    private static final int TEST_LOOKUP_AFTER_DELETE_DELETED_QUERY_FRACTION_WISE = 5;
    private static final int TEST_LOOKUP_AFTER_DELETE_DATA_SIZE_WISE = 6;
    private static final int TEST_INSERTION = 7;

    private static final int CURRENT_TEST = TEST_INSERTION;

    public static void main(String[] args) {
        try {
            Cluster cluster = Cluster.builder()
                    .addContactPoints(SERVER_IP)
                    .withSocketOptions(
                            new SocketOptions()
                                    .setConnectTimeoutMillis(9999999)
                                    .setReadTimeoutMillis(9999999)
                    )
                    .build();

            Session session = cluster.connect(KEYSPACE);
            PreparedStatement lookupPreparedStatement = session.prepare(CURRENT_TEST == TEST_LOOKUP_PERFORMANCE_POSITIVE_QUERY_FRACTION_WISE_MANY_KEYS ? LOOKUP_QUERY_MANY_KEYS : LOOKUP_QUERY).enableTracing();
            PreparedStatement lookupManyKeysPreparedStatement = session.prepare(CURRENT_TEST == TEST_LOOKUP_PERFORMANCE_POSITIVE_QUERY_FRACTION_WISE_MANY_KEYS ? LOOKUP_QUERY_MANY_KEYS : LOOKUP_QUERY).enableTracing();
            PreparedStatement deletePreparedStatement = session.prepare(CURRENT_TEST == TEST_LOOKUP_PERFORMANCE_POSITIVE_QUERY_FRACTION_WISE_MANY_KEYS ? DELETE_QUERY_MANY_KEYS : DELETE_QUERY).enableTracing();
            PreparedStatement insertPreparedStatement = session.prepare(INSERT_QUERY);

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
                case TEST_LOOKUP_AFTER_DELETE_DELETED_QUERY_FRACTION_WISE:
                    runLookupAfterDeleteTestForVaryingDeletedDataPercentage(session, lookupPreparedStatement, deletePreparedStatement);
                case TEST_LOOKUP_AFTER_DELETE_DATA_SIZE_WISE:
                    runLookupAfterDeleteTestForVaryingDataSize(session, lookupPreparedStatement, deletePreparedStatement);
                    break;
                case TEST_INSERTION:
                    runInsertionTest(session, insertPreparedStatement);
                    break;
                default:
                    throw new RuntimeException("Unknown value for CURRENT_TEST: " + CURRENT_TEST);
            }

            session.close();
            cluster.close();
        } catch (Exception e) {
            System.out.println(new Date().toString() + ": " + e.getMessage());
            e.printStackTrace();
        } finally {
//            SoundUtils.tone(100, 250);
        }
    }

    private static void showNoOfRows(Session session, PreparedStatement preparedStatement) {
        for (String key : CARRIERS_HAVING_DATA) {
            BoundStatement statement = preparedStatement.bind(key);
            long rows = executeQuery(-1, "HAS_DATA", key, session, statement);
            String log = "Key " + key + " has " + rows + " rows.\n";
            System.out.print(log);
            try {
                Files.write(Paths.get("~/myfile.txt"), log.getBytes(), StandardOpenOption.APPEND);
            }catch (IOException ignored) {
            }
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

    private static void runLookupAfterDeleteTestForVaryingDeletedDataPercentage(Session session, PreparedStatement lookupPreparedStatement,
                                                 PreparedStatement deletePreparedStatement) {
//        for (String key : CARRIERS_HAVING_DATA_DELETED) {
//            BoundStatement deleteBoundStatement = deletePreparedStatement.bind(key);
//            executeQuery(-1, "DELETING_DATA", key, session, deleteBoundStatement);
//        }
//
//        for (String key : CARRIERS_HAVING_DATA_DELETED) {
//            BoundStatement statement = lookupPreparedStatement.bind(key);
//            executeQuery(-1, "READING_BACK_DELETED_DATA", key, session, statement);
//        }

        for (Integer fraction : FRACTIONS_FOR_DELETION) {
            long start = System.currentTimeMillis();

            for (int j = 0; j < fraction; j++) {
                BoundStatement statement = lookupPreparedStatement.bind(CARRIERS_HAVING_DATA_DELETED[j]);
                executeQuery(fraction, "_DELETED_DATA", CARRIERS_HAVING_DATA_DELETED[j], session, statement);
            }

            for (int j = fraction; j < CARRIERS_HAVING_DATA_DELETED.length; j++) {
                BoundStatement statement = lookupPreparedStatement.bind(CARRIERS_HAVING_DATA[j]);
                executeQuery(fraction, "HAS_DATA", CARRIERS_HAVING_DATA[j], session, statement);
            }

            long end = System.currentTimeMillis();

            double duration = (end - start) / 1000.0;

            fractionDurationMap.put(fraction, duration);
        }

        for (Map.Entry<Integer, Double> entry : fractionDurationMap.entrySet()) {
            System.out.println(entry.getValue());
        }
    }

    private static void runLookupAfterDeleteTestForVaryingDataSize(Session session, PreparedStatement lookupPreparedStatement,
                                                                                PreparedStatement deletePreparedStatement) {
//        for (String key : CARRIERS_DATA_DELETED.get(50)) {
//            BoundStatement deleteBoundStatement = deletePreparedStatement.bind(key);
//            executeQuery(-1, "DELETING_DATA", key, session, deleteBoundStatement);
//        }
//
//        for (String key : CARRIERS_DATA_DELETED.get(50)) {
//            BoundStatement statement = lookupPreparedStatement.bind(key);
//            executeQuery(-1, "READING_BACK_DELETED_DATA", key, session, statement);
//        }

        for (Map.Entry<Integer, String[]> entry : CARRIERS_DATA_DELETED.entrySet()) {
            long start = System.currentTimeMillis();

            for (String key : entry.getValue()) {
                BoundStatement statement = lookupPreparedStatement.bind(key);
                executeQuery(-1, "_DELETED_DATA", key, session, statement);
            }

            long end = System.currentTimeMillis();

            double duration = (end - start) / 1000.0;

            fractionDurationMap.put(entry.getKey(), duration);
        }

        for (Map.Entry<Integer, Double> entry : fractionDurationMap.entrySet()) {
            System.out.println(entry.getKey() + " GB. Duration (seconds): " + entry.getValue());
        }
    }

    private static void runInsertionTest(Session session, PreparedStatement preparedStatement) throws FileNotFoundException {
        TsvParserSettings settings = new TsvParserSettings();
        settings.setMaxCharsPerColumn(999999);
        TsvParser parser = new TsvParser(settings);

        long start = System.currentTimeMillis();

        long rows = 0;

        for (String inputFileName : INPUT_FILE_NAMES) {
            long fileRows = 0;

            long innerStart = System.currentTimeMillis();

            int idx = -1;
            for (String[] row : parser.iterate(new FileReader(INPUT_FILE_PATH + inputFileName))){
                idx++;
                if (idx == 0) {
                    // header row
                    continue;
                }

                String[] values = new String[15];
                for (int i = 0; i < values.length; i++) {
                    values[i] = i < row.length ? row[i] : null;
                }
                BoundStatement statement = preparedStatement.bind(values[0], values[1], values[2], values[3], values[4],
                        values[5], values[6],
                        values[7] != null ? Integer.parseInt(values[7]) : null,
                        values[7] != null ? Integer.parseInt(values[8]) : null,
                        values[7] != null ? Integer.parseInt(values[9]) : null,
                        values[10], values[11], values[12], values[13], values[14]);
                executeQuery(-1, "INSERT", Arrays.toString(values), session, statement);
                fileRows++;
            }

            long innerEnd = System.currentTimeMillis();

            rows += fileRows;

            System.out.println(inputFileName + " Rows: " + fileRows + ", duration: " + ((innerEnd - innerStart) / 1000.0) + " seconds.");
        }

        long end = System.currentTimeMillis();

        double duration = (end - start) / 1000.0;

        System.out.println("Total Rows: " + rows + ", duration: " + duration + " seconds");
    }

    private static long executeQuery(int fraction, String segment, String key, Session session, BoundStatement statement) {
//        System.out.println(new Date().toString() + ": Fraction: " + fraction + ", Segment: " + segment + ", Key: " + key);

        ResultSet resultSet = session.execute(statement.setReadTimeoutMillis(9999999));
        long rows = 0;
        while (resultSet.iterator().hasNext()) {
            resultSet.iterator().next();
            rows++;
        }

//        System.out.println(new Date().toString() + ": Fraction: " + fraction + ", Segment: " + segment + ", Key: " + key + ", Rows: " + rows);

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
