package bd.ac.buet.cse.ms.thesis;

import com.datastax.driver.core.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {

    private static final boolean POSITIVE_RESULTS = true;

    private static final String SERVER_IP = "127.0.0.1";
    private static final String KEYSPACE = "cuckoo_test";

    private static final String QUERY = "SELECT * FROM air_traffic WHERE \"FlightNum\" = ? LIMIT 1000 ALLOW FILTERING;";

    private static final String QUERY_TO_RUN = QUERY;

    private static final List<Integer> unavailableFlightNumbers = Arrays.asList(13, 666, 911);

    private static List<Integer> queryExecutionDurations = new ArrayList<Integer>(1000);

    public static void main(String[] args) {
        try {
            Cluster cluster = Cluster.builder()
                    .addContactPoints(SERVER_IP)
                    .build();

            Session session = cluster.connect(KEYSPACE);
            PreparedStatement preparedStatement = session.prepare(QUERY_TO_RUN).enableTracing();

            int startFlightNum = POSITIVE_RESULTS ? 1 : 10001;
            int endFlightNum = POSITIVE_RESULTS ? 1003 : 11000;

            for (int flightNum = startFlightNum; flightNum <= endFlightNum; flightNum++) {
                if (unavailableFlightNumbers.contains(flightNum)) {
                    continue;
                }

                BoundStatement statement = preparedStatement.bind(Integer.toString(flightNum));

                ResultSet resultSet = session.execute(statement);

                com.datastax.driver.core.QueryTrace queryTrace = resultSet.getExecutionInfo().getQueryTrace();
                int duration = getQueryExecutionDuration(queryTrace);
                queryExecutionDurations.add(duration);
            }

            printDurations(queryExecutionDurations);

            session.close();
            cluster.close();
        } finally {
            SoundUtils.tone(100,250);
        }
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
