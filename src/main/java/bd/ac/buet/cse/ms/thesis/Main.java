package bd.ac.buet.cse.ms.thesis;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class Main {

    private static final String SERVER_IP = "127.0.0.1";
    private static final String KEYSPACE = "cuckoo_test";

    private static final String QUERY_1 = "SELECT * FROM air_traffic WHERE \"Year\" = 2008 AND \"Month\" > 1 AND \"Month\" < 9 LIMIT 1000 ALLOW FILTERING;";

    private static final int QUERY_RUN_LENGTH = 1000;

    private static List<QueryTrace> queryTraces = new ArrayList<QueryTrace>();

    public static void main(String[] args) {

        Cluster cluster = Cluster.builder()
                .addContactPoints(SERVER_IP)
                .build();

        Session session = cluster.connect(KEYSPACE);

        BoundStatement statement = session.prepare(QUERY_1).enableTracing().bind();

        for (int i = 0; i < QUERY_RUN_LENGTH; i++) {
            ResultSet resultSet = session.execute(statement);

            com.datastax.driver.core.QueryTrace queryTrace = resultSet.getExecutionInfo().getQueryTrace();

            addQueryTraceResult(queryTrace);
//            printQueryTrace(queryTrace);
        }

        printQueryDurations(queryTraces);

        session.close();
        cluster.close();
    }

    private static void addQueryTraceResult(com.datastax.driver.core.QueryTrace queryTrace) {
        QueryTrace qTrace = new QueryTrace();
        qTrace.id = queryTrace.getTraceId().toString();

        for (com.datastax.driver.core.QueryTrace.Event event : queryTrace.getEvents()) {
            QueryTraceEvent qtEvent = new QueryTraceEvent();
            qtEvent.timestamp = event.getTimestamp();
            qtEvent.time = event.getSourceElapsedMicros();
            qtEvent.thread = event.getThreadName();
            qtEvent.desc = event.getDescription();

            if (event.getDescription().startsWith(QueryTraceEvent.EVENT_COMPUTING_RANGE)) {
                qtEvent.name = QueryTraceEvent.EVENT_COMPUTING_RANGE;
            } else if (event.getDescription().startsWith(QueryTraceEvent.EVENT_SUBMITTING_RANGE)) {
                qtEvent.name = QueryTraceEvent.EVENT_SUBMITTING_RANGE;
            } else if (event.getDescription().startsWith(QueryTraceEvent.EVENT_EXECUTE_SCAN)) {
                qtEvent.name = QueryTraceEvent.EVENT_EXECUTE_SCAN;
            }

            qTrace.events.add(qtEvent);
        }

        qTrace.computeEventDurations();

        queryTraces.add(qTrace);
    }

    private static void printQueryTrace(com.datastax.driver.core.QueryTrace queryTrace) {
        System.out.printf("\n\nQuery trace: %s\n\n", queryTrace.getTraceId());
        System.out.println("--------------------------------------------------------------------------------------------------------------");
        System.out.printf("%15s | %12s  | %30s | %s \n",
                "Timestamp",
                "Time (μs)",
                "Thead",
                "Description"
        );
        System.out.println("--------------------------------------------------------------------------------------------------------------");
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
        for (com.datastax.driver.core.QueryTrace.Event event : queryTrace.getEvents()) {
            System.out.printf("%15s | %12s  | %30s | %s \n",
                    sdf.format(event.getTimestamp()),
                    event.getSourceElapsedMicros(),
                    event.getThreadName(),
                    event.getDescription()

            );
        }
        System.out.println("--------------------------------------------------------------------------------------------------------------");
    }

    private static void printQueryDurations(List<QueryTrace> queryTraces) {
        System.out.print("\n\nQuery trace results in μs (with average μs per column):\n\n");
        System.out.println("--------------------------------------------------------------------------------------------");
        System.out.printf("%20s | %25s  | %20s | %5s \n",
                QueryTraceEvent.EVENT_COMPUTING_RANGE,
                QueryTraceEvent.EVENT_SUBMITTING_RANGE,
                QueryTraceEvent.EVENT_EXECUTE_SCAN,
                "Total"
        );
        System.out.println("--------------------------------------------------------------------------------------------");
        for (QueryTrace trace : queryTraces) {
            System.out.printf("%20s | %25s  | %20s | %5s \n",
                    trace.getEvent(QueryTraceEvent.EVENT_COMPUTING_RANGE).duration,
                    trace.getEvent(QueryTraceEvent.EVENT_SUBMITTING_RANGE).duration,
                    trace.getEvent(QueryTraceEvent.EVENT_EXECUTE_SCAN).duration,
                    trace.totalDuration
            );
        }
        System.out.println("--------------------------------------------------------------------------------------------");
        System.out.printf("%20s | %25s  | %20s | %5s \n",
                getAverage(queryTraces, QueryTraceEvent.EVENT_COMPUTING_RANGE),
                getAverage(queryTraces, QueryTraceEvent.EVENT_SUBMITTING_RANGE),
                getAverage(queryTraces, QueryTraceEvent.EVENT_EXECUTE_SCAN),
                getAverage(queryTraces, null)
        );
        System.out.println("--------------------------------------------------------------------------------------------");
    }

    private static long getAverage(List<QueryTrace> queryTraces, String eventName) {
        long totalDuration = 0;
        for (QueryTrace trace : queryTraces) {
            totalDuration += eventName == null ? trace.totalDuration : trace.getEvent(eventName).duration;
        }

        return totalDuration / queryTraces.size();
    }
}
