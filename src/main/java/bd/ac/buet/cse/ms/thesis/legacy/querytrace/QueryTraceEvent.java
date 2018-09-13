package bd.ac.buet.cse.ms.thesis.legacy.querytrace;

public class QueryTraceEvent {

    public static final String EVENT_COMPUTING_RANGE = "Computing ranges";  // Computing ranges to query
    public static final String EVENT_SUBMITTING_RANGE = "Submitting range requests"; // Submitting range requests on 257 ranges with a concurrency of 1 (1.47475072E9 rows per range expected)
    public static final String EVENT_EXECUTE_SCAN = "Executing seq scan"; // Executing seq scan across 80 sstables for (min(-9223372036854775808), min(-9223372036854775808)]
    public static final String EVENT_READ = "Read";

    public String name;
    public long timestamp;
    public long time;
    public long duration;
    public String thread;
    public String desc;

}
