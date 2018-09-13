package bd.ac.buet.cse.ms.thesis.legacy.querytrace;

import java.util.ArrayList;
import java.util.List;

public class QueryTrace {

    public List<QueryTraceEvent> events = new ArrayList<QueryTraceEvent>();

    public String id;
    public long totalDuration;

    public void computeEventDurations() {
        for (int i = 0; i < events.size() - 1; i++) {
            QueryTraceEvent event = events.get(i);
            event.duration = events.get(i + 1).time - event.time;

            totalDuration += event.duration;
        }
    }

    public QueryTraceEvent getEvent(String name) {
        for (QueryTraceEvent event : events) {
            if (name.equals(event.name)) {
                return event;
            }
        }

        throw new RuntimeException("Event '" + name + "' not found!");
    }
}
