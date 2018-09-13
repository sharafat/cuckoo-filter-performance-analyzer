package bd.ac.buet.cse.ms.thesis.experiments;

import bd.ac.buet.cse.ms.thesis.Config;
import bd.ac.buet.cse.ms.thesis.utils.OutputWriter;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

public abstract class Experiment {

    private Session session;

    private OutputWriter outputWriter;

    private Map<Integer, Double> fractionDurationMap = new LinkedHashMap<Integer, Double>();

    public void setSession(Session session) {
        this.session = session;
    }

    public void setOutputWriter(OutputWriter outputWriter) {
        this.outputWriter = outputWriter;
    }

    /**
     * @return number of rows in result set
     */
    protected long executeQuery(int fraction, String segment, String key, Statement statement) {
        if (Config.DEBUG_LOG_QUERY_EXECUTION) {
            outputWriter.writeLine(new Date().toString() + ": Fraction: " + fraction + ", Segment: " + segment
                    + ", Key: " + key);
        }

        ResultSet resultSet = session.execute(statement.setReadTimeoutMillis(Config.READ_TIMEOUT_MILLIS));
        long rows = 0;
        while (resultSet.iterator().hasNext()) {
            resultSet.iterator().next();
            rows++;
        }

        if (Config.DEBUG_LOG_QUERY_EXECUTION) {
            outputWriter.writeLine(new Date().toString() + ": Fraction: " + fraction + ", Segment: " + segment
                    + ", Key: " + key + ", Rows: " + rows);
        }

        return rows;
    }

    protected void addResult(int fraction, double duration) {
        fractionDurationMap.put(fraction, duration);
    }

    protected void outputResults() {
        for (Map.Entry<Integer, Double> entry : fractionDurationMap.entrySet()) {
            outputWriter.writeLine(entry.getValue());
        }
    }

    public abstract void run();
}
