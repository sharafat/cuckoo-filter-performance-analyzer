package bd.ac.buet.cse.ms.thesis.experiments.amazonreviews.multinode;

import com.datastax.driver.core.Statement;

import java.util.ArrayList;
import java.util.List;

public class LookupPerformanceExperiment extends AmazonReviewsMultiNodeExperiment {

    private String[] keySet1;
    private String[] keySet2;
    private String segment;

    public void run() {
        for (Integer fraction : connectedNodeData.getFractions()) {
            List<String> keys = new ArrayList<String>();

            for (int i = 0; i < fraction; i++) {
                keys.add(keySet2[i]);
            }

            for (int i = fraction; i < keySet1.length; i++) {
                keys.add(keySet1[i]);
            }

            Statement lookupStatement = getLookupStatement(keys);

            long start = System.currentTimeMillis();

            executeQuery(fraction, segment, keys.toString(), lookupStatement);

            long end = System.currentTimeMillis();

            double durationInSeconds = (end - start) / 1000.0;

            addResult(fraction, durationInSeconds);
        }

        outputResults();
    }

    void set(String[] keySet1, String[] keySet2, String segment) {
        this.keySet1 = keySet1;
        this.keySet2 = keySet2;
        this.segment = segment;
    }
}
