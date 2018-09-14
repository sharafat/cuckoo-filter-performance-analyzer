package bd.ac.buet.cse.ms.thesis.experiments.amazonreviews.multinode;

import bd.ac.buet.cse.ms.thesis.experiments.DataProvider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class LookupPerformanceInRemoteNodesDeletedRowsQueriesDataSizeWiseExperiment
        extends AmazonReviewsMultiNodeExperiment {

    @Override
    public String toString() {
        return "Lookup Performance by varying deleted data size, " +
                "meaning, some keys will return 10 GB of deleted data rows, some will return 20 GB of deleted data rows, etc.";
    }

    public void run() {
        for (Map.Entry<Integer, String[]> entry : remoteNodesData.getKeysThatHaveDataDeletedPerDataSize().entrySet()) {
            List<String> keys = new ArrayList<String>();

            Collections.addAll(keys, entry.getValue());

            long start = System.currentTimeMillis();

            executeQuery(-1, DataProvider.LABEL_DELETED_DATA, keys.toString(), getLookupStatement(keys));

            long end = System.currentTimeMillis();

            double durationInSeconds = (end - start) / 1000.0;

            addResult(entry.getKey(), durationInSeconds);
        }

        outputResults();
    }
}
