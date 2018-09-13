package bd.ac.buet.cse.ms.thesis.experiments.amazonreviews.singlenode;

import bd.ac.buet.cse.ms.thesis.experiments.DataProvider;

import java.util.Map;

public class LookupPerformanceAfterDeletionDataSizeWiseExperiment extends AmazonReviewsSingleNodeExperiment {

    @Override
    public String toString() {
        return "Lookup Performance by varying deleted data size, " +
                "meaning, some keys will return 10 GB of deleted data rows, some will return 20 GB of deleted data rows, etc.";
    }

    public void run() {
        for (Map.Entry<Integer, String[]> entry : data.getKeysThatHaveDataDeletedPerDataSize().entrySet()) {
            long start = System.currentTimeMillis();

            for (String key : entry.getValue()) {
                executeQuery(-1, DataProvider.LABEL_DELETED_DATA, key, getLookupStatement(key));
            }

            long end = System.currentTimeMillis();

            double durationInSeconds = (end - start) / 1000.0;

            addResult(entry.getKey(), durationInSeconds);
        }

        outputResults();
    }
}
