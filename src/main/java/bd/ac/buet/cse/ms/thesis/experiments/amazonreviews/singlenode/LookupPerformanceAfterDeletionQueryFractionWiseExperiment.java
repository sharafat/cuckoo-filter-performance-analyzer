package bd.ac.buet.cse.ms.thesis.experiments.amazonreviews.singlenode;

import bd.ac.buet.cse.ms.thesis.experiments.DataProvider;

public class LookupPerformanceAfterDeletionQueryFractionWiseExperiment extends AmazonReviewsSingleNodeExperiment {

    @Override
    public String toString() {
        return "Lookup Performance by varying fraction of queries returning deleted rows, " +
                "meaning, some keys will return positive results, while other keys will return deleted rows.";
    }

    public void run() {
        for (Integer fraction : data.getFractions()) {
            long start = System.currentTimeMillis();

            String[] keys = data.getKeysThatHaveDataDeleted();
            for (int i = 0; i < fraction; i++) {
                executeQuery(fraction, DataProvider.LABEL_DELETED_DATA, keys[i], getLookupStatement(keys[i]));
            }

            keys = data.getKeysThatHaveData();
            for (int i = fraction; i < keys.length; i++) {
                executeQuery(fraction, DataProvider.LABEL_HAS_DATA, keys[i], getLookupStatement(keys[i]));
            }

            long end = System.currentTimeMillis();

            double durationInSeconds = (end - start) / 1000.0;

            addResult(fraction, durationInSeconds);
        }

        outputResults();
    }
}
