package bd.ac.buet.cse.ms.thesis.experiments.amazonreviews.singlenode;

import bd.ac.buet.cse.ms.thesis.experiments.DataProvider;

public class LookupPerformancePositiveResultQueryFractionWiseExperiment extends AmazonReviewsSingleNodeExperiment {

    @Override
    public String toString() {
        return "Lookup Performance by varying fraction of queries returning positive results, " +
                "meaning, some keys will return positive results, while other keys will not return any result.";
    }

    public void run() {
        for (Integer fraction : data.getFractions()) {
            long start = System.currentTimeMillis();

            String[] keysThatHaveData = data.getKeysThatHaveData();
            for (int i = 0; i < fraction; i++) {
                executeQuery(fraction, DataProvider.LABEL_HAS_DATA, keysThatHaveData[i],
                        getLookupStatement(keysThatHaveData[i]));
            }

            String[] keysThatDoNotHaveData = data.getKeysThatDoNotHaveData();
            for (int i = fraction; i < keysThatDoNotHaveData.length; i++) {
                executeQuery(fraction, DataProvider.LABEL_NO_DATA, keysThatDoNotHaveData[i],
                        getLookupStatement(keysThatDoNotHaveData[i]));
            }

            long end = System.currentTimeMillis();

            double durationInSeconds = (end - start) / 1000.0;

            addResult(fraction, durationInSeconds);
        }

        outputResults();
    }
}
