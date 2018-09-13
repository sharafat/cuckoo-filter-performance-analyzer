package bd.ac.buet.cse.ms.thesis.experiments.amazonreviews.multinode;

import bd.ac.buet.cse.ms.thesis.experiments.DataProvider;

public class LookupPerformanceInRemoteNodesPositiveResultQueryFractionWiseExperiment
        extends LookupPerformanceExperiment {

    @Override
    public String toString() {
        return "Lookup Performance by varying fraction of queries returning positive results from remote nodes only, " +
                "meaning, some keys will return positive results from remote nodes, while other keys will not return any result from remote nodes.";
    }

    public LookupPerformanceInRemoteNodesPositiveResultQueryFractionWiseExperiment() {
        set(remoteNodesData.getKeysThatHaveData(),
                remoteNodesData.getKeysThatDoNotHaveData(),
                DataProvider.LABEL_POS_NEG_ALL_REMOTE);
    }
}
