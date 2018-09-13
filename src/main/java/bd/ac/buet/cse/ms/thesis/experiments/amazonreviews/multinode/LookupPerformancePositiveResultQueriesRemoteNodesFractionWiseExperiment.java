package bd.ac.buet.cse.ms.thesis.experiments.amazonreviews.multinode;

import bd.ac.buet.cse.ms.thesis.experiments.DataProvider;

public class LookupPerformancePositiveResultQueriesRemoteNodesFractionWiseExperiment
        extends LookupPerformanceExperiment {

    @Override
    public String toString() {
        return "Lookup Performance by varying fraction of queries from connected vs. remote nodes while all queries resulting in positive, " +
                "meaning, some keys will return positive results from connected node, while other keys will return positive results from remote nodes.";
    }

    public LookupPerformancePositiveResultQueriesRemoteNodesFractionWiseExperiment() {
        set(connectedNodeData.getKeysThatHaveData(),
                remoteNodesData.getKeysThatHaveData(),
                DataProvider.LABEL_SAME_AND_OTHER_NODES_ALL_POSITIVE);
    }
}
