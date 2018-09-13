package bd.ac.buet.cse.ms.thesis.experiments.amazonreviews.multinode;

import bd.ac.buet.cse.ms.thesis.experiments.DataProvider;

public class LookupPerformanceNegativeResultQueriesRemoteNodesFractionWiseExperiment
        extends LookupPerformanceExperiment {

    @Override
    public String toString() {
        return "Lookup Performance by varying fraction of queries from connected vs. remote nodes while all queries resulting in no rows, " +
                "meaning, some keys will return no rows from connected node, while other keys will return no rows from remote nodes.";
    }

    public LookupPerformanceNegativeResultQueriesRemoteNodesFractionWiseExperiment() {
        set(connectedNodeData.getKeysThatDoNotHaveData(),
                remoteNodesData.getKeysThatDoNotHaveData(),
                DataProvider.LABEL_SAME_AND_OTHER_NODES_ALL_NEGATIVE);
    }
}
