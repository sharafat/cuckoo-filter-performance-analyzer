package bd.ac.buet.cse.ms.thesis.experiments.amazonreviews.multinode;

import bd.ac.buet.cse.ms.thesis.experiments.DataProvider;

public class LookupPerformanceDeletedRowsQueriesRemoteNodesFractionWiseExperiment
        extends LookupPerformanceExperiment {

    @Override
    public String toString() {
        return "Lookup Performance by varying fraction of queries from connected vs. remote nodes while all queries resulting in deleted rows, " +
                "meaning, some keys will return deleted rows from connected node, while other keys will return deleted rows from remote nodes.";
    }

    public LookupPerformanceDeletedRowsQueriesRemoteNodesFractionWiseExperiment() {
        set(connectedNodeData.getKeysThatHaveDataDeleted(),
                remoteNodesData.getKeysThatHaveDataDeleted(),
                DataProvider.LABEL_SAME_AND_OTHER_NODES_ALL_DELETED);
    }
}
