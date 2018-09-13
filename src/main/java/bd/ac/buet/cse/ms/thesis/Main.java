package bd.ac.buet.cse.ms.thesis;

import bd.ac.buet.cse.ms.thesis.experiments.*;
import bd.ac.buet.cse.ms.thesis.experiments.amazonreviews.multinode.LookupPerformanceDeletedRowsQueriesRemoteNodesFractionWiseExperiment;
import bd.ac.buet.cse.ms.thesis.experiments.amazonreviews.multinode.LookupPerformanceInRemoteNodesPositiveResultQueryFractionWiseExperiment;
import bd.ac.buet.cse.ms.thesis.experiments.amazonreviews.multinode.LookupPerformanceNegativeResultQueriesRemoteNodesFractionWiseExperiment;
import bd.ac.buet.cse.ms.thesis.experiments.amazonreviews.singlenode.LookupPerformanceAfterDeletionDataSizeWiseExperiment;
import bd.ac.buet.cse.ms.thesis.experiments.amazonreviews.singlenode.LookupPerformanceAfterDeletionQueryFractionWiseExperiment;
import bd.ac.buet.cse.ms.thesis.experiments.amazonreviews.singlenode.LookupPerformancePositiveResultQueryFractionWiseExperiment;
import bd.ac.buet.cse.ms.thesis.utils.ConsoleWriter;
import bd.ac.buet.cse.ms.thesis.utils.OutputWriter;

public class Main {

    private static final Experiment[] EXPERIMENTS = new Experiment[] {
            // Amazon Reviews Data on Single Node
//            new LookupPerformancePositiveResultQueryFractionWiseExperiment(),
//            new LookupPerformanceAfterDeletionQueryFractionWiseExperiment(),
            new LookupPerformanceAfterDeletionDataSizeWiseExperiment(),

            // Amazon Reviews Data on Multi Nodes
//            new LookupPerformanceInRemoteNodesPositiveResultQueryFractionWiseExperiment(),
//            new LookupPerformancePositiveResultQueriesRemoteNodesFractionWiseExperiment(),
//            new LookupPerformanceNegativeResultQueriesRemoteNodesFractionWiseExperiment(),
//            new LookupPerformanceDeletedRowsQueriesRemoteNodesFractionWiseExperiment(),

            // Dummy
            new DummyExperiment()
    };

    private static final OutputWriter OUTPUT_WRITER = new ConsoleWriter();

    public static void main(String[] args) {
        ExperimentRunner experimentRunner = new ExperimentRunner();
        experimentRunner.prepare(OUTPUT_WRITER);

        experimentRunner.runExperiments(EXPERIMENTS);
    }
}
