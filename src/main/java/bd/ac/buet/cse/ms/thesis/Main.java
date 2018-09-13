package bd.ac.buet.cse.ms.thesis;

import bd.ac.buet.cse.ms.thesis.experiments.*;
import bd.ac.buet.cse.ms.thesis.experiments.amazonreviews.singlenode.LookupPerformanceAfterDeletionDataSizeWiseExperiment;
import bd.ac.buet.cse.ms.thesis.experiments.amazonreviews.singlenode.LookupPerformanceAfterDeletionQueryFractionWiseExperiment;
import bd.ac.buet.cse.ms.thesis.experiments.amazonreviews.singlenode.LookupPerformancePositiveResultQueryFractionWiseExperiment;
import bd.ac.buet.cse.ms.thesis.utils.ConsoleWriter;
import bd.ac.buet.cse.ms.thesis.utils.OutputWriter;

public class Main {

    private static final Experiment[] EXPERIMENTS = new Experiment[] {
//            new LookupPerformancePositiveResultQueryFractionWiseExperiment(),
            new LookupPerformanceAfterDeletionQueryFractionWiseExperiment(),
            new LookupPerformanceAfterDeletionDataSizeWiseExperiment(),
            new DummyExperiment()
    };

    private static final OutputWriter OUTPUT_WRITER = new ConsoleWriter();

    public static void main(String[] args) {
        ExperimentRunner experimentRunner = new ExperimentRunner();
        experimentRunner.prepare(OUTPUT_WRITER);

        experimentRunner.runExperiments(EXPERIMENTS);
    }
}
