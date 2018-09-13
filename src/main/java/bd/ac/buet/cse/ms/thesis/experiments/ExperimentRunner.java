package bd.ac.buet.cse.ms.thesis.experiments;

import bd.ac.buet.cse.ms.thesis.Config;
import bd.ac.buet.cse.ms.thesis.utils.OutputWriter;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;

import java.net.InetSocketAddress;
import java.util.ArrayList;

public class ExperimentRunner {

    private Session session;
    private OutputWriter outputWriter;

    public void prepare(OutputWriter outputWriter) {
        this.outputWriter = outputWriter;

        Cluster cluster = Cluster.builder()
                .addContactPoint(Config.SERVER_ADDRESS)
                .withLoadBalancingPolicy(new WhiteListPolicy(DCAwareRoundRobinPolicy.builder().build(),
                        new ArrayList<InetSocketAddress>() {{
                            add(new InetSocketAddress(Config.SERVER_ADDRESS, Config.SERVER_PORT));
                        }}))
                .withSocketOptions(
                        new SocketOptions()
                                .setConnectTimeoutMillis(Config.CONNECT_TIMEOUT_MILLIS)
                                .setReadTimeoutMillis(Config.READ_TIMEOUT_MILLIS)
                )
                .build();

        session = cluster.connect(Config.KEYSPACE);
    }

    public void runExperiments(Experiment[] experiments) {
        for (Experiment experiment : experiments) {
            experiment.setSession(session);
            experiment.setOutputWriter(outputWriter);
            outputWriter.writeLine(experiment.toString() + "\n");
            experiment.run();
        }
    }
}
