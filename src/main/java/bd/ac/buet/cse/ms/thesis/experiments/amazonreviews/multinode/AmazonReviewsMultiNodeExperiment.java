package bd.ac.buet.cse.ms.thesis.experiments.amazonreviews.multinode;

import bd.ac.buet.cse.ms.thesis.experiments.DataProvider;
import bd.ac.buet.cse.ms.thesis.experiments.Experiment;
import bd.ac.buet.cse.ms.thesis.experiments.amazonreviews.multinode.data1.ConnectedNodeData;
import bd.ac.buet.cse.ms.thesis.experiments.amazonreviews.multinode.data1.RemoteNodesData;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;

import java.util.List;

public abstract class AmazonReviewsMultiNodeExperiment extends Experiment {

    protected DataProvider<String> connectedNodeData = new ConnectedNodeData();
    protected DataProvider<String> remoteNodesData = new RemoteNodesData();

    private String lookupQuery;

    @Override
    public void setSession(Session session) {
        super.setSession(session);

        lookupQuery = connectedNodeData.getLookupQuery();
    }

    Statement getLookupStatement(List<String> bindKeys) {
        String bindKeysString = bindKeys.toString()
                .substring(1, bindKeys.toString().length() - 1)
                .replace(", ", "', '");
        String query = String.format(lookupQuery, bindKeysString);

        return new SimpleStatement(query);
    }
}
