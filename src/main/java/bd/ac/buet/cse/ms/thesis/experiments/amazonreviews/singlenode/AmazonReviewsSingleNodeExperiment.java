package bd.ac.buet.cse.ms.thesis.experiments.amazonreviews.singlenode;

import bd.ac.buet.cse.ms.thesis.experiments.DataProvider;
import bd.ac.buet.cse.ms.thesis.experiments.Experiment;
import bd.ac.buet.cse.ms.thesis.experiments.amazonreviews.singlenode.data2.Data;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

public abstract class AmazonReviewsSingleNodeExperiment extends Experiment {

    protected DataProvider<String> data = new Data();

    private PreparedStatement lookupStatement;

    @Override
    public void setSession(Session session) {
        super.setSession(session);

        lookupStatement = session.prepare(data.getLookupQuery());
    }

    Statement getLookupStatement(String bindKey) {
        return lookupStatement.bind(bindKey);
    }
}
