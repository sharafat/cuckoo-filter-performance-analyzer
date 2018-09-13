package bd.ac.buet.cse.ms.thesis.experiments;

import java.util.Map;

public interface DataProvider<T> {

    String LABEL_HAS_DATA = "HAS_DATA";
    String LABEL_NO_DATA = "NO_DATA";
    String LABEL_DELETED_DATA = "DELETED_DATA";

    String LABEL_POS_NEG_ALL_REMOTE = "POS_NEG_ALL_REMOTE_NODES";
    String LABEL_SAME_AND_OTHER_NODES_ALL_POSITIVE = "SAME_AND_OTHER_NODES_ALL_POSITIVE";
    String LABEL_SAME_AND_OTHER_NODES_ALL_NEGATIVE = "SAME_AND_OTHER_NODES_ALL_NEGATIVE";
    String LABEL_SAME_AND_OTHER_NODES_ALL_DELETED = "SAME_AND_OTHER_NODES_ALL_DELETED";

    int[] getFractions();

    /**
     * Each consecutive pair in the returned array should return similar-size data1 cumulatively (e.g. 2 GB)
     */
    T[] getKeysThatHaveData();

    T[] getKeysThatDoNotHaveData();

    /**
     * Each consecutive pair in the returned array should have similar-size data1 deleted cumulatively (e.g. 2 GB)
     */
    T[] getKeysThatHaveDataDeleted();

    Map<Integer, T[]> getKeysThatHaveDataDeletedPerDataSize();

    String getLookupQuery();
}
