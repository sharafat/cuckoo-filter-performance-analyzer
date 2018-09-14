package bd.ac.buet.cse.ms.thesis.experiments.amazonreviews.multinode.data1;

import com.google.common.collect.ObjectArrays;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class ConnectedNodeData extends Data {

    /**
     * ~2GB data size per pair
     */
    private static final String[] KEYS_HAVING_DATA = new String[]{
            "Home_Improvement",
            "Outdoors",
            "Kitchen",
            "Digital_Video_Games",
            "Beauty",
            "Mobile_Electronics",
            "PC",
            "Personal_Care_Appliances"
    };

    private static final String[] KEYS_NOT_HAVING_DATA = new String[]{
            "D",
            "E",
            "U",
            "Z",
            "DD",
            "EE",
            "LL",
            "YY"
    };

    /**
     * ~2GB data size per pair
     */
    private static final String[] KEYS_HAVING_DATA_DELETED = KEYS_HAVING_DATA;

    @Override
    public String[] getKeysThatHaveData() {
        return KEYS_HAVING_DATA;
    }

    @Override
    public String[] getKeysThatDoNotHaveData() {
        return KEYS_NOT_HAVING_DATA;
    }

    @Override
    public String[] getKeysThatHaveDataDeleted() {
        return KEYS_HAVING_DATA_DELETED;
    }

    @Override
    public Map<Integer, String[]> getKeysThatHaveDataDeletedPerDataSize() {
        return new HashMap<Integer, String[]>();
    }
}
