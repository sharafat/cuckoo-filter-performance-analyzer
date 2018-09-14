package bd.ac.buet.cse.ms.thesis.experiments.amazonreviews.multinode.data1;

import com.google.common.collect.ObjectArrays;

import java.util.LinkedHashMap;
import java.util.Map;

public class RemoteNodesData extends Data {

    /**
     * ~2GB data size per pair
     */
    private static final String[] KEYS_HAVING_DATA = new String[]{
            "Toys",
            "Software",
            "Digital_Video_Download",
            "Camera",
            "Music",
            "Tools",
            "Sports",
            "Software"
    };

    private static final String[] KEYS_NOT_HAVING_DATA = new String[]{
            "K",
            "L",
            "O",
            "Q",
            "Y",
            "PP",
            "SS",
            "YZ"
    };

    /**
     * ~2GB data size per pair
     */
    private static final String[] KEYS_HAVING_DATA_DELETED = KEYS_HAVING_DATA;

    private static final Map<Integer /* Data size in GB */, String[] /* Key list */> KEYS_HAVING_DATA_DELETED_SIZE_WISE
            = new LinkedHashMap<Integer, String[]>() {{
        String[] oneGb = new String[]{"Lawn_and_Garden"};
        String[] tenGb = new String[]{"Books", "Digital_Ebook_Purchase", "Electronics"};
        String[] twentyGb = ObjectArrays.concat(tenGb,
                new String[]{"Jewelry", "Automotive", "Pet_Products", "Camera", "Office_Products", "Home", "Toys"},
                String.class);
        String[] thirtyGb = ObjectArrays.concat(twentyGb,
                new String[]{"Video_DVD", "Grocery", "Lawn_and_Garden", "Health_Personal_Care", "Apparel", "Shoes"},
                String.class);
        String[] fortyGb = ObjectArrays.concat(thirtyGb,
                new String[]{"Digital_Video_Download", "Mobile_Apps", "Tools", "Jewelry", "Sports", "Music",
                        "Digital_Music_Purchase"},
                String.class);

        put(1, oneGb);
        put(10, tenGb);
        put(20, twentyGb);
        put(30, thirtyGb);
        put(40, fortyGb);
    }};

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
        return KEYS_HAVING_DATA_DELETED_SIZE_WISE;
    }
}
