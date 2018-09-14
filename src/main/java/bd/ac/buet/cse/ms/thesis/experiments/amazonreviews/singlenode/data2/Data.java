package bd.ac.buet.cse.ms.thesis.experiments.amazonreviews.singlenode.data2;

import bd.ac.buet.cse.ms.thesis.experiments.DataProvider;
import com.google.common.collect.ObjectArrays;

import java.util.LinkedHashMap;
import java.util.Map;

public class Data implements DataProvider<String> {

    private static final int[] FRACTIONS = new int[]{0, 2, 4, 6, 8};

    /**
     * ~2GB data size per pair
     */
    private static final String[] KEYS_HAVING_DATA = new String[]{
            "Beauty",
            "Personal_Care_Appliances",
            "Video_DVD",
            "Digital_Software",
            "Mobile_Apps",
            "Mobile_Electronics",
            "Sports",
            "Luggage"
    };

    private static final String[] KEYS_NOT_HAVING_DATA = new String[]{
            "A",
            "B",
            "C",
            "D",
            "E",
            "F",
            "G",
            "H"
    };

    /**
     * ~2GB data size per pair
     */
    private static final String[] KEYS_HAVING_DATA_DELETED = new String[]{
            "Shoes",
            "Furniture",
            "Automotive",
            "Digital_Music_Purchase",
            "Pet_Products",
            "Grocery",
            "Home_Improvement",
            "Lawn_and_Garden"
    };

    private static final Map<Integer /* Data size in GB */, String[] /* Key list */> KEYS_HAVING_DATA_DELETED_SIZE_WISE
            = new LinkedHashMap<Integer, String[]>() {{
        String[] oneGb = new String[]{"Lawn_and_Garden"};
        String[] tenGb = new String[]{"Books", "Digital_Ebook_Purchase", "Electronics"};
        String[] twentyGb = ObjectArrays.concat(tenGb,
                new String[]{"Wireless", "PC", "Home", "Toys"},
                String.class);
        String[] thirtyGb = ObjectArrays.concat(twentyGb,
                new String[]{"Video_DVD", "Beauty", "Health_Personal_Care", "Apparel", "Shoes"},
                String.class);
        String[] fortyGb = ObjectArrays.concat(thirtyGb,
                new String[]{"Digital_Video_Download", "Mobile_Apps", "Kitchen", "Sports", "Music",
                        "Digital_Music_Purchase", "Watches"},
                String.class);
        String[] fiftyGb = ObjectArrays.concat(fortyGb,
                new String[]{"Video_Games", "Automotive", "Pet_Products", "Home_Improvement", "Office_Products",
                        "Lawn_and_Garden", "Camera", "Outdoors", "Grocery", "Tools", "Jewelry"},
                String.class);

        put(1, oneGb);
        put(10, tenGb);
        put(20, twentyGb);
        put(30, thirtyGb);
        put(40, fortyGb);
        put(50, fiftyGb);
    }};

    private static final String LOOKUP_QUERY = "SELECT * FROM amazon_reviews WHERE product_category = ?";
    private static final String DELETION_QUERY = "DELETE FROM amazon_reviews WHERE product_category IN ('Shoes', 'Furniture', 'Automotive', 'Digital_Music_Purchase', 'Pet_Products', 'Grocery', 'Home_Improvement', 'Lawn_and_Garden');";
    private static final String LOOKUP_AFTER_DELETION_QUERY = "SELECT * FROM amazon_reviews WHERE product_category IN ('Shoes', 'Furniture', 'Automotive', 'Digital_Music_Purchase', 'Pet_Products', 'Grocery', 'Home_Improvement', 'Lawn_and_Garden');";
    private static final String FULL_DELETION_QUERY = "DELETE FROM amazon_reviews WHERE product_category IN ('Lawn_and_Garden','Books','Digital_Ebook_Purchase','Electronics','Wireless','PC','Home','Toys','Video_DVD','Beauty','Health_Personal_Care','Apparel','Shoes','Digital_Video_Download','Mobile_Apps','Kitchen','Sports','Music','Digital_Music_Purchase','Watches','Video_Games','Automotive','Pet_Products','Home_Improvement','Office_Products','Lawn_and_Garden','Camera','Outdoors','Grocery','Tools','Jewelry');";
    private static final String LOOKUP_AFTER_FULL_DELETION_QUERY = "SELECT * FROM amazon_reviews WHERE product_category IN ('Lawn_and_Garden','Books','Digital_Ebook_Purchase','Electronics','Wireless','PC','Home','Toys','Video_DVD','Beauty','Health_Personal_Care','Apparel','Shoes','Digital_Video_Download','Mobile_Apps','Kitchen','Sports','Music','Digital_Music_Purchase','Watches','Video_Games','Automotive','Pet_Products','Home_Improvement','Office_Products','Lawn_and_Garden','Camera','Outdoors','Grocery','Tools','Jewelry');";

    @Override
    public int[] getFractions() {
        return FRACTIONS;
    }

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

    @Override
    public String getLookupQuery() {
        return LOOKUP_QUERY;
    }
}
