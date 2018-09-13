package bd.ac.buet.cse.ms.thesis.experiments.amazonreviews.multinode.data1;

import bd.ac.buet.cse.ms.thesis.experiments.DataProvider;

public abstract class Data  implements DataProvider<String> {

    private static final int[] FRACTIONS = new int[]{0, 2, 4, 6, 8};

    protected static final String LOOKUP_QUERY = "SELECT * FROM amazon_reviews WHERE product_category IN (%s)";
    private static final String DELETION_QUERY = "DELETE FROM amazon_reviews WHERE product_category IN ('Shoes', 'Furniture', 'Automotive', 'Digital_Music_Purchase', 'Pet_Products', 'Grocery', 'Home_Improvement', 'Lawn_and_Garden');";
    private static final String LOOKUP_AFTER_DELETION_QUERY = "SELECT * FROM amazon_reviews WHERE product_category IN ('Shoes', 'Furniture', 'Automotive', 'Digital_Music_Purchase', 'Pet_Products', 'Grocery', 'Home_Improvement', 'Lawn_and_Garden');";
    private static final String FULL_DELETION_QUERY = "DELETE FROM amazon_reviews WHERE product_category IN ('Lawn_and_Garden','Books','Digital_Ebook_Purchase','Electronics','Wireless','PC','Home','Toys','Video_DVD','Beauty','Health_Personal_Care','Apparel','Shoes','Digital_Video_Download','Mobile_Apps','Kitchen','Sports','Music','Digital_Music_Purchase','Watches','Video_Games','Automotive','Pet_Products','Home_Improvement','Office_Products','Lawn_and_Garden','Camera','Outdoors','Grocery','Tools','Jewelry');";
    private static final String LOOKUP_AFTER_FULL_DELETION_QUERY = "SELECT * FROM amazon_reviews WHERE product_category IN ('Lawn_and_Garden','Books','Digital_Ebook_Purchase','Electronics','Wireless','PC','Home','Toys','Video_DVD','Beauty','Health_Personal_Care','Apparel','Shoes','Digital_Video_Download','Mobile_Apps','Kitchen','Sports','Music','Digital_Music_Purchase','Watches','Video_Games','Automotive','Pet_Products','Home_Improvement','Office_Products','Lawn_and_Garden','Camera','Outdoors','Grocery','Tools','Jewelry');";

    @Override
    public int[] getFractions() {
        return FRACTIONS;
    }

    @Override
    public String getLookupQuery() {
        return LOOKUP_QUERY;
    }
}
