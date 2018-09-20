package bd.ac.buet.cse.ms.thesis.experiments.amazonreviews.multinode.data1;

import bd.ac.buet.cse.ms.thesis.experiments.DataProvider;

public abstract class Data  implements DataProvider<String> {

    private static final int[] FRACTIONS = new int[]{0, 2, 4, 6, 8};

    protected static final String LOOKUP_QUERY = "SELECT * FROM amazon_reviews WHERE product_category IN ('%s')";
    private static final String DELETION_QUERY = "DELETE FROM amazon_reviews WHERE product_category IN ('Home_Improvement', 'Outdoors', 'Kitchen', 'Digital_Video_Games', 'Beauty', 'Mobile_Electronics', 'PC', 'Personal_Care_Appliances', 'Toys', 'Software', 'Digital_Video_Download', 'Camera', 'Music', 'Tools', 'Sports', 'Software');";
    private static final String LOOKUP_AFTER_DELETION_QUERY = "SELECT * FROM amazon_reviews WHERE product_category IN ('Toys', 'Software', 'Digital_Video_Download', 'Camera', 'Music', 'Tools', 'Sports', 'Software');";
    private static final String FULL_DELETION_QUERY = "DELETE FROM amazon_reviews WHERE product_category IN ('Lawn_and_Garden', 'Books', 'Digital_Ebook_Purchase', 'Electronics', 'Jewelry', 'Automotive', 'Pet_Products', 'Camera', 'Office_Products', 'Home', 'Toys', 'Video_DVD', 'Grocery', 'Health_Personal_Care', 'Apparel', 'Shoes', 'Digital_Video_Download', 'Mobile_Apps', 'Tools', 'Jewelry', 'Sports', 'Music', 'Digital_Music_Purchase');";
    private static final String LOOKUP_AFTER_FULL_DELETION_QUERY = "SELECT * FROM amazon_reviews WHERE product_category IN ('Lawn_and_Garden', 'Books', 'Digital_Ebook_Purchase', 'Electronics', 'Jewelry', 'Automotive', 'Pet_Products', 'Camera', 'Office_Products', 'Home', 'Toys', 'Video_DVD', 'Grocery', 'Health_Personal_Care', 'Apparel', 'Shoes', 'Digital_Video_Download', 'Mobile_Apps', 'Tools', 'Jewelry', 'Sports', 'Music', 'Digital_Music_Purchase');";

    @Override
    public int[] getFractions() {
        return FRACTIONS;
    }

    @Override
    public String getLookupQuery() {
        return LOOKUP_QUERY;
    }
}
