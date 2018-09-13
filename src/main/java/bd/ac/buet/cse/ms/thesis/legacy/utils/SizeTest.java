package bd.ac.buet.cse.ms.thesis.legacy.utils;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class SizeTest {

    public static String URL_PREFIX = "https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_";

    public static String[] URLS = new String[]{
            "Wireless_v1_00.tsv.gz",
            "Watches_v1_00.tsv.gz",
            "Video_Games_v1_00.tsv.gz",
            "Video_DVD_v1_00.tsv.gz",
            "Video_v1_00.tsv.gz",
            "Toys_v1_00.tsv.gz",
            "Tools_v1_00.tsv.gz",
            "Sports_v1_00.tsv.gz",
            "Software_v1_00.tsv.gz",
            "Shoes_v1_00.tsv.gz",
            "Pet_Products_v1_00.tsv.gz",
            "Personal_Care_Appliances_v1_00.tsv.gz",
            "PC_v1_00.tsv.gz",
            "Outdoors_v1_00.tsv.gz",
            "Office_Products_v1_00.tsv.gz",
            "Musical_Instruments_v1_00.tsv.gz",
            "Music_v1_00.tsv.gz",
            "Mobile_Electronics_v1_00.tsv.gz",
            "Mobile_Apps_v1_00.tsv.gz",
            "Major_Appliances_v1_00.tsv.gz",
            "Luggage_v1_00.tsv.gz",
            "Lawn_and_Garden_v1_00.tsv.gz",
            "Kitchen_v1_00.tsv.gz",
            "Jewelry_v1_00.tsv.gz",
            "Home_Improvement_v1_00.tsv.gz",
            "Home_Entertainment_v1_00.tsv.gz",
            "Home_v1_00.tsv.gz",
            "Health_Personal_Care_v1_00.tsv.gz",
            "Grocery_v1_00.tsv.gz",
            "Gift_Card_v1_00.tsv.gz",
            "Furniture_v1_00.tsv.gz",
            "Electronics_v1_00.tsv.gz",
            "Digital_Video_Games_v1_00.tsv.gz",
            "Digital_Video_Download_v1_00.tsv.gz",
            "Digital_Software_v1_00.tsv.gz",
            "Digital_Music_Purchase_v1_00.tsv.gz",
            "Digital_Ebook_Purchase_v1_01.tsv.gz",
            "Digital_Ebook_Purchase_v1_00.tsv.gz",
            "Camera_v1_00.tsv.gz",
            "Books_v1_02.tsv.gz",
            "Books_v1_01.tsv.gz",
            "Books_v1_00.tsv.gz",
            "Beauty_v1_00.tsv.gz",
            "Baby_v1_00.tsv.gz",
            "Automotive_v1_00.tsv.gz",
            "Apparel_v1_00.tsv.gz"
    };

    public static void main(String args[]) throws Exception {
        Map<Long, String> sizeMap = new TreeMap<>(new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                return o1 < o2 ? -1 : o1 > o2 ? 1 : 0;
            }
        });

        long totalSize = 0;
        for (String url : URLS) {
            long size = getFileSize(new URL(URL_PREFIX + url));
            totalSize += size;
            sizeMap.put(size, url);
        }

        for (Map.Entry<Long, String> entry : sizeMap.entrySet()) {
            System.out.println((entry.getKey() / 1024.0 / 1024.0 / 1024.0) + "   " + entry.getValue());
        }

        System.out.println("\n\nTotal Size: " + totalSize / 1024.0 / 1024.0 / 1024.0);
    }

    private static long getFileSize(URL url) {
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("HEAD");
            return conn.getContentLengthLong();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }
}
