package bd.ac.buet.cse.ms.thesis;

import bd.ac.buet.cse.ms.thesis.experiments.amazonreviews.Dataset;
import bd.ac.buet.cse.ms.thesis.utils.ConsoleWriter;
import bd.ac.buet.cse.ms.thesis.utils.Keys2Endpoints;
import bd.ac.buet.cse.ms.thesis.utils.OutputWriter;

import java.util.Map;

public class Key2EndpointMapper {

    private static final OutputWriter out = new ConsoleWriter();

    private static final String[] imaginaryKeys = new String[] {
            "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V",
            "W", "X", "Y", "Z",
            "AA", "BB", "CC", "DD", "EE", "FF", "GG", "HH", "II", "JJ", "KK", "LL", "MM", "NN", "OO", "PP", "QQ", "RR",
            "SS", "TT", "UU", "VV", "WW", "XX", "YY", "ZZ",
            "ABC", "DEF", "GHI", "JKL", "MNO", "PQR", "STU", "VWX", "YZ",
            "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"
    };

    public static void main(String[] args) {
        Keys2Endpoints<String> keys2Endpoints = new Keys2Endpoints<String>(Config.CASSANDRA_BIN_PATH);

        Map<String, String> map = keys2Endpoints.map(Dataset.KEYS, Config.KEYSPACE, Dataset.TABLE);
//        Map<String, String> map = keys2Endpoints.map(imaginaryKeys, Config.KEYSPACE, Dataset.TABLE);

        out.writeLine(map);
    }
}
