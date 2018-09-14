package bd.ac.buet.cse.ms.thesis;

import bd.ac.buet.cse.ms.thesis.experiments.amazonreviews.Dataset;
import bd.ac.buet.cse.ms.thesis.utils.ConsoleWriter;
import bd.ac.buet.cse.ms.thesis.utils.Keys2Endpoints;
import bd.ac.buet.cse.ms.thesis.utils.OutputWriter;

import java.util.Map;

public class Key2EndpointMapper {

    private static final OutputWriter out = new ConsoleWriter();

    public static void main(String[] args) {
        Keys2Endpoints<String> keys2Endpoints = new Keys2Endpoints<String>(Config.CASSANDRA_BIN_PATH);

        Map<String, String> map = keys2Endpoints.map(Dataset.keys, Config.KEYSPACE, Dataset.table);

        out.writeLine(map);
    }
}
