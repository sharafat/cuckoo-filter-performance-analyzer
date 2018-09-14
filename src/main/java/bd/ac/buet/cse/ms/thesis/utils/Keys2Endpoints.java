package bd.ac.buet.cse.ms.thesis.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedHashMap;
import java.util.Map;

public class Keys2Endpoints<T> {

    private String nodeToolPath;

    public Keys2Endpoints(String cassandraBinPath) {
        nodeToolPath = cassandraBinPath + (cassandraBinPath.endsWith("/") ? "" : "/") + "nodetool";
    }

    public Map<T, String> map(T[] keys, String keyspace, String table) {
        Map<T, String> map = new LinkedHashMap<T, String>(keys.length);

        for (T key : keys) {
            String[] commands = {nodeToolPath, "getendpoints", keyspace, table, key.toString()};

            try {
                Process process = Runtime.getRuntime().exec(commands);
                BufferedReader lineReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                String node = lineReader.lines().findFirst().get();

                map.put(key, node);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return map;
    }
}
