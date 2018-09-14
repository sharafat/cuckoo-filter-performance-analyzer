package bd.ac.buet.cse.ms.thesis;

public interface Config {

    String SERVER_ADDRESS = "18.220.232.203";

    int SERVER_PORT = 9042;

    String KEYSPACE = "cuckoo_test";

    int CONNECT_TIMEOUT_MILLIS = 9999999;

    int READ_TIMEOUT_MILLIS = 9999999;

    boolean DEBUG_LOG_QUERY_EXECUTION = true;

    String CASSANDRA_BIN_PATH = "/home/ubuntu/cassandra/bin";

}
