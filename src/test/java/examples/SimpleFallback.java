package examples;

import com.couchbase.client.xc.XCBucket;
import com.couchbase.client.xc.XCClient;

import java.util.Arrays;

public class SimpleFallback {

    public static void main(String... args) throws Exception {
        XCClient client = new XCClient(Arrays.asList(
            Arrays.asList("10.142.150.101"), // main cluster
            Arrays.asList("10.142.150.102")  // fallback cluster
        ));

        XCBucket bucket = client.openBucket("travel-sample", "");

        while(true) {
            try {
                System.out.println(System.currentTimeMillis() + ": " + bucket.get("airline_10"));
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            Thread.sleep(1000);
        }
    }
}
