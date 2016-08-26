package examples;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.xc.XCBucket;
import com.couchbase.client.xc.XCClient;

import java.util.Arrays;

public class SimpleFallback {

    public static void main(String... args) throws Exception {
        XCClient client = new XCClient(Arrays.asList(
            // In a real program, these would come from properties.
            Arrays.asList("centos7lx-1"), // main cluster
            Arrays.asList("centos7lx-2")  // fallback cluster
        ));

        XCBucket bucket = client.openBucket("travel-sample", "");
        int itercount = 0;

        while(true) {
            try {
                JsonDocument airline_10 = bucket.get("airline_10");
                System.out.println(System.currentTimeMillis() + ": " + airline_10);
                if (itercount %2 == 0) {
                    airline_10.content().put("was here", "Matt");

                } else {
                    airline_10.content().put("was here", "Michael");
                }
                Thread.sleep(100);
                bucket.upsert(airline_10);
                itercount++;
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            Thread.sleep(1000);
        }
    }
}
