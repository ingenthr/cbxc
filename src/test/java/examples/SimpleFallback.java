package examples;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.xc.XCBucket;
import com.couchbase.client.xc.XCClient;

import java.util.Arrays;

public class SimpleFallback {

    public static void main(String... args) throws Exception {
//        CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder().mutationTokensEnabled(true).build();
        CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder().connectTimeout(30000).build();
        XCClient client = new XCClient(env, Arrays.asList(
            // In a real program, these would come from properties.
            Arrays.asList("centos7lx-1"), // main cluster
            Arrays.asList("centos7lx-2")  // fallback cluster
        ));

        XCBucket bucket = client.openBucket("travel-sample", "");
        int itercount = 0;
        boolean getSuccess = false, setSuccess = false;
        JsonDocument airline_10 = null;

        // do an initial get, since later we're going to get and set the doc
        try {
            airline_10 = bucket.get("airline_10");
        } catch (Exception ex) {
            System.err.println("Failed on first retrieval.");
            System.exit(-1);
        }

        while(true) {
            long now = System.currentTimeMillis();
            getSuccess = false; setSuccess= false;
            try {
                airline_10.content().put("was here at", now);
                bucket.upsert(airline_10);
                setSuccess = true;
                airline_10 = bucket.get("airline_10");
                getSuccess = true;
                System.out.println(now + ": " + airline_10);
                Thread.sleep(100);
                itercount++;
            } catch (Exception ex) {
                ex.printStackTrace();
            } 
            Thread.sleep(1000);
            System.out.println("LOOOOOOOOOOOOOOOOOOooooooping.");
        }
    }
}
