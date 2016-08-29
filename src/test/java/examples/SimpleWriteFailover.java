package examples;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.xc.XCBucket;
import com.couchbase.client.xc.XCClient;

import java.util.Arrays;

public class SimpleWriteFailover {

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
                airline_10 = bucket.upsert(airline_10);
                setSuccess = true;
                System.out.println(now + ": " + airline_10);
                itercount++;
            } catch (RuntimeException ex) {
                // Arrive here on failed upserts
                // Note that the programming model of the client is to try to complete an operation until timeout, so
                // it will take longer for the operation.  The FallbackBucket encapsulates a place where a declared
                // failure, either by the event bus or heuristics, can allow for failover to maintain
                // throughput and latency in a failure case.
                System.err.println("Writing to alternate cluster for later recovery.");
                airline_10 = bucket.upsertRecover(airline_10);
                System.out.println(now + ": " + airline_10);

            } catch (Exception ex) {
                ex.printStackTrace();
            } 
            Thread.sleep(1000);
            System.out.println("LOOOOOOOOOOOOOOOOOOooooooping.");
        }
    }
}
