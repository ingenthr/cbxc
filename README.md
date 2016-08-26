Couchbase Cross-Cluster Support
===============================

This library is a work-in-progress of providing cross-cluster functionality
including transparent failover.


Questions
---------
 Q: open replica cluster connections upfront or only on demand?  
 A: Up-front so you can identify problems with that cluster connection sooner if needed.

TODOs
-----
 - Add full bucket api to XCBucket
 - Have separate connect from init (fully async & sync)
 - Add async and sync APIs like with the regular API
 - Do something about the continuous logging of errors on the downed
   cluster (since its noisy after failover)
 - Test that a network dropoff of a node is detected properly
 
```java
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
```
