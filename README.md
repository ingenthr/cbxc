Couchbase Cross-Cluster Support
===============================

This library is a work-in-progress of providing cross-cluster functionality
including transparent failover.


Questions
---------
 - open replica cluster connections upfront or only on demand?

Todos
-----
 - add full bucket api to XCBucket
 - have separate connect from init (fully async & sync)
 - add asnyc and sync APIs like with the regular API
 - do something about the continuous logging of errors on the downed
   cluster (since its noisy after failover)
 
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
