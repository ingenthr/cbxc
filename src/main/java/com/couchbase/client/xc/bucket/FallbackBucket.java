package com.couchbase.client.xc.bucket;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.xc.XCBucket;
import com.couchbase.client.xc.cluster.ClusterWrapper;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

/**
 * When primary cluster is down completely falls back to a replica.
 *
 * For now it opens all buckets upfront and keeps a "primary" bucket reference that is used, if all nodes
 * are considered down fallback to replica. Right now no logic to figure out if the master is up but this
 * can be added.
 */
public class FallbackBucket implements XCBucket {

    private final AtomicReference<Bucket> currentBucket;
    private final AtomicReference<ClusterWrapper> currentCluster;

    private final List<Bucket> openBuckets;

    private final List<ClusterWrapper> couchbaseClusters;
    private final String name;
    private final String password;

    public FallbackBucket(final List<ClusterWrapper> couchbaseClusters, String name, String password) {
        this.couchbaseClusters = couchbaseClusters;
        this.name = name;
        this.password = password;

        openBuckets = new CopyOnWriteArrayList<Bucket>();
        for (ClusterWrapper cluster : couchbaseClusters) {
            openBuckets.add(cluster.openBucket(name, password));
        }

        // Set the first one (primary) as current upfront
        this.currentCluster = new AtomicReference<ClusterWrapper>(couchbaseClusters.get(0));
        this.currentBucket = new AtomicReference<Bucket>(openBuckets.get(0));

        currentCluster.get().addCallback(new ClusterWrapper.Callback() {
            @Override
            public void handle(ClusterWrapper.ClusterEvent event) {
                switch (event) {
                    case FULLY_DISCONNECTED:
                        System.err.println("FULLY DISCONNECTED - SWITCHING OVER");
                        currentCluster.set(couchbaseClusters.get(1));
                        currentBucket.set(openBuckets.get(1));
                        // TODO: be more intelligent about fallbacks of course
                }
            }
        });
    }

    public JsonDocument get(String id) {
        return currentBucket.get().get(id);
    }


}
