package com.couchbase.client.xc;

import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.xc.bucket.FallbackBucket;
import com.couchbase.client.xc.cluster.ClusterWrapper;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * The main interface into the cross cluster client.
 */
public class XCClient {

    private final List<ClusterWrapper> clusters;

    /**
     * Creates a new {@link XCClient}.
     *
     * @param clusterBootstraps list of cluster bootstrap nodes, in order of their priority.
     */
    public XCClient(List<List<String>> clusterBootstraps) {
        this.clusters = new CopyOnWriteArrayList<ClusterWrapper>();

        for (List<String> bootstrap : clusterBootstraps) {
            clusters.add(new ClusterWrapper(bootstrap));
        }
    }

    /**
     * Creates a new {@link XCClient} with an environment.
     *
     * @param clusterBootstraps list of cluster bootstrap nodes, in order of their priority.
     */
    public XCClient(CouchbaseEnvironment env, List<List<String>> clusterBootstraps) {
        this.clusters = new CopyOnWriteArrayList<ClusterWrapper>();

        for (List<String> bootstrap : clusterBootstraps) {
            clusters.add(new ClusterWrapper(env, bootstrap));
        }
    }

    /**
     * Open bucket - reuse!!
     *
     * @param name name of bucket
     * @param password password of bucket
     * @return bucket reference
     */
    public XCBucket openBucket(String name, String password) {
        return new FallbackBucket(clusters, name, password);
    }

}
