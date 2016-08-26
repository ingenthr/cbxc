package com.couchbase.client.xc.cluster;

import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.core.event.system.NodeConnectedEvent;
import com.couchbase.client.core.event.system.NodeDisconnectedEvent;
import com.couchbase.client.core.message.internal.GetConfigProviderRequest;
import com.couchbase.client.core.message.internal.GetConfigProviderResponse;
import com.couchbase.client.deps.io.netty.util.internal.ConcurrentSet;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.dsl.functions.Collections;
import rx.functions.Action1;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Track and respond to cluster events.
 *
 * Conceptually, this sets up events on a given cluster and its
 * underlying nodes, then using the event bus to identify problems
 * from the local perspective. If for instance all nodes are down,
 * notify other layers.
 *
 * Currently, notifications aren't here for fail-back, but enough
 * of a model is here that demonstrates how that may be done.
 *
 */
public class ClusterWrapper {

    private final CouchbaseCluster cluster;

    private final AtomicReference<Set<InetAddress>> clusterHosts;
    private final Set<InetAddress> connectedHosts;
    private final ConfigurationProvider configProvider;
    private final List<Callback> callbacks;

    public ClusterWrapper(List<String> bootstrap) {
        callbacks = new CopyOnWriteArrayList<Callback>();
        clusterHosts = new AtomicReference<Set<InetAddress>>(new HashSet<InetAddress>());
        connectedHosts = new ConcurrentSet<InetAddress>();

        CouchbaseEnvironment env = DefaultCouchbaseEnvironment.create();

        env.eventBus()
            .get()
            .forEach(new Action1<CouchbaseEvent>() {
                public void call(CouchbaseEvent ev) {
                    if (ev instanceof NodeConnectedEvent) {
                        connectedHosts.add(((NodeConnectedEvent) ev).host());
                    } else if (ev instanceof NodeDisconnectedEvent) {
                        boolean prevEmpty = connectedHosts.isEmpty();
                        connectedHosts.remove(((NodeDisconnectedEvent) ev).host());
                        if (!prevEmpty && connectedHosts.isEmpty()) {
                            for (Callback cb : callbacks) {
                                cb.handle(ClusterEvent.FULLY_DISCONNECTED);
                            }
                        }
                    }
                }
            });

        cluster = CouchbaseCluster.create(env, bootstrap);

        configProvider = cluster
            .core()
            .<GetConfigProviderResponse>send(new GetConfigProviderRequest())
            .toBlocking()
            .single()
            .provider();

        configProvider
            .configs()
            .subscribe(new Action1<ClusterConfig>() {
            @Override
            public void call(ClusterConfig config) {
                Set<InetAddress> clusterNodes = new HashSet<InetAddress>();
                for (BucketConfig bc : config.bucketConfigs().values()) {
                    for (NodeInfo ni : bc.nodes()) {
                        clusterNodes.add(ni.hostname());
                    }
                }
                clusterHosts.set(clusterNodes);
            }
        });
    }

    public void addCallback(Callback cb) {
        callbacks.add(cb);
    }

    public Bucket openBucket(String name, String password) {
        return cluster.openBucket(name, password);
    }


    public enum ClusterEvent {
        /* all nodes are disconnected */
        FULLY_DISCONNECTED
    }

    public interface Callback {
        void handle(ClusterEvent event);
    }
}
