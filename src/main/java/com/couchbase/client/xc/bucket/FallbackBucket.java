package com.couchbase.client.xc.bucket;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.java.*;
import com.couchbase.client.java.bucket.BucketManager;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.JsonLongDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.repository.Repository;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.result.SearchQueryResult;
import com.couchbase.client.java.subdoc.LookupInBuilder;
import com.couchbase.client.java.subdoc.MutateInBuilder;
import com.couchbase.client.java.view.SpatialViewQuery;
import com.couchbase.client.java.view.SpatialViewResult;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.xc.XCBucket;
import com.couchbase.client.xc.cluster.ClusterWrapper;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
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
    private final AtomicReference<Bucket> recoveryBucket;
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
        this.recoveryBucket = new AtomicReference<Bucket>(openBuckets.get(1));

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

    @Override
    public JsonDocument get(String id) {
        return currentBucket.get().get(id);
    }

    @Override
    public <D extends Document<?>> D get(String id, Class<D> target) {
        return currentBucket.get().get(id, target);
    }

    @Override
    public AsyncBucket async() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public ClusterFacade core() {
        throw new UnsupportedOperationException("Not available on the XCBucket");
    }

    @Override
    public CouchbaseEnvironment environment() {
        return currentBucket.get().environment();
    }

    @Override
    public String name() {
        return currentBucket.get().name();
    }

    @Override
    public JsonDocument get(String id, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().get(id, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D get(D document) {
        return currentBucket.get().get(document);
    }

    @Override
    public <D extends Document<?>> D get(D document, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().get(document, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D get(String id, Class<D> target, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().get(id, target, timeout, timeUnit);
    }

    @Override
    public boolean exists(String id) {
        return currentBucket.get().exists(id);
    }

    @Override
    public boolean exists(String id, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().exists(id, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> boolean exists(D document) {
        return currentBucket.get().exists(document);
    }

    @Override
    public <D extends Document<?>> boolean exists(D document, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().exists(document, timeout, timeUnit);
    }

    @Override
    public List<JsonDocument> getFromReplica(String id, ReplicaMode type) {
        return currentBucket.get().getFromReplica(id, type);
    }

    @Override
    public Iterator<JsonDocument> getFromReplica(String id) {
        return currentBucket.get().getFromReplica(id);
    }

    @Override
    public List<JsonDocument> getFromReplica(String id, ReplicaMode type, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().getFromReplica(id, type, timeout, timeUnit);
    }

    @Override
    public Iterator<JsonDocument> getFromReplica(String id, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().getFromReplica(id, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> List<D> getFromReplica(D document, ReplicaMode type) {
        return currentBucket.get().getFromReplica(document, type);
    }

    @Override
    public <D extends Document<?>> Iterator<D> getFromReplica(D document) {
        return currentBucket.get().getFromReplica(document);
    }

    @Override
    public <D extends Document<?>> List<D> getFromReplica(D document, ReplicaMode type, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().getFromReplica(document, type, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> Iterator<D> getFromReplica(D document, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().getFromReplica(document, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> List<D> getFromReplica(String id, ReplicaMode type, Class<D> target) {
        return currentBucket.get().getFromReplica(id, type, target);
    }

    @Override
    public <D extends Document<?>> Iterator<D> getFromReplica(String id, Class<D> target) {
        return currentBucket.get().getFromReplica(id, target);
    }

    @Override
    public <D extends Document<?>> List<D> getFromReplica(String id, ReplicaMode type, Class<D> target, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().getFromReplica(id, type, target, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> Iterator<D> getFromReplica(String id, Class<D> target, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().getFromReplica(id, target, timeout, timeUnit);
    }

    @Override
    public JsonDocument getAndLock(String id, int lockTime) {
        return currentBucket.get().getAndLock(id, lockTime);
    }

    @Override
    public JsonDocument getAndLock(String id, int lockTime, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().getAndLock(id, lockTime, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D getAndLock(D document, int lockTime) {
        return currentBucket.get().getAndLock(document, lockTime);
    }

    @Override
    public <D extends Document<?>> D getAndLock(D document, int lockTime, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().getAndLock(document, lockTime, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D getAndLock(String id, int lockTime, Class<D> target) {
        return currentBucket.get().getAndLock(id, lockTime, target);
    }

    @Override
    public <D extends Document<?>> D getAndLock(String id, int lockTime, Class<D> target, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().getAndLock(id, lockTime, target, timeout, timeUnit);
    }

    @Override
    public JsonDocument getAndTouch(String id, int expiry) {
        return currentBucket.get().getAndTouch(id, expiry);
    }

    @Override
    public JsonDocument getAndTouch(String id, int expiry, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().getAndTouch(id, expiry, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D getAndTouch(D document) {
        return currentBucket.get().getAndTouch(document);
    }

    @Override
    public <D extends Document<?>> D getAndTouch(D document, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().getAndTouch(document, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D getAndTouch(String id, int expiry, Class<D> target) {
        return currentBucket.get().getAndTouch(id, expiry, target);
    }

    @Override
    public <D extends Document<?>> D getAndTouch(String id, int expiry, Class<D> target, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().getAndTouch(id, expiry, target, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D insert(D document) {
        return currentBucket.get().insert(document);
    }

    @Override
    public <D extends Document<?>> D insert(D document, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().insert(document, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D insert(D document, PersistTo persistTo, ReplicateTo replicateTo) {
        return currentBucket.get().insert(document, persistTo, replicateTo);
    }

    @Override
    public <D extends Document<?>> D insert(D document, PersistTo persistTo, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().insert(document, persistTo, replicateTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D insert(D document, PersistTo persistTo) {
        return currentBucket.get().insert(document, persistTo);
    }

    @Override
    public <D extends Document<?>> D insert(D document, PersistTo persistTo, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().insert(document, persistTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D insert(D document, ReplicateTo replicateTo) {
        return currentBucket.get().insert(document, replicateTo);
    }

    @Override
    public <D extends Document<?>> D insert(D document, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().insert(document, replicateTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D upsert(D document) {
        return currentBucket.get().upsert(document);
    }

    @Override
    public <D extends Document<?>> D upsertRecover(D document) {
        return recoveryBucket.get().upsert(document);
    }

    @Override
    public <D extends Document<?>> D upsert(D document, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().upsert(document, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D upsert(D document, PersistTo persistTo, ReplicateTo replicateTo) {
        return currentBucket.get().upsert(document, persistTo, replicateTo);
    }

    @Override
    public <D extends Document<?>> D upsert(D document, PersistTo persistTo, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().upsert(document, persistTo, replicateTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D upsert(D document, PersistTo persistTo) {
        return currentBucket.get().upsert(document, persistTo);
    }

    @Override
    public <D extends Document<?>> D upsert(D document, PersistTo persistTo, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().upsert(document, persistTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D upsert(D document, ReplicateTo replicateTo) {
        return currentBucket.get().upsert(document, replicateTo);
    }

    @Override
    public <D extends Document<?>> D upsert(D document, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().upsert(document, replicateTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D replace(D document) {
        return currentBucket.get().replace(document);
    }

    @Override
    public <D extends Document<?>> D replace(D document, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().replace(document, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D replace(D document, PersistTo persistTo, ReplicateTo replicateTo) {
        return currentBucket.get().replace(document, persistTo, replicateTo);
    }

    @Override
    public <D extends Document<?>> D replace(D document, PersistTo persistTo, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().replace(document, persistTo, replicateTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D replace(D document, PersistTo persistTo) {
        return currentBucket.get().replace(document, persistTo);
    }

    @Override
    public <D extends Document<?>> D replace(D document, PersistTo persistTo, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().replace(document, persistTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D replace(D document, ReplicateTo replicateTo) {
        return currentBucket.get().replace(document, replicateTo);
    }

    @Override
    public <D extends Document<?>> D replace(D document, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().replace(document, replicateTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D remove(D document) {
        return currentBucket.get().remove(document);
    }

    @Override
    public <D extends Document<?>> D remove(D document, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().remove(document, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D remove(D document, PersistTo persistTo, ReplicateTo replicateTo) {
        return currentBucket.get().remove(document, persistTo, replicateTo);
    }

    @Override
    public <D extends Document<?>> D remove(D document, PersistTo persistTo, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().remove(document, persistTo, replicateTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D remove(D document, PersistTo persistTo) {
        return currentBucket.get().remove(document, persistTo);
    }

    @Override
    public <D extends Document<?>> D remove(D document, PersistTo persistTo, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().remove(document, persistTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D remove(D document, ReplicateTo replicateTo) {
        return currentBucket.get().remove(document, replicateTo);
    }

    @Override
    public <D extends Document<?>> D remove(D document, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().remove(document, replicateTo, timeout, timeUnit);
    }

    @Override
    public JsonDocument remove(String id) {
        return currentBucket.get().remove(id);
    }

    @Override
    public JsonDocument remove(String id, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().remove(id, timeout, timeUnit);
    }

    @Override
    public JsonDocument remove(String id, PersistTo persistTo, ReplicateTo replicateTo) {
        return currentBucket.get().remove(id, persistTo, replicateTo);
    }

    @Override
    public JsonDocument remove(String id, PersistTo persistTo, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().remove(id, persistTo, replicateTo, timeout, timeUnit);
    }

    @Override
    public JsonDocument remove(String id, PersistTo persistTo) {
        return currentBucket.get().remove(id, persistTo);
    }

    @Override
    public JsonDocument remove(String id, PersistTo persistTo, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().remove(id, persistTo, timeout, timeUnit);
    }

    @Override
    public JsonDocument remove(String id, ReplicateTo replicateTo) {
        return currentBucket.get().remove(id, replicateTo);
    }

    @Override
    public JsonDocument remove(String id, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().remove(id, replicateTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D remove(String id, Class<D> target) {
        return currentBucket.get().remove(id, target);
    }

    @Override
    public <D extends Document<?>> D remove(String id, Class<D> target, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().remove(id, target, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D remove(String id, PersistTo persistTo, ReplicateTo replicateTo, Class<D> target) {
        return currentBucket.get().remove(id, persistTo, replicateTo, target);
    }

    @Override
    public <D extends Document<?>> D remove(String id, PersistTo persistTo, ReplicateTo replicateTo, Class<D> target, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().remove(id, persistTo, replicateTo, target, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D remove(String id, PersistTo persistTo, Class<D> target) {
        return currentBucket.get().remove(id, persistTo, target);
    }

    @Override
    public <D extends Document<?>> D remove(String id, PersistTo persistTo, Class<D> target, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().remove(id, persistTo, target, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D remove(String id, ReplicateTo replicateTo, Class<D> target) {
        return currentBucket.get().remove(id, replicateTo, target);
    }

    @Override
    public <D extends Document<?>> D remove(String id, ReplicateTo replicateTo, Class<D> target, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().remove(id, replicateTo, target, timeout, timeUnit);
    }

    @Override
    public ViewResult query(ViewQuery query) {
        return currentBucket.get().query(query);
    }

    @Override
    public SpatialViewResult query(SpatialViewQuery query) {
        return currentBucket.get().query(query);
    }

    @Override
    public ViewResult query(ViewQuery query, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().query(query, timeout, timeUnit);
    }

    @Override
    public SpatialViewResult query(SpatialViewQuery query, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().query(query, timeout, timeUnit);
    }

    @Override
    public N1qlQueryResult query(Statement statement) {
        return currentBucket.get().query(statement);
    }

    @Override
    public N1qlQueryResult query(Statement statement, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().query(statement, timeout, timeUnit);
    }

    @Override
    public N1qlQueryResult query(N1qlQuery query) {
        return currentBucket.get().query(query);
    }

    @Override
    public N1qlQueryResult query(N1qlQuery query, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().query(query, timeout, timeUnit);
    }

    @Override
    public SearchQueryResult query(SearchQuery query) {
        return currentBucket.get().query(query);
    }

    @Override
    public SearchQueryResult query(SearchQuery query, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().query(query, timeout, timeUnit);
    }

    @Override
    public Boolean unlock(String id, long cas) {
        return currentBucket.get().unlock(id, cas);
    }

    @Override
    public Boolean unlock(String id, long cas, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().unlock(id, cas, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> Boolean unlock(D document) {
        return currentBucket.get().unlock(document);
    }

    @Override
    public <D extends Document<?>> Boolean unlock(D document, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().unlock(document, timeout, timeUnit);
    }

    @Override
    public Boolean touch(String id, int expiry) {
        return currentBucket.get().touch(id, expiry);
    }

    @Override
    public Boolean touch(String id, int expiry, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().touch(id, expiry, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> Boolean touch(D document) {
        return currentBucket.get().touch(document);
    }

    @Override
    public <D extends Document<?>> Boolean touch(D document, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().touch(document, timeout, timeUnit);
    }

    @Override
    public JsonLongDocument counter(String id, long delta) {
        return currentBucket.get().counter(id, delta);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, PersistTo persistTo) {
        return currentBucket.get().counter(id, delta, persistTo);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, ReplicateTo replicateTo) {
        return currentBucket.get().counter(id, delta, replicateTo);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, PersistTo persistTo, ReplicateTo replicateTo) {
        return currentBucket.get().counter(id, delta, persistTo, replicateTo);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().counter(id, delta, timeout, timeUnit);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, PersistTo persistTo, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().counter(id, delta, persistTo, timeout, timeUnit);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().counter(id, delta, replicateTo, timeout, timeUnit);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, PersistTo persistTo, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().counter(id, delta, persistTo, replicateTo, timeout, timeUnit);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long initial) {
        return currentBucket.get().counter(id, delta, initial);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long initial, PersistTo persistTo) {
        return currentBucket.get().counter(id, delta, initial, persistTo);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long initial, ReplicateTo replicateTo) {
        return currentBucket.get().counter(id, delta, initial, replicateTo);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long initial, PersistTo persistTo, ReplicateTo replicateTo) {
        return currentBucket.get().counter(id, delta, initial, persistTo, replicateTo);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long initial, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().counter(id, delta, initial, timeout, timeUnit);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long initial, PersistTo persistTo, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().counter(id, delta, initial, persistTo, timeout, timeUnit);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long initial, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().counter(id, delta, initial, replicateTo, timeout, timeUnit);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long initial, PersistTo persistTo, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().counter(id, delta, initial, persistTo, replicateTo, timeout, timeUnit);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long initial, int expiry) {
        return currentBucket.get().counter(id, delta, initial, expiry);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long initial, int expiry, PersistTo persistTo) {
        return currentBucket.get().counter(id, delta, initial, expiry, persistTo);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long initial, int expiry, ReplicateTo replicateTo) {
        return currentBucket.get().counter(id, delta, initial, expiry, replicateTo);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long initial, int expiry, PersistTo persistTo, ReplicateTo replicateTo) {
        return currentBucket.get().counter(id, delta, initial, expiry, persistTo, replicateTo);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long initial, int expiry, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().counter(id, delta, initial, expiry, timeout, timeUnit);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long initial, int expiry, PersistTo persistTo, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().counter(id, delta, initial, expiry, persistTo, timeout, timeUnit);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long initial, int expiry, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().counter(id, delta, initial, expiry, replicateTo, timeout, timeUnit);
    }

    @Override
    public JsonLongDocument counter(String id, long delta, long initial, int expiry, PersistTo persistTo, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().counter(id, delta, initial, expiry, persistTo, replicateTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D append(D document) {
        return currentBucket.get().append(document);
    }

    @Override
    public <D extends Document<?>> D append(D document, PersistTo persistTo) {
        return currentBucket.get().append(document, persistTo);
    }

    @Override
    public <D extends Document<?>> D append(D document, ReplicateTo replicateTo) {
        return currentBucket.get().append(document, replicateTo);
    }

    @Override
    public <D extends Document<?>> D append(D document, PersistTo persistTo, ReplicateTo replicateTo) {
        return currentBucket.get().append(document, persistTo, replicateTo);
    }

    @Override
    public <D extends Document<?>> D append(D document, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().append(document, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D append(D document, PersistTo persistTo, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().append(document, persistTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D append(D document, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().append(document, replicateTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D append(D document, PersistTo persistTo, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().append(document, persistTo, replicateTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D prepend(D document) {
        return currentBucket.get().prepend(document);
    }

    @Override
    public <D extends Document<?>> D prepend(D document, PersistTo persistTo) {
        return currentBucket.get().prepend(document, persistTo);
    }

    @Override
    public <D extends Document<?>> D prepend(D document, ReplicateTo replicateTo) {
        return currentBucket.get().prepend(document, replicateTo);
    }

    @Override
    public <D extends Document<?>> D prepend(D document, PersistTo persistTo, ReplicateTo replicateTo) {
        return currentBucket.get().prepend(document, persistTo, replicateTo);
    }

    @Override
    public <D extends Document<?>> D prepend(D document, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().prepend(document, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D prepend(D document, PersistTo persistTo, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().prepend(document, persistTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D prepend(D document, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().prepend(document, replicateTo, timeout, timeUnit);
    }

    @Override
    public <D extends Document<?>> D prepend(D document, PersistTo persistTo, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return currentBucket.get().prepend(document, persistTo, replicateTo, timeout, timeUnit);
    }

    @Override
    public LookupInBuilder lookupIn(String docId) {
        return currentBucket.get().lookupIn(docId);
    }

    @Override
    public MutateInBuilder mutateIn(String docId) {
        return currentBucket.get().mutateIn(docId);
    }

    @Override
    public int invalidateQueryCache() {
        return currentBucket.get().invalidateQueryCache();
    }

    @Override
    public BucketManager bucketManager() {
        return currentBucket.get().bucketManager();
    }

    @Override
    public Repository repository() {
        return currentBucket.get().repository();
    }

    @Override
    public Boolean close() {
       throw new UnsupportedOperationException();
    }

    @Override
    public Boolean close(long timeout, TimeUnit timeUnit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isClosed() {
        return currentBucket.get().isClosed();
    }
}
