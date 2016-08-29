package com.couchbase.client.xc;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.RequestCancelledException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.error.CouchbaseOutOfMemoryException;
import com.couchbase.client.java.error.RequestTooBigException;
import com.couchbase.client.java.error.TemporaryFailureException;

import java.util.concurrent.TimeoutException;

/**
 * A level of indirection to a real {@link Bucket}, managed by this library.
 */
public interface XCBucket extends Bucket {

    /**
     * Insert or overwrite a {@link Document} with the default key/value timeout to an alternate, known bucket.
     *
     * This method is very similar to the upsert() method, but is intended for use from an exception handler.
     * While it will give write availability, the recovery of the document when the failure is restored is dependent
     * on how the underlying clusters are configured and the order of the writes.
     *
     * If the given {@link Document} (identified by its unique ID) already exists, it will be overridden by the current
     * one. The returned {@link Document} contains original properties, but has the refreshed CAS value set.
     *
     * Please note that this method will not use the {@link Document#cas()} for optimistic concurrency checks. If
     * this behavior is needed, the {@link #replace(Document)} method needs to be used.
     *
     * This operation will return successfully if the {@link Document} has been acknowledged in the managed cache layer
     * on the master server node. If increased data durability is a concern,
     * {@link #upsert(Document, PersistTo, ReplicateTo)} should be used instead.
     *
     * This method throws under the following conditions:
     *
     * - The operation takes longer than the specified timeout: {@link TimeoutException} wrapped in a {@link RuntimeException}
     * - The producer outpaces the SDK: {@link BackpressureException}
     * - The request content is too big: {@link RequestTooBigException}
     * - The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of
     *   retrying: {@link RequestCancelledException}
     * - The server is currently not able to process the request, retrying may help: {@link TemporaryFailureException}
     * - The server is out of memory: {@link CouchbaseOutOfMemoryException}
     * - Unexpected errors are caught and contained in a generic {@link CouchbaseException}.
     *
     * @param document the {@link Document} to upsert.
     * @return the new {@link Document}.
     */
    public <D extends Document<?>> D upsertRecover(D document);
}
