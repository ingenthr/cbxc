package com.couchbase.client.xc;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;

/**
 * A level of indirection to a real {@link Bucket}, managed by this library.
 */
public interface XCBucket /* todo: make me implement the Bucket IF ? */ {

    JsonDocument get(String id);
}
