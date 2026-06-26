package com.codelry.util.cbdb3;

import org.testcontainers.containers.GenericContainer;

/**
 * Base for test classes that share one Couchbase container across the JVM.
 */
abstract class AbstractServerSharedTestcontainerTest extends AbstractServerTestcontainerTest {
  protected static final GenericContainer<?> couchbase = CouchbaseServerContainer.sharedContainer();
}
