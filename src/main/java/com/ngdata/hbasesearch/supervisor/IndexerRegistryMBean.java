package com.ngdata.hbasesearch.supervisor;

import java.util.Set;

/**
 * MBean exposing the index names known by the IndexerRegistry instance in this server (JVM).
 */
public interface IndexerRegistryMBean {
    Set<String> getIndexNames();
}
