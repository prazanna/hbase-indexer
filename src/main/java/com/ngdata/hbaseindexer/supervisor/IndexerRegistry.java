package com.ngdata.hbaseindexer.supervisor;

import com.ngdata.hbaseindexer.Indexer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class IndexerRegistry  implements IndexerRegistryMBean {
    private final Map<String, Indexer> indexers = new ConcurrentHashMap<String, Indexer>();
    private final Log log = LogFactory.getLog(getClass());
    private ObjectName jmxObjectName;

    public void register(String indexName, Indexer indexer) {
        indexers.put(indexName, indexer);
    }

    public void unregister(String indexName) {
        indexers.remove(indexName);
    }

    /**
     * Get the index with the corresponding name.
     *
     * @param indexName name of the index
     * @return index or <code>null</code> if no index with the given name exists
     */
    public Indexer getIndexer(String indexName) {
        return indexers.get(indexName);
    }

    public Collection<Indexer> getAllIndexers() {
        return indexers.values();
    }

    @Override
    public Set<String> getIndexNames() {
        return new HashSet<String>(indexers.keySet());
    }

    @PostConstruct
    public void start() {
        registerMBean();
    }

    @PreDestroy
    public void stop() {
        unregisterMBean();
    }

    private void registerMBean() {
        try {
            jmxObjectName = new ObjectName("Lily:name=Indexer");
            ManagementFactory.getPlatformMBeanServer().registerMBean(this, jmxObjectName);
        } catch (Exception e) {
            log.warn("Error registering mbean '"+ jmxObjectName, e);
        }
    }

    private void unregisterMBean() {
        try {
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(jmxObjectName);
        } catch (Exception e) {
            log.warn("Error unregistering mbean '"+ jmxObjectName, e);
        }
    }
}

