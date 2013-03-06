/*
 * Copyright 2013 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ngdata.hbasesearch.model.impl;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PreDestroy;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import com.ngdata.hbasesearch.model.api.IndexUpdateState;
import com.ngdata.sep.util.zookeeper.ZkUtil;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import com.ngdata.sep.util.zookeeper.ZooKeeperOperation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import com.ngdata.hbasesearch.model.api.ActiveBatchBuildInfo;
import com.ngdata.hbasesearch.model.api.BatchBuildInfo;
import com.ngdata.hbasesearch.model.api.IndexBatchBuildState;
import com.ngdata.hbasesearch.model.api.IndexConcurrentModificationException;
import com.ngdata.hbasesearch.model.api.IndexDefinition;
import com.ngdata.hbasesearch.model.api.IndexExistsException;
import com.ngdata.hbasesearch.model.api.IndexGeneralState;
import com.ngdata.hbasesearch.model.api.IndexModelException;
import com.ngdata.hbasesearch.model.api.IndexNotFoundException;
import com.ngdata.hbasesearch.model.api.IndexUpdateException;
import com.ngdata.hbasesearch.model.api.IndexValidityException;
import com.ngdata.hbasesearch.model.api.IndexerModelEvent;
import com.ngdata.hbasesearch.model.api.IndexerModelListener;
import com.ngdata.hbasesearch.model.api.WriteableIndexerModel;
import com.ngdata.hbasesearch.util.zookeeper.ZkLock;
import com.ngdata.hbasesearch.util.zookeeper.ZkLockException;

import static com.ngdata.hbasesearch.model.api.IndexerModelEventType.*;
import static org.apache.zookeeper.Watcher.Event.EventType.NodeChildrenChanged;
import static org.apache.zookeeper.Watcher.Event.EventType.NodeDataChanged;

// About how the indexer conf is stored in ZooKeeper
// -------------------------------------------------
// I had to make the decision of whether to store all properties of an index in the
// data of one node, or rather to add these properties as subnodes.
//
// The advantages of putting index properties in subnodes are:
//  - they allow to watch inidividual properties, so you know which one changed
//  - each property can be updated individually
//  - there is no impact of big properties, like the indexerconf XML, on other
//    ones
//
// The advantages of putting all index properties in the data of one znode are:
//  - the atomic create or update of an index is easy/possible. ZK does not have
//    transactions. Intermediate state while performing updates is not visible.
//  - watching is simpler: just watch one node, rather than having to register
//    watches on every child node and on the parent for children changes.
//  - in practice it is usually still easy to know what individual properties
//    changed, by comparing with previous state you hold yourself.
//
// So the clear winner was to put all properties in the data of one znode.
// It is much easier: less work, reduced complexity, less chance for errors.
// Also, the state of indexes does not change frequently, so that the data
// of this znode is somewhat bigger is not really important.


/**
 * Implementation of IndexerModel.
 *
 * <p>Usage: typically in an application you will create just one instance of IndexerModel and share it.
 * This is a relatively heavy object which runs threads. IMPORTANT: when done using it, call the stop()
 * method to properly shut down everything.</p>
 *
 */
public class IndexerModelImpl implements WriteableIndexerModel {
    private final ZooKeeperItf zk;

    /**
     * Cache of the indexes as they are stored in ZK. Updated based on ZK watcher events. People who update
     * this cache should synchronize on {@link #indexes_lock}.
     */
    private final Map<String, IndexDefinition> indexes = new ConcurrentHashMap<String, IndexDefinition>(16, 0.75f, 1);

    private final Object indexes_lock = new Object();

    private final Set<IndexerModelListener> listeners = Collections.newSetFromMap(new IdentityHashMap<IndexerModelListener, Boolean>());

    private final Watcher watcher = new IndexModelChangeWatcher();

    private final Watcher connectStateWatcher = new ConnectStateWatcher();

    private final IndexCacheRefresher indexCacheRefresher = new IndexCacheRefresher();

    private boolean stopped = false;

    private final Log log = LogFactory.getLog(getClass());

    private static final String INDEX_COLLECTION_PATH = "/lily/indexer/index";

    private static final String INDEX_TRASH_PATH = "/lily/indexer/index-trash";

    private static final String INDEX_COLLECTION_PATH_SLASH = INDEX_COLLECTION_PATH + "/";

    public IndexerModelImpl(ZooKeeperItf zk) throws InterruptedException, KeeperException {
        this.zk = zk;
        ZkUtil.createPath(zk, INDEX_COLLECTION_PATH);
        ZkUtil.createPath(zk, INDEX_TRASH_PATH);

        zk.addDefaultWatcher(connectStateWatcher);

        indexCacheRefresher.start();
        indexCacheRefresher.waitUntilStarted();
    }

    @PreDestroy
    public void stop() throws InterruptedException {
        stopped = true;
        zk.removeDefaultWatcher(connectStateWatcher);
        indexCacheRefresher.shutdown();
    }

    @Override
    public IndexDefinition newIndex(String name) {
        return new IndexDefinitionImpl(name);
    }

    @Override
    public void addIndex(IndexDefinition index) throws IndexExistsException, IndexModelException, IndexValidityException {
        assertValid(index);

        if (index.getUpdateState() != IndexUpdateState.DO_NOT_SUBSCRIBE) {
            index.setSubscriptionTimestamp(System.currentTimeMillis());
        }
        final String indexPath = INDEX_COLLECTION_PATH + "/" + index.getName();
        final byte[] data = IndexDefinitionConverter.INSTANCE.toJsonBytes(index);

        try {
            zk.retryOperation(new ZooKeeperOperation<String>() {
                @Override
                public String execute() throws KeeperException, InterruptedException {
                    return zk.create(indexPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            });
        } catch (KeeperException.NodeExistsException e) {
            throw new IndexExistsException(index.getName());
        } catch (Exception e) {
            throw new IndexModelException("Error creating index.", e);
        }
    }

    private void assertValid(IndexDefinition index) throws IndexValidityException {
        if (index.getName() == null || index.getName().length() == 0)
            throw new IndexValidityException("Name should not be null or zero-length");

        if (index.getConfiguration() == null)
            throw new IndexValidityException("Configuration should not be null.");

        if (index.getGeneralState() == null)
            throw new IndexValidityException("General state should not be null.");

        if (index.getBatchBuildState() == null)
            throw new IndexValidityException("Build state should not be null.");

        if (index.getUpdateState() == null)
            throw new IndexValidityException("Update state should not be null.");

        if (index.getActiveBatchBuildInfo() != null) {
            ActiveBatchBuildInfo info = index.getActiveBatchBuildInfo();
            if (info.getJobId() == null)
                throw new IndexValidityException("Job id of active batch build cannot be null.");
        }


        // TODO FIXME disabled this code (after copying from Lily)
//       boolean hasShards = index.getSolrShards() != null && !index.getSolrShards().isEmpty();
//       boolean hasCollection = index.getSolrCollection() != null;
//       boolean hasZkConnectionString = index.getZkConnectionString() != null && !index.getZkConnectionString().isEmpty();
//
//       if (hasCollection && hasShards) {
//           throw new IndexValidityException("Ambiguous solr configuration in index defintion. Setting a solr " +
//                   "collection together with solr shards has no use. Set either the solr shards or collection.");
//       }
//
//       if (hasShards && hasZkConnectionString) {
//           throw new IndexValidityException("Ambiguous solr configuration in index defintion. Setting a solr " +
//                   "zookeeper connection together with solr shards has no use. Set either the solr shards or " +
//                   "zookeeper connection.");
//       }
//       if (!hasShards && !hasZkConnectionString) {
//           throw new IndexValidityException("Incomplete solr configuration in index defintion. You need at least " +
//                   "a shard or a zookeeper connection string.");
//       }

        if (index.getLastBatchBuildInfo() != null) {
            BatchBuildInfo info = index.getLastBatchBuildInfo();
            if (info.getJobId() == null)
                throw new IndexValidityException("Job id of last batch build cannot be null.");
            if (info.getJobState() == null)
                throw new IndexValidityException("Job state of last batch build cannot be null.");
        }

        for (String shard : index.getSolrShards().values()) {
            try {
                URI uri = new URI(shard);
                if (!uri.isAbsolute()) {
                    throw new IndexValidityException("Solr shard URI is not absolute: " + shard);
                }
            } catch (URISyntaxException e) {
                throw new IndexValidityException("Invalid Solr shard URI: " + shard);
            }
        }

        // TODO FIXME disabled this code (after copying from Lily)
//        if (index.getShardingConfiguration() != null) {
//            // parse it + check used shards -> requires dependency on the engine or moving the relevant classes
//            // to the model
//            ShardSelector selector;
//            try {
//                selector = JsonShardSelectorBuilder.build(index.getShardingConfiguration());
//            } catch (ShardingConfigException e) {
//                throw new IndexValidityException("Error with sharding configuration.", e);
//            }
//
//            Set<String> shardNames = index.getSolrShards().keySet();
//
//            for (String shard : selector.getShards()) {
//                if (!shardNames.contains(shard)) {
//                    throw new IndexValidityException("The sharding configuration refers to a shard that is not" +
//                    " in the set of available shards. Shard: " + shard);
//                }
//            }
//        }
//
//        try {
//            IndexerConfBuilder.validate(new ByteArrayInputStream(index.getConfiguration()));
//        } catch (IndexerConfException e) {
//            throw new IndexValidityException("The indexer configuration is not XML well-formed or valid.", e);
//        }
//
//        if (index.getBatchIndexConfiguration() != null && index.getBatchBuildState() !=
//                IndexBatchBuildState.BUILD_REQUESTED) {
//            throw new IndexValidityException("The build state must be set to BUILD_REQUESTED when setting a batchIndexConfiguration");
//        }
    }

    @Override
    public void updateIndexInternal(final IndexDefinition index) throws InterruptedException, KeeperException,
            IndexNotFoundException, IndexConcurrentModificationException, IndexValidityException {

        assertValid(index);

        final byte[] newData = IndexDefinitionConverter.INSTANCE.toJsonBytes(index);

        try {
            zk.retryOperation(new ZooKeeperOperation<Stat>() {
                @Override
                public Stat execute() throws KeeperException, InterruptedException {
                    return zk.setData(INDEX_COLLECTION_PATH_SLASH + index.getName(), newData, index.getZkDataVersion());
                }
            });
        } catch (KeeperException.NoNodeException e) {
            throw new IndexNotFoundException(index.getName());
        } catch (KeeperException.BadVersionException e) {
            throw new IndexConcurrentModificationException(index.getName());
        }
    }

    @Override
    public void updateIndex(final IndexDefinition index, String lock) throws InterruptedException, KeeperException,
            IndexNotFoundException, IndexConcurrentModificationException, ZkLockException, IndexUpdateException,
            IndexValidityException {

        if (!ZkLock.ownsLock(zk, lock)) {
            throw new IndexUpdateException("You are not owner of the indexes lock, your lock path is: " + lock);
        }

        assertValid(index);

        IndexDefinition currentIndex = getMutableIndex(index.getName());

        if (currentIndex.getGeneralState() == IndexGeneralState.DELETE_REQUESTED ||
                currentIndex.getGeneralState() == IndexGeneralState.DELETING) {
            throw new IndexUpdateException("An index in state " + index.getGeneralState() + " cannot be modified.");
        }

        if (index.getBatchBuildState() == IndexBatchBuildState.BUILD_REQUESTED &&
                currentIndex.getBatchBuildState() != IndexBatchBuildState.INACTIVE &&
                currentIndex.getBatchBuildState() != IndexBatchBuildState.BUILD_REQUESTED) {
            throw new IndexUpdateException("Cannot move index build state from " + currentIndex.getBatchBuildState() +
                    " to " + index.getBatchBuildState());
        }

        if (currentIndex.getGeneralState() == IndexGeneralState.DELETE_REQUESTED) {
            throw new IndexUpdateException("An index in the state " + IndexGeneralState.DELETE_REQUESTED +
                    " cannot be updated.");
        }

        if (!Objects.equal(currentIndex.getActiveBatchBuildInfo(), index.getActiveBatchBuildInfo())) {
            throw new IndexUpdateException("The active batch build info cannot be modified by users.");
        }

        if (!Objects.equal(currentIndex.getLastBatchBuildInfo(), index.getLastBatchBuildInfo())) {
            throw new IndexUpdateException("The last batch build info cannot be modified by users.");
        }

        updateIndexInternal(index);

    }

    @Override
    public void deleteIndex(final String indexName) throws IndexModelException {
        final String indexPath = INDEX_COLLECTION_PATH_SLASH + indexName;
        final String indexLockPath = indexPath + "/lock";

        try {
            // Make a copy of the index data in the index trash
            zk.retryOperation(new ZooKeeperOperation<Object>() {
                @Override
                public Object execute() throws KeeperException, InterruptedException {
                    byte[] data = zk.getData(indexPath, false, null);

                    String trashPath = INDEX_TRASH_PATH + "/" + indexName;

                    // An index with the same name might have existed before and hence already exist
                    // in the index trash, handle this by appending a sequence number until a unique
                    // name is found.
                    String baseTrashpath = trashPath;
                    int count = 0;
                    while (zk.exists(trashPath, false) != null) {
                        count++;
                        trashPath = baseTrashpath + "." + count;
                    }

                    zk.create(trashPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

                    return null;
                }
            });

            // The loop below is normally not necessary, since we disallow taking new locks on indexes
            // which are being deleted.
            int tryCount = 0;
            while (true) {
                boolean success = zk.retryOperation(new ZooKeeperOperation<Boolean>() {
                    @Override
                    public Boolean execute() throws KeeperException, InterruptedException {
                        try {
                            // Delete the index lock if it exists
                            if (zk.exists(indexLockPath, false) != null) {
                                List<String> children = Collections.emptyList();
                                try {
                                    children = zk.getChildren(indexLockPath, false);
                                } catch (KeeperException.NoNodeException e) {
                                    // ok
                                }

                                for (String child : children) {
                                    try {
                                        zk.delete(indexLockPath + "/" + child, -1);
                                    } catch (KeeperException.NoNodeException e) {
                                        // ignore, node was already removed
                                    }
                                }

                                try {
                                    zk.delete(indexLockPath, -1);
                                } catch (KeeperException.NoNodeException e) {
                                    // ignore
                                }
                            }


                            zk.delete(indexPath, -1);

                            return true;
                        } catch (KeeperException.NotEmptyException e) {
                            // Someone again took a lock on the index, retry
                        }
                        return false;
                    }
                });

                if (success)
                    break;

                tryCount++;
                if (tryCount > 10) {
                    throw new IndexModelException("Failed to delete index because it still has child data. Index: " + indexName);
                }
            }
        } catch (Throwable t) {
            if (t instanceof InterruptedException)
                Thread.currentThread().interrupt();
            throw new IndexModelException("Failed to delete index " + indexName, t);
        }
    }

    @Override
    public String lockIndexInternal(String indexName, boolean checkDeleted) throws ZkLockException, IndexNotFoundException, InterruptedException,
            KeeperException, IndexModelException {

        IndexDefinition index = getIndex(indexName);

        if (checkDeleted) {
            if (index.getGeneralState() == IndexGeneralState.DELETE_REQUESTED ||
                    index.getGeneralState() == IndexGeneralState.DELETING) {
                throw new IndexModelException("An index in state " + index.getGeneralState() + " cannot be locked.");
            }
        }

        final String lockPath = INDEX_COLLECTION_PATH_SLASH + indexName + "/lock";

        //
        // Create the lock path if necessary
        //
        Stat stat = zk.retryOperation(new ZooKeeperOperation<Stat>() {
            @Override
            public Stat execute() throws KeeperException, InterruptedException {
                return zk.exists(lockPath, null);
            }
        });

        if (stat == null) {
            // We do not make use of ZkUtil.createPath (= recursive path creation) on purpose,
            // because if the parent path does not exist, this means the index does not exist,
            // and we do not want to create an index path (with null data) like that.
            try {
                zk.retryOperation(new ZooKeeperOperation<String>() {
                    @Override
                    public String execute() throws KeeperException, InterruptedException {
                        return zk.create(lockPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                });
            } catch (KeeperException.NodeExistsException e) {
                // ok, someone created it since we checked
            } catch (KeeperException.NoNodeException e) {
                throw new IndexNotFoundException(indexName);
            }
        }

        //
        // Take the actual lock
        //
        return ZkLock.lock(zk, INDEX_COLLECTION_PATH_SLASH + indexName + "/lock");

    }


    @Override
    public String lockIndex(String indexName) throws ZkLockException, IndexNotFoundException, InterruptedException,
            KeeperException, IndexModelException {
        return lockIndexInternal(indexName, true);
    }

    @Override
    public void unlockIndex(String lock) throws ZkLockException {
        ZkLock.unlock(zk, lock);
    }

    @Override
    public void unlockIndex(String lock, boolean ignoreMissing) throws ZkLockException {
        ZkLock.unlock(zk, lock, ignoreMissing);
    }

    @Override
    public IndexDefinition getIndex(String name) throws IndexNotFoundException {
        IndexDefinition index = indexes.get(name);
        if (index == null) {
            throw new IndexNotFoundException(name);
        }
        return index;
    }

    @Override
    public boolean hasIndex(String name) {
        return indexes.containsKey(name);
    }

    @Override
    public IndexDefinition getMutableIndex(String name) throws InterruptedException, KeeperException, IndexNotFoundException {
        return loadIndex(name, false);
    }

    @Override
    public Collection<IndexDefinition> getIndexes() {
        return new ArrayList<IndexDefinition>(indexes.values());
    }

    @Override
    public Collection<IndexDefinition> getIndexes(IndexerModelListener listener) {
        synchronized (indexes_lock) {
            registerListener(listener);
            return new ArrayList<IndexDefinition>(indexes.values());
        }
    }

    private IndexDefinitionImpl loadIndex(String indexName, boolean forCache)
            throws InterruptedException, KeeperException, IndexNotFoundException {
        final String childPath = INDEX_COLLECTION_PATH + "/" + indexName;
        final Stat stat = new Stat();

        byte[] data;
        try {
            if (forCache) {
                // do not retry, install watcher
                data = zk.getData(childPath, watcher, stat);
            } else {
                // do retry, do not install watcher
                data = zk.retryOperation(new ZooKeeperOperation<byte[]>() {
                    @Override
                    public byte[] execute() throws KeeperException, InterruptedException {
                        return zk.getData(childPath, false, stat);
                    }
                });
            }
        } catch (KeeperException.NoNodeException e) {
            throw new IndexNotFoundException(indexName);
        }

        IndexDefinitionImpl index = new IndexDefinitionImpl(indexName);
        index.setZkDataVersion(stat.getVersion());
        IndexDefinitionConverter.INSTANCE.fromJsonBytes(data, index);

        return index;
    }

    private void notifyListeners(List<IndexerModelEvent> events) {
        for (IndexerModelEvent event : events) {
            for (IndexerModelListener listener : listeners) {
                listener.process(event);
            }
        }
    }

    @Override
    public void registerListener(IndexerModelListener listener) {
        this.listeners.add(listener);
    }

    @Override
    public void unregisterListener(IndexerModelListener listener) {
        this.listeners.remove(listener);
    }

    private class IndexModelChangeWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            if (stopped) {
                return;
            }

            try {
                if (NodeChildrenChanged.equals(event.getType()) && event.getPath().equals(INDEX_COLLECTION_PATH)) {
                    indexCacheRefresher.triggerRefreshAllIndexes();
                } else if (NodeDataChanged.equals(event.getType()) && event.getPath().startsWith(INDEX_COLLECTION_PATH_SLASH)) {
                    String indexName = event.getPath().substring(INDEX_COLLECTION_PATH_SLASH.length());
                    indexCacheRefresher.triggerIndexToRefresh(indexName);
                }
            } catch (Throwable t) {
                log.error("Indexer Model: error handling event from ZooKeeper. Event: " + event, t);
            }
        }
    }

    public class ConnectStateWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            if (stopped) {
                return;
            }

            if (event.getType() == Event.EventType.None && event.getState() == Event.KeeperState.SyncConnected) {
                // Each time the connection is established, we trigger refreshing, since the previous refresh
                // might have failed with a ConnectionLoss exception
                indexCacheRefresher.triggerRefreshAllIndexes();
            }
        }
    }

    /**
     * Responsible for updating our internal cache of IndexDefinition's. Should be triggered upon each related
     * change on ZK, as well as on ZK connection established, since this refresher simply fails on ZK connection
     * loss exceptions, rather than retrying.
     */
    private class IndexCacheRefresher implements Runnable {
        private volatile Set<String> indexesToRefresh = new HashSet<String>();
        private volatile boolean refreshAllIndexes;
        private final Object refreshLock = new Object();
        private Thread thread;
        private final Object startedLock = new Object();
        private volatile boolean started = false;

        public synchronized void shutdown() throws InterruptedException {
            if (thread == null || !thread.isAlive()) {
                return;
            }

            thread.interrupt();
            thread.join();
            thread = null;
        }

        public synchronized void start() {
            // Upon startup, be sure to run a refresh of all indexes
            this.refreshAllIndexes = true;

            thread = new Thread(this, "Indexer model refresher");
            // Set as daemon thread: IndexerModel can be used in tools like the indexer admin CLI tools,
            // where we should not require explicit shutdown.
            thread.setDaemon(true);
            thread.start();
        }

        /**
         * Waits until the initial cache fill up happened.
         */
        public void waitUntilStarted() throws InterruptedException {
            synchronized (startedLock) {
                while (!started) {
                    startedLock.wait();
                }
            }
        }

        @Override
        public void run() {
            while (!Thread.interrupted()) {
                try {
                    List<IndexerModelEvent> events = new ArrayList<IndexerModelEvent>();
                    try {
                        Set<String> indexesToRefresh = null;
                        boolean refreshAllIndexes = false;

                        synchronized (refreshLock) {
                            if (this.refreshAllIndexes || this.indexesToRefresh.isEmpty()) {
                                refreshAllIndexes = true;
                            } else {
                                indexesToRefresh = new HashSet<String>(this.indexesToRefresh);
                            }
                            this.refreshAllIndexes = false;
                            this.indexesToRefresh.clear();
                        }

                        if (refreshAllIndexes) {
                            synchronized (indexes_lock) {
                                refreshIndexes(events);
                            }
                        } else {
                            synchronized (indexes_lock) {
                                for (String indexName : indexesToRefresh) {
                                    refreshIndex(indexName, events);
                                }
                            }
                        }

                        if (!started) {
                            started = true;
                            synchronized (startedLock) {
                                startedLock.notifyAll();
                            }
                        }
                    } finally {
                        // We notify the listeners here because we want to be sure events for every
                        // change are delivered, even if halfway through the refreshing we would have
                        // failed due to some error like a ZooKeeper connection loss
                        if (!events.isEmpty() && !stopped && !Thread.currentThread().isInterrupted()) {
                            notifyListeners(events);
                        }
                    }

                    synchronized (refreshLock) {
                        if (!this.refreshAllIndexes && this.indexesToRefresh.isEmpty()) {
                            refreshLock.wait();
                        }
                    }
                } catch (KeeperException.ConnectionLossException e) {
                    // we will be retriggered when the connection is back
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Throwable t) {
                    log.error("Indexer Model Refresher: some exception happened.", t);
                }
            }
        }

        public void triggerIndexToRefresh(String indexName) {
            synchronized (refreshLock) {
                indexesToRefresh.add(indexName);
                refreshLock.notifyAll();
            }
        }

        public void triggerRefreshAllIndexes() {
            synchronized (refreshLock) {
                refreshAllIndexes = true;
                refreshLock.notifyAll();
            }
        }

        private void refreshIndexes(List<IndexerModelEvent> events) throws InterruptedException, KeeperException {
            List<String> indexNames = zk.getChildren(INDEX_COLLECTION_PATH, watcher);

            Set<String> indexNameSet = new HashSet<String>();
            indexNameSet.addAll(indexNames);

            // Remove indexes which no longer exist in ZK
            Iterator<String> currentIndexNamesIt = indexes.keySet().iterator();
            while (currentIndexNamesIt.hasNext()) {
                String indexName = currentIndexNamesIt.next();
                if (!indexNameSet.contains(indexName)) {
                    currentIndexNamesIt.remove();
                    events.add(new IndexerModelEvent(INDEX_REMOVED, indexName));
                }
            }

            // Add/update the other indexes
            for (String indexName : indexNames) {
                refreshIndex(indexName, events);
            }
        }

        /**
         * Adds or updates the given index to the internal cache.
         */
        private void refreshIndex(final String indexName, List<IndexerModelEvent> events)
                throws InterruptedException, KeeperException {
            try {
                IndexDefinitionImpl index = loadIndex(indexName, true);
                index.makeImmutable();

                IndexDefinition oldIndex = indexes.get(indexName);

                if (oldIndex != null && oldIndex.getZkDataVersion() == index.getZkDataVersion()) {
                    // nothing changed
                } else {
                    final boolean isNew = oldIndex == null;
                    indexes.put(indexName, index);
                    events.add(new IndexerModelEvent(isNew ? INDEX_ADDED : INDEX_UPDATED, indexName));
                }
            } catch (IndexNotFoundException e) {
                Object oldIndex = indexes.remove(indexName);

                if (oldIndex != null) {
                    events.add(new IndexerModelEvent(INDEX_REMOVED, indexName));
                }
            }
        }
    }

    /**
     * Check the validity of an index name.
     * <p>
     * An index name can be any string of printable unicode characters that has a length greater than 0. Printable
     * characters in this context are considered to be anything that is not an ISO control character as defined by
     * {@link Character#isISOControl(int)}.
     * 
     * @param indexName The name to validate
     */
    public static void validateIndexName(String indexName) {
        Preconditions.checkNotNull(indexName);
        if (indexName.length() == 0) {
            throw new IllegalArgumentException("Index name is empty");
        }
        for (int charIdx = 0; charIdx < indexName.length(); charIdx++) {
            if (Character.isISOControl(indexName.codePointAt(charIdx))) {
                throw new IllegalArgumentException("Index names may only consist of printable characters");
            }
        }
    }
}
