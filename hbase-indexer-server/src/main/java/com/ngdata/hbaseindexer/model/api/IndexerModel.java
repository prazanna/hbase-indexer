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
package com.ngdata.hbaseindexer.model.api;

import java.util.Collection;

/**
 * The repository of {@link IndexerDefinition}s.
 *
 * <p>This interface only contains read-related methods, to modify the indexer model please see
 * the sub-interface {@link WriteableIndexerModel}.</p>
 */
public interface IndexerModel {
    /**
     * Returns all defined indexers.
     */
    Collection<IndexerDefinition> getIndexers();

    /**
     * Gets the list of indexers, and registers a listener for future changes to the indexers. It guarantees
     * that the listener will receive events for all updates that happened after the returned snapshot
     * of the indexers.
     *
     * <p>In case you are familiar with ZooKeeper, note that the listener does not work like the watcher
     * in ZooKeeper: listeners are not one-time only.
     */
    Collection<IndexerDefinition> getIndexers(IndexerModelListener listener);

    /**
     * Retrieves the definition of the indexer with the given name.
     *
     * <p>This throws an exception in case an indexer with this name does not exist, use {@link #hasIndexer(String)}
     * if you want to check this.</p>
     */
    IndexerDefinition getIndexer(String name) throws IndexerNotFoundException;

    /**
     * Checks if an indexer with the given name exists.
     */
    boolean hasIndexer(String name);

    /**
     * Register a listener that will be notified of changes to the indexer model.
     */
    void registerListener(IndexerModelListener listener);

    /**
     * Unregister a listener previously registered via {@link #registerListener(IndexerModelListener)}.
     *
     * <p>In case the listeners would not exist, this method will silently return.</p>
     */
    void unregisterListener(IndexerModelListener listener);
}
