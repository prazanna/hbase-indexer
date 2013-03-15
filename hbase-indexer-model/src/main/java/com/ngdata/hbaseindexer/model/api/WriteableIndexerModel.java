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

import com.ngdata.hbaseindexer.util.zookeeper.ZkLockException;
import org.apache.zookeeper.KeeperException;

public interface WriteableIndexerModel extends IndexerModel {
    /**
     * Creates a new indexer.
     */
    void addIndexer(IndexerDefinition indexer) throws IndexerExistsException, IndexerModelException, IndexerValidityException;

    /**
     * Loads an indexer definition bypassing the internal cache.
     */
    IndexerDefinition getFreshIndexer(String name) throws InterruptedException, KeeperException, IndexerNotFoundException;

    /**
     * Updates an indexer.
     *
     * <p>Due to the optimistic concurrency control (see {@link IndexerDefinition#getOccVersion()}, the update will
     * only succeed if it was not modified since it was read. This situation can be avoided either by disabling
     * the occ check (by setting the OCC version to -1), but also by taking a lock on the index before
     * reading it. In fact, usage of the lock is obliged.</p>
     *
     * <p>The canonical flow to update an indexer is:</p>
     * <ul>
     *     <li>Obtain a lock by calling {@link #lockIndexer(String)}</li>
     *     <li>Read existing state using {@link #getFreshIndexer(String)}</li>
     *     <li>Create an updated IndexerDefinition using the {@link IndexerDefinitionBuilder}</li>
     *     <li>Call this update method</li>
     *     <li>In a finally block, release the lock using {@link #unlockIndexer(String)}. Note that the lock
     *     will expire in case the client process gets killed (it's a ZooKeeper ephemeral node).</li>
     * </ul>
     */
    void updateIndexer(final IndexerDefinition index, String lock) throws InterruptedException, KeeperException,
            IndexerNotFoundException, IndexerConcurrentModificationException, ZkLockException, IndexerUpdateException, IndexerValidityException;

    /**
     * Internal indexer update method, <b>this method is only intended for internal HBase-indexer components</b>. It
     * is similar to the update method but bypasses some checks.
     *
     * <p>Ordinary clients should use {@link #updateIndexer(IndexerDefinition, String)} instead.</p>
     */
    void updateIndexerInternal(final IndexerDefinition indexer) throws InterruptedException, KeeperException,
            IndexerNotFoundException, IndexerConcurrentModificationException, IndexerValidityException;

    /**
     * Internal indexer delete method, <b>this method is only intended for internal HBase-indexer components</b>.
     *
     * <p>To delete an indexer from an ordinary client, update the indexer with the
     * {@link IndexerDefinition#getLifecycleState() life cycle state} set to
     * {@link IndexerDefinition.LifecycleState#DELETE_REQUESTED}. The system will then react asynchronously
     * to this (first stopping related processes) and eventually delete the indexer.</p>
     */
    void deleteIndexerInternal(final String indexerName) throws IndexerModelException;

    /**
     * Takes a lock on this indexer.
     */
    String lockIndexer(String indexerName) throws ZkLockException, IndexerNotFoundException, InterruptedException,
            KeeperException, IndexerModelException;

    void unlockIndexer(String lock) throws ZkLockException;

    void unlockIndexer(String lock, boolean ignoreMissing) throws ZkLockException;

    /**
     * Internal index lock method, <b>this method is only intended for internal indexer components</b>.
     */
    String lockIndexerInternal(String indexerName, boolean checkDeleted) throws ZkLockException, IndexerNotFoundException,
            InterruptedException, KeeperException, IndexerModelException;
}
