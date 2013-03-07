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

import java.util.List;
import java.util.Map;

public interface IndexDefinition {
    String getName();
    
    IndexGeneralState getGeneralState();

    void setGeneralState(IndexGeneralState state);

    IndexBatchBuildState getBatchBuildState();

    void setBatchBuildState(IndexBatchBuildState state);

    /**
     * If the state implies that there should be a message queue subscription, check
     * {@link #getQueueSubscriptionId()} to see if the subscription is already assigned,
     * same for unsubscribe.
     */
    IndexUpdateState getUpdateState();

    void setUpdateState(IndexUpdateState state);

    String getQueueSubscriptionId();

    void setQueueSubscriptionId(String queueSubscriptionId);

    /**
     * The XML configuration for the Indexer.
     */
    byte[] getConfiguration();

    void setConfiguration(byte[] configuration);

    /**
     * The JSON configuration for the shard selector.
     */
    byte[] getShardingConfiguration();

    void setShardingConfiguration(byte[] configuration);

    /**
     * Map containing the Solr shards: the key is a logical name for the shard, the value is the
     * address (URL) of the shard.
     */
    Map<String, String> getSolrShards();

    void setSolrShards(Map<String, String> shards);

    int getZkDataVersion();

    BatchBuildInfo getLastBatchBuildInfo();

    void setLastBatchBuildInfo(BatchBuildInfo info);

    ActiveBatchBuildInfo getActiveBatchBuildInfo();

    void setActiveBatchBuildInfo(ActiveBatchBuildInfo info);

    /**
     * The JSON configuration for the batch indexer
     */
    byte[] getBatchIndexConfiguration ();
    void setBatchIndexConfiguration(byte[] batchIndexConfiguration);

    byte[] getDefaultBatchIndexConfiguration();
    void setDefaultBatchIndexConfiguration(byte[] defaultBatchIndexConfiguration);
    
    /**
     * Get the default list of repository tables that a batch index rebuild will run on.
     * <p>
     * This method can return an empty list, in which case a batch index rebuild will run on
     * all repository tables.
     * 
     * @return default list of tables
     */
    List<String> getDefaultBatchTables();
    
    /**
     * Set the default list of repository tables that a batch index rebuild will run on.
     * <p>
     * If the value is null or an empty list, all tables will be used for batch rebuilds.
     * 
     * @param tables default list of tables
     */
    void setDefaultBatchTables(List<String> tables);
    
    /**
     * Get the list of repository tables that the next batch index rebuild will run on.
     * <p>
     * This method can return an empty list, in which case a batch index rebuild will run on
     * all repository tables.
     * 
     * @return list of tables
     */
    List<String> getBatchTables();
    
    /**
     * Set the list of repository tables that the next batch index rebuild will run on.
     * <p>
     * If the value is null or an empty list, all tables will be used for batch rebuilds.
     * 
     * @param tables list of tables
     */
    void setBatchTables(List<String> tables);

    String getZkConnectionString();
    void setZkConnectionString(String zkConnectionString);

    String getSolrCollection();
    void setSolrCollection(String collection);

    /**
     * Do we need to maintain a deref map for this index. Note that if this returns true (the default) but the index
     * configuration doesn't have any dereference expressions, than no deref map will be maintained anyways (because
     * there is no need).
     *
     * Setting this to false might be useful in the situation that you have an index with dereference expressions, but
     * you plan to populate this index only through batch index building (which doesn't use the deref map anyways).
     *
     * @return true if a deref map should be maintained for this index (this is the default), false otherwise.
     */
    boolean isEnableDerefMap();

    void setEnableDerefMap(boolean enableDerefMap);
    
    /**
     * Set the timestamp of when this index's update subscription started. Only record updates that have
     * occurred after this timestamp will be consumed by this index.
     * 
     * @param timestamp Number of milliseconds since the epoch
     */
    void setSubscriptionTimestamp(long timestamp);

    /**
     * Get the timestamp of when this index's update subscription started.
     * 
     * @return Number of milliseconds since the epoch
     */
    long getSubscriptionTimestamp();

}
