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
package com.ngdata.hbaseindexer.model.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.builder.EqualsBuilder;
import com.ngdata.hbaseindexer.model.api.ActiveBatchBuildInfo;
import com.ngdata.hbaseindexer.model.api.BatchBuildInfo;
import com.ngdata.hbaseindexer.model.api.IndexBatchBuildState;
import com.ngdata.hbaseindexer.model.api.IndexDefinition;
import com.ngdata.hbaseindexer.model.api.IndexGeneralState;
import com.ngdata.hbaseindexer.model.api.IndexUpdateState;

public class IndexDefinitionImpl implements IndexDefinition {
    private final String name;
    private IndexGeneralState generalState = IndexGeneralState.ACTIVE;
    private IndexBatchBuildState buildState = IndexBatchBuildState.INACTIVE;
    private IndexUpdateState updateState = IndexUpdateState.SUBSCRIBE_AND_LISTEN;
    private String queueSubscriptionId;
    private byte[] configuration;
    private byte[] shardingConfiguration;
    private byte[] defaultBatchIndexConfiguration;
    private byte[] batchIndexConfiguration;
    private List<String> defaultBatchTables;
    private List<String> batchTables;
    private Map<String, String> solrShards = Collections.emptyMap();
    private int zkDataVersion = -1;
    private BatchBuildInfo lastBatchBuildInfo;
    private ActiveBatchBuildInfo activeBatchBuildInfo;
    private boolean immutable;
    private String zkConnectionString;
    private String solrCollection;
    private boolean enableDerefMap;
    private long subscriptionTimestamp;

    public IndexDefinitionImpl(String name) {
        this.name = name;
        this.enableDerefMap = true; // default true
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public IndexGeneralState getGeneralState() {
        return generalState;
    }

    @Override
    public void setGeneralState(IndexGeneralState state) {
        checkIfMutable();
        this.generalState = state;
    }

    @Override
    public IndexBatchBuildState getBatchBuildState() {
        return buildState;
    }

    @Override
    public void setBatchBuildState(IndexBatchBuildState state) {
        checkIfMutable();
        this.buildState = state;
    }

    @Override
    public IndexUpdateState getUpdateState() {
        return updateState;
    }

    @Override
    public void setUpdateState(IndexUpdateState state) {
        checkIfMutable();
        if (this.updateState == IndexUpdateState.DO_NOT_SUBSCRIBE && state != IndexUpdateState.DO_NOT_SUBSCRIBE) {
            setSubscriptionTimestamp(System.currentTimeMillis());
        }
        this.updateState = state;
    }

    @Override
    public String getQueueSubscriptionId() {
        return queueSubscriptionId;
    }

    @Override
    public void setQueueSubscriptionId(String queueSubscriptionId) {
        checkIfMutable();
        this.queueSubscriptionId = queueSubscriptionId;
    }

    @Override
    public byte[] getConfiguration() {
        // Note that while one could modify the returned byte array, it is very unlikely to do this
        // by accident, and we assume cooperating users.
        return configuration;
    }

    @Override
    public void setConfiguration(byte[] configuration) {
        this.configuration = configuration;
    }

    @Override
    public byte[] getShardingConfiguration() {
        return shardingConfiguration;
    }

    @Override
    public void setShardingConfiguration(byte[] shardingConfiguration) {
        this.shardingConfiguration = shardingConfiguration;
    }

    @Override
    public Map<String, String> getSolrShards() {
        return new HashMap<String, String>(solrShards);
    }

    @Override
    public void setSolrShards(Map<String, String> shards) {
        this.solrShards = new HashMap<String, String>(shards);
    }

    @Override
    public int getZkDataVersion() {
        return zkDataVersion;
    }

    public void setZkDataVersion(int zkDataVersion) {
        checkIfMutable();
        this.zkDataVersion = zkDataVersion;
    }

    @Override
    public BatchBuildInfo getLastBatchBuildInfo() {
        return lastBatchBuildInfo;
    }

    @Override
    public void setLastBatchBuildInfo(BatchBuildInfo info) {
        checkIfMutable();
        this.lastBatchBuildInfo = info;
    }

    @Override
    public ActiveBatchBuildInfo getActiveBatchBuildInfo() {
        return activeBatchBuildInfo;
    }

    @Override
    public void setActiveBatchBuildInfo(ActiveBatchBuildInfo info) {
        checkIfMutable();
        this.activeBatchBuildInfo = info;
    }

    public void makeImmutable() {
        this.immutable = true;
        if (lastBatchBuildInfo != null)
            lastBatchBuildInfo.makeImmutable();
        if (activeBatchBuildInfo != null)
            activeBatchBuildInfo.makeImmutable();
    }

    private void checkIfMutable() {
        if (immutable)
            throw new RuntimeException("This IndexDefinition is immutable");
    }

    @Override
    public byte[] getDefaultBatchIndexConfiguration() {
        return defaultBatchIndexConfiguration;
    }

    @Override
    public void setDefaultBatchIndexConfiguration(byte[] defaultBatchIndexConfiguration) {
        this.defaultBatchIndexConfiguration = defaultBatchIndexConfiguration;
    }

    @Override
    public byte[] getBatchIndexConfiguration() {
        return batchIndexConfiguration;
    }

    @Override
    public void setBatchIndexConfiguration(byte[] batchIndexConfiguration) {
        this.batchIndexConfiguration = batchIndexConfiguration;
    }
    
    @Override
    public void setDefaultBatchTables(List<String> tables) {
        this.defaultBatchTables = (tables == null || tables.isEmpty()) ? null : tables;
    }
    
    @Override
    public List<String> getDefaultBatchTables() {
        return this.defaultBatchTables;
    }
    
    @Override
    public void setBatchTables(List<String> tables) {
        this.batchTables = (tables == null || tables.isEmpty()) ? null : tables;
    }
    
    @Override
    public List<String> getBatchTables() {
        return this.batchTables;
    }

    @Override
    public String getZkConnectionString() {
        return zkConnectionString;
    }

    @Override
    public void setZkConnectionString(String zkConnectionString) {
        this.zkConnectionString = zkConnectionString;
    }

    @Override
    public String getSolrCollection() {
        return solrCollection;
    }

    @Override
    public void setSolrCollection(String collection) {
        this.solrCollection = collection;
    }

    @Override
    public boolean isEnableDerefMap() {
        return this.enableDerefMap;
    }

    @Override
    public void setEnableDerefMap(boolean enableDerefMap) {
        this.enableDerefMap = enableDerefMap;
    }
    
    @Override
    public void setSubscriptionTimestamp(long timestamp) {
        this.subscriptionTimestamp = timestamp;
    }
    
    @Override
    public long getSubscriptionTimestamp() {
        return subscriptionTimestamp;
    }
    
    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }
}
