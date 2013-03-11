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

import org.apache.commons.lang.builder.EqualsBuilder;

/**
 * Defines an indexer within the {@link IndexerModel}.
 *
 * <p>This object is immutable, use {@link IndexerDefinitionBuilder} to construct instances.</p>
 */
public class IndexerDefinition {
    private String name;
    private IndexGeneralState generalState = IndexGeneralState.ACTIVE;
    private IndexBatchBuildState batchBuildState = IndexBatchBuildState.INACTIVE;
    private IndexUpdateState updateState = IndexUpdateState.SUBSCRIBE_AND_LISTEN;
    private String subscriptionId;
    private byte[] configuration;
    private byte[] connectionConfiguration;
    private byte[] defaultBatchIndexConfiguration;
    private byte[] batchIndexConfiguration;
    private BatchBuildInfo lastBatchBuildInfo;
    private ActiveBatchBuildInfo activeBatchBuildInfo;
    private long subscriptionTimestamp;
    private int zkDataVersion = -1;

    /**
     * Use {@link IndexerDefinitionBuilder} to make instances of this class.
     */
    IndexerDefinition(String name,
            IndexGeneralState generalState,
            IndexBatchBuildState batchBuildState,
            IndexUpdateState updateState,
            String subscriptionId,
            byte[] configuration,
            byte[] connectionConfiguration,
            byte[] defaultBatchIndexConfiguration,
            byte[] batchIndexConfiguration,
            BatchBuildInfo lastBatchBuildInfo,
            ActiveBatchBuildInfo activeBatchBuildInfo,
            long subscriptionTimestamp,
            int zkDataVersion) {
        this.name = name;
        this.generalState = generalState;
        this.batchBuildState = batchBuildState;
        this.updateState = updateState;
        this.subscriptionId = subscriptionId;
        this.configuration = configuration;
        this.connectionConfiguration = connectionConfiguration;
        this.defaultBatchIndexConfiguration = defaultBatchIndexConfiguration;
        this.batchIndexConfiguration = batchIndexConfiguration;
        this.lastBatchBuildInfo = lastBatchBuildInfo;
        this.activeBatchBuildInfo = activeBatchBuildInfo;
        this.subscriptionTimestamp = subscriptionTimestamp;
        this.zkDataVersion = zkDataVersion;
    }

    public String getName() {
        return name;
    }

    public IndexGeneralState getGeneralState() {
        return generalState;
    }

    public IndexBatchBuildState getBatchBuildState() {
        return batchBuildState;
    }

    public IndexUpdateState getUpdateState() {
        return updateState;
    }

    public String getSubscriptionId() {
        return subscriptionId;
    }

    /**
     * The XML configuration for the Indexer.
     */
    public byte[] getConfiguration() {
        // Note that while one could modify the returned byte array, it is very unlikely to do this
        // by accident, and we assume cooperating users.
        return configuration;
    }

    public byte[] getConnectionConfiguration() {
        return connectionConfiguration;
    }

    public int getZkDataVersion() {
        return zkDataVersion;
    }

    public BatchBuildInfo getLastBatchBuildInfo() {
        return lastBatchBuildInfo;
    }

    public ActiveBatchBuildInfo getActiveBatchBuildInfo() {
        return activeBatchBuildInfo;
    }

    public byte[] getBatchIndexConfiguration() {
        return batchIndexConfiguration;
    }

    public byte[] getDefaultBatchIndexConfiguration() {
        return defaultBatchIndexConfiguration;
    }

    /**
     * Get the timestamp of when this index's update subscription started.
     * 
     * @return Number of milliseconds since the epoch
     */
    public long getSubscriptionTimestamp() {
        return subscriptionTimestamp;
    }

    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }
}
