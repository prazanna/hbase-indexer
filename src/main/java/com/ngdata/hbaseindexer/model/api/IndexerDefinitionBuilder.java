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

import com.google.common.base.Preconditions;

public class IndexerDefinitionBuilder {
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

    public IndexerDefinitionBuilder startFrom(IndexerDefinition existingDefinition) {
        this.name = existingDefinition.getName();
        this.generalState = existingDefinition.getGeneralState();
        this.batchBuildState = existingDefinition.getBatchBuildState();
        this.updateState = existingDefinition.getUpdateState();
        this.subscriptionId = existingDefinition.getSubscriptionId();
        this.configuration = existingDefinition.getConfiguration();
        this.connectionConfiguration = existingDefinition.getConnectionConfiguration();
        this.defaultBatchIndexConfiguration = existingDefinition.getDefaultBatchIndexConfiguration();
        this.batchIndexConfiguration = existingDefinition.getBatchIndexConfiguration();
        this.lastBatchBuildInfo = existingDefinition.getLastBatchBuildInfo();
        this.activeBatchBuildInfo = existingDefinition.getActiveBatchBuildInfo();
        this.subscriptionTimestamp = existingDefinition.getSubscriptionTimestamp();
        this.zkDataVersion = existingDefinition.getZkDataVersion();
        return this;
    }

    public IndexerDefinitionBuilder name(String name) {
        this.name = name;
        return this;
    }

    public IndexerDefinitionBuilder generalState(IndexGeneralState state) {
        this.generalState = state;
        return this;
    }

    public IndexerDefinitionBuilder updateState(IndexUpdateState state) {
        this.updateState = state;
        return this;
    }

    public IndexerDefinitionBuilder batchBuildState(IndexBatchBuildState state) {
        this.batchBuildState = state;
        return this;
    }

    public IndexerDefinitionBuilder subscriptionId(String queueSubscriptionId) {
        this.subscriptionId = queueSubscriptionId;
        return this;
    }

    public IndexerDefinitionBuilder configuration(byte[] configuration) {
        this.configuration = configuration;
        return this;
    }

    public IndexerDefinitionBuilder connectionConfiguration(byte[] configuration) {
        this.connectionConfiguration = configuration;
        return this;
    }

    public IndexerDefinitionBuilder lastBatchBuildInfo(BatchBuildInfo info) {
        this.lastBatchBuildInfo = info;
        return this;
    }

    public IndexerDefinitionBuilder activeBatchBuildInfo(ActiveBatchBuildInfo info) {
        this.activeBatchBuildInfo = info;
        return this;
    }

    public IndexerDefinitionBuilder defaultBatchIndexConfiguration(byte[] defaultBatchIndexConfiguration) {
        this.defaultBatchIndexConfiguration = defaultBatchIndexConfiguration;
        return this;
    }

    public IndexerDefinitionBuilder batchIndexConfiguration(byte[] batchIndexConfiguration) {
        this.batchIndexConfiguration = batchIndexConfiguration;
        return this;
    }

    /**
     * Set the timestamp of when this index's update subscription started. Only record updates that have
     * occurred after this timestamp will be consumed by this index.
     *
     * @param timestamp Number of milliseconds since the epoch
     */
    public IndexerDefinitionBuilder subscriptionTimestamp(long timestamp) {
        this.subscriptionTimestamp = timestamp;
        return this;
    }

    public IndexerDefinitionBuilder zkDataVersion(int zkDataVersion) {
        this.zkDataVersion = zkDataVersion;
        return this;
    }

    public IndexerDefinition build() {
        Preconditions.checkNotNull(name, "name");
        Preconditions.checkNotNull(generalState, "generalState");
        Preconditions.checkNotNull(batchBuildState, "batchBuildState");
        Preconditions.checkNotNull(updateState, "updateState");

        return new IndexerDefinition(name, generalState, batchBuildState, updateState, subscriptionId,
                configuration, connectionConfiguration, defaultBatchIndexConfiguration, batchIndexConfiguration,
                lastBatchBuildInfo, activeBatchBuildInfo, subscriptionTimestamp, zkDataVersion);
    }
}
