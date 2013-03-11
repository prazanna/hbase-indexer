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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import com.ngdata.hbaseindexer.model.api.ActiveBatchBuildInfoBuilder;
import com.ngdata.hbaseindexer.model.api.BatchBuildInfoBuilder;
import com.ngdata.hbaseindexer.model.api.IndexerDefinition;
import com.ngdata.hbaseindexer.model.api.IndexerDefinitionBuilder;
import com.ngdata.hbaseindexer.util.json.JsonUtil;
import net.iharder.Base64;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import com.ngdata.hbaseindexer.model.api.ActiveBatchBuildInfo;
import com.ngdata.hbaseindexer.model.api.BatchBuildInfo;
import com.ngdata.hbaseindexer.model.api.IndexBatchBuildState;
import com.ngdata.hbaseindexer.model.api.IndexGeneralState;
import com.ngdata.hbaseindexer.model.api.IndexUpdateState;

public class IndexerDefinitionJsonSerDeser {
    public static IndexerDefinitionJsonSerDeser INSTANCE = new IndexerDefinitionJsonSerDeser();

    public IndexerDefinitionBuilder fromJsonBytes(byte[] json) {
        ObjectNode node;
        try {
            node = (ObjectNode)new ObjectMapper().readTree(new ByteArrayInputStream(json));
        } catch (IOException e) {
            throw new RuntimeException("Error parsing index definition JSON.", e);
        }
        return fromJson(node);
    }

    public IndexerDefinitionBuilder fromJson(ObjectNode node) {
        String name = JsonUtil.getString(node, "name");
        IndexGeneralState state = IndexGeneralState.valueOf(JsonUtil.getString(node, "generalState"));
        IndexUpdateState updateState = IndexUpdateState.valueOf(JsonUtil.getString(node, "updateState"));
        IndexBatchBuildState buildState = IndexBatchBuildState.valueOf(JsonUtil.getString(node, "batchBuildState"));

        String queueSubscriptionId = JsonUtil.getString(node, "subscriptionId", null);
        long subscriptionTimestamp = JsonUtil.getLong(node, "subscriptionTimestamp", 0L);
        
        byte[] configuration = getByteArrayProperty(node, "configuration");

        byte[] connectionConfiguration = getByteArrayProperty(node, "connectionConfiguration");

        ActiveBatchBuildInfo activeBatchBuild = null;
        if (node.get("activeBatchBuild") != null) {
            ObjectNode buildNode = JsonUtil.getObject(node, "activeBatchBuild");
            ActiveBatchBuildInfoBuilder builder = new ActiveBatchBuildInfoBuilder();
            builder.jobId(JsonUtil.getString(buildNode, "jobId"));
            builder.submitTime(JsonUtil.getLong(buildNode, "submitTime"));
            builder.trackingUrl(JsonUtil.getString(buildNode, "trackingUrl", null));
            builder.batchIndexConfiguration(getByteArrayProperty(buildNode, "batchIndexConfiguration"));
            activeBatchBuild = builder.build();
        }

        BatchBuildInfo lastBatchBuild = null;
        if (node.get("lastBatchBuild") != null) {
            ObjectNode buildNode = JsonUtil.getObject(node, "lastBatchBuild");
            BatchBuildInfoBuilder builder = new BatchBuildInfoBuilder();
            builder.jobId(JsonUtil.getString(buildNode, "jobId"));
            builder.submitTime(JsonUtil.getLong(buildNode, "submitTime"));
            builder.success(JsonUtil.getBoolean(buildNode, "success"));
            builder.jobState(JsonUtil.getString(buildNode, "jobState"));
            builder.trackingUrl(JsonUtil.getString(buildNode, "trackingUrl", null));
            ObjectNode countersNode = JsonUtil.getObject(buildNode, "counters");
            Iterator<String> it = countersNode.getFieldNames();
            while (it.hasNext()) {
                String key = it.next();
                long value = JsonUtil.getLong(countersNode, key);
                builder.counter(key, value);
            }
            builder.batchIndexConfiguration(getByteArrayProperty(buildNode, "batchIndexConfiguration"));
            lastBatchBuild = builder.build();
        }

        byte[] batchIndexConfiguration = getByteArrayProperty(node, "batchIndexConfiguration");
        byte[] defaultBatchIndexConfiguration = getByteArrayProperty(node, "defaultBatchIndexConfiguration");

        int zkDataVersion = JsonUtil.getInt(node, "zkDataVersion");

        IndexerDefinitionBuilder builder = new IndexerDefinitionBuilder();
        builder.name(name);
        builder.generalState(state);
        builder.updateState(updateState);
        builder.batchBuildState(buildState);
        builder.subscriptionId(queueSubscriptionId);
        builder.subscriptionTimestamp(subscriptionTimestamp);
        builder.configuration(configuration);
        builder.connectionConfiguration(connectionConfiguration);
        builder.activeBatchBuildInfo(activeBatchBuild);
        builder.lastBatchBuildInfo(lastBatchBuild);
        builder.batchIndexConfiguration(batchIndexConfiguration);
        builder.defaultBatchIndexConfiguration(defaultBatchIndexConfiguration);
        builder.zkDataVersion(zkDataVersion);
        return builder;
    }

    private byte[] getByteArrayProperty(ObjectNode node, String property) {
        try {
            String string = JsonUtil.getString(node, property, null);
            if (string == null)
                return null;
            return Base64.decode(string);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void setByteArrayProperty(ObjectNode node, String property, byte[] data) {
        if (data == null)
            return;
        try {
            node.put(property, Base64.encodeBytes(data, Base64.GZIP));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] toJsonBytes(IndexerDefinition index) {
        try {
            return new ObjectMapper().writeValueAsBytes(toJson(index));
        } catch (IOException e) {
            throw new RuntimeException("Error serializing index definition to JSON.", e);
        }
    }

    public ObjectNode toJson(IndexerDefinition index) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();

        node.put("name", index.getName());
        node.put("generalState", index.getGeneralState().toString());
        node.put("batchBuildState", index.getBatchBuildState().toString());
        node.put("updateState", index.getUpdateState().toString());

        node.put("zkDataVersion", index.getZkDataVersion());

        if (index.getSubscriptionId() != null)
            node.put("subscriptionId", index.getSubscriptionId());
        
        node.put("subscriptionTimestamp", index.getSubscriptionTimestamp());

        setByteArrayProperty(node, "configuration", index.getConfiguration());
        setByteArrayProperty(node, "connectionConfiguration", index.getConnectionConfiguration());

        if (index.getActiveBatchBuildInfo() != null) {
            ActiveBatchBuildInfo buildInfo = index.getActiveBatchBuildInfo();
            ObjectNode batchNode = node.putObject("activeBatchBuild");
            batchNode.put("jobId", buildInfo.getJobId());
            batchNode.put("submitTime", buildInfo.getSubmitTime());
            batchNode.put("trackingUrl", buildInfo.getTrackingUrl());
            setByteArrayProperty(batchNode, "batchIndexConfiguration", buildInfo.getBatchIndexConfiguration());
        }

        if (index.getLastBatchBuildInfo() != null) {
            BatchBuildInfo buildInfo = index.getLastBatchBuildInfo();
            ObjectNode batchNode = node.putObject("lastBatchBuild");
            batchNode.put("jobId", buildInfo.getJobId());
            batchNode.put("submitTime", buildInfo.getSubmitTime());
            batchNode.put("success", buildInfo.getSuccess());
            batchNode.put("jobState", buildInfo.getJobState());
            if (buildInfo.getTrackingUrl() != null)
                batchNode.put("trackingUrl", buildInfo.getTrackingUrl());
            ObjectNode countersNode = batchNode.putObject("counters");
            for (Map.Entry<String, Long> counter : buildInfo.getCounters().entrySet()) {
                countersNode.put(counter.getKey(), counter.getValue());
            }
            setByteArrayProperty(batchNode, "batchIndexConfiguration", buildInfo.getBatchIndexConfiguration());
        }

        setByteArrayProperty(node, "batchIndexConfiguration", index.getBatchIndexConfiguration());
        setByteArrayProperty(node, "defaultBatchIndexConfiguration", index.getDefaultBatchIndexConfiguration());

        return node;
    }
}
