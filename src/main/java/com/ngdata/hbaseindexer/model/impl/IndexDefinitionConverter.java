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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.ngdata.hbaseindexer.util.json.JsonUtil;
import net.iharder.Base64;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import com.ngdata.hbaseindexer.model.api.ActiveBatchBuildInfo;
import com.ngdata.hbaseindexer.model.api.BatchBuildInfo;
import com.ngdata.hbaseindexer.model.api.IndexBatchBuildState;
import com.ngdata.hbaseindexer.model.api.IndexDefinition;
import com.ngdata.hbaseindexer.model.api.IndexGeneralState;
import com.ngdata.hbaseindexer.model.api.IndexUpdateState;

public class IndexDefinitionConverter {
    public static IndexDefinitionConverter INSTANCE = new IndexDefinitionConverter();

    public void fromJsonBytes(byte[] json, IndexDefinitionImpl index) {
        ObjectNode node;
        try {
            node = (ObjectNode)new ObjectMapper().readTree(new ByteArrayInputStream(json));
        } catch (IOException e) {
            throw new RuntimeException("Error parsing index definition JSON.", e);
        }
        fromJson(node, index);
    }

    public void fromJson(ObjectNode node, IndexDefinitionImpl index) {
        IndexGeneralState state = IndexGeneralState.valueOf(JsonUtil.getString(node, "generalState"));
        IndexUpdateState updateState = IndexUpdateState.valueOf(JsonUtil.getString(node, "updateState"));
        IndexBatchBuildState buildState = IndexBatchBuildState.valueOf(JsonUtil.getString(node, "batchBuildState"));

        String queueSubscriptionId = JsonUtil.getString(node, "queueSubscriptionId", null);
        long subscriptionTimestamp = JsonUtil.getLong(node, "subscriptionTimestamp", 0L);
        
        List<String> defaultBatchTables = JsonUtil.getStrings(node, "defaultBatchTables", null);
        List<String> batchTables = JsonUtil.getStrings(node, "batchTables", null);

        byte[] configuration;
        try {
            String configurationAsString = JsonUtil.getString(node, "configuration");
            configuration = Base64.decode(configurationAsString);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        byte[] shardingConfiguration = null;
        if (node.get("shardingConfiguration") != null) {
            String shardingConfAsString = JsonUtil.getString(node, "shardingConfiguration");
            try {
                shardingConfiguration = Base64.decode(shardingConfAsString);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        Map<String, String> solrShards = new HashMap<String, String>();
        ArrayNode shardsArray = JsonUtil.getArray(node, "solrShards");
        for (int i = 0; i < shardsArray.size(); i++) {
            ObjectNode shardNode = (ObjectNode)shardsArray.get(i);
            String shardName = JsonUtil.getString(shardNode, "name");
            String address = JsonUtil.getString(shardNode, "address");
            solrShards.put(shardName, address);
        }

        if (node.has("zkConnectionString")) index.setZkConnectionString(node.get("zkConnectionString").getTextValue());
        if (node.has("solrCollection")) index.setSolrCollection(node.get("solrCollection").getTextValue());

        ActiveBatchBuildInfo activeBatchBuild = null;
        if (node.get("activeBatchBuild") != null) {
            ObjectNode buildNode = JsonUtil.getObject(node, "activeBatchBuild");
            activeBatchBuild = new ActiveBatchBuildInfo();
            activeBatchBuild.setJobId(JsonUtil.getString(buildNode, "jobId"));
            activeBatchBuild.setSubmitTime(JsonUtil.getLong(buildNode, "submitTime"));
            activeBatchBuild.setTrackingUrl(JsonUtil.getString(buildNode, "trackingUrl", null));
            // no likely that this attribute isn't available but check for it just in case
            if (buildNode.has("batchIndexConfiguration") && !buildNode.get("batchIndexConfiguration").isNull()) {
                activeBatchBuild.setBatchIndexConfiguration(serializeJsonNode(
                        JsonUtil.getObject(buildNode, "batchIndexConfiguration")));
            }

        }

        BatchBuildInfo lastBatchBuild = null;
        if (node.has("lastBatchBuild") && ! node.get("lastBatchBuild").isNull()) {
            ObjectNode buildNode = JsonUtil.getObject(node, "lastBatchBuild");
            lastBatchBuild = new BatchBuildInfo();
            lastBatchBuild.setJobId(JsonUtil.getString(buildNode, "jobId"));
            lastBatchBuild.setSubmitTime(JsonUtil.getLong(buildNode, "submitTime"));
            lastBatchBuild.setSuccess(JsonUtil.getBoolean(buildNode, "success"));
            lastBatchBuild.setJobState(JsonUtil.getString(buildNode, "jobState"));
            lastBatchBuild.setTrackingUrl(JsonUtil.getString(buildNode, "trackingUrl", null));
            ObjectNode countersNode = JsonUtil.getObject(buildNode, "counters");
            Iterator<String> it = countersNode.getFieldNames();
            while (it.hasNext()) {
                String key = it.next();
                long value = JsonUtil.getLong(countersNode, key);
                lastBatchBuild.addCounter(key, value);
            }
            // this attribute isn't available after doing an upgrade so check
            if (buildNode.has("batchIndexConfiguration") && !buildNode.get("batchIndexConfiguration").isNull()) {
                lastBatchBuild.setBatchIndexConfiguration(serializeJsonNode(
                        JsonUtil.getObject(buildNode, "batchIndexConfiguration")));
            }

        }
        byte[] batchIndexConfiguration = null;
        if (node.has("batchIndexConfiguration") && !node.get("batchIndexConfiguration").isNull()) {
            batchIndexConfiguration = serializeJsonNode(JsonUtil.getObject(node, "batchIndexConfiguration"));
        }
        byte[] defaultBatchIndexConfiguration = null;
        if (node.has("defaultBatchIndexConfiguration") && !node.get("defaultBatchIndexConfiguration").isNull()) {
            defaultBatchIndexConfiguration = serializeJsonNode(JsonUtil.getObject(node, "defaultBatchIndexConfiguration"));
        }

        if (node.has("maintainDerefMap") && !node.get("maintainDerefMap").isNull()) index.setEnableDerefMap(node.get("maintainDerefMap").asBoolean());

        index.setGeneralState(state);
        index.setUpdateState(updateState);
        index.setBatchBuildState(buildState);
        index.setQueueSubscriptionId(queueSubscriptionId);
        index.setSubscriptionTimestamp(subscriptionTimestamp);
        index.setConfiguration(configuration);
        index.setSolrShards(solrShards);
        index.setShardingConfiguration(shardingConfiguration);
        index.setActiveBatchBuildInfo(activeBatchBuild);
        index.setLastBatchBuildInfo(lastBatchBuild);
        index.setBatchIndexConfiguration(batchIndexConfiguration);
        index.setDefaultBatchIndexConfiguration(defaultBatchIndexConfiguration);
        index.setDefaultBatchTables(defaultBatchTables);
        index.setBatchTables(batchTables);
    }

    public byte[] toJsonBytes(IndexDefinition index) {
        try {
            return new ObjectMapper().writeValueAsBytes(toJson(index));
        } catch (IOException e) {
            throw new RuntimeException("Error serializing index definition to JSON.", e);
        }
    }

    public ObjectNode toJson(IndexDefinition index) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();

        node.put("name", index.getName());
        node.put("generalState", index.getGeneralState().toString());
        node.put("batchBuildState", index.getBatchBuildState().toString());
        node.put("updateState", index.getUpdateState().toString());

        node.put("zkDataVersion", index.getZkDataVersion());

        if (index.getQueueSubscriptionId() != null)
            node.put("queueSubscriptionId", index.getQueueSubscriptionId());
        
        node.put("subscriptionTimestamp", index.getSubscriptionTimestamp());

        String configurationAsString;
        try {
            configurationAsString = Base64.encodeBytes(index.getConfiguration(), Base64.GZIP);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        node.put("configuration", configurationAsString);

        if (index.getShardingConfiguration() != null) {
            String shardingConfAsString;
            try {
                shardingConfAsString = Base64.encodeBytes(index.getShardingConfiguration(), Base64.GZIP);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            node.put("shardingConfiguration", shardingConfAsString);
        }

        ArrayNode shardsNode = node.putArray("solrShards");
        for (Map.Entry<String, String> shard : index.getSolrShards().entrySet()) {
            ObjectNode shardNode = shardsNode.addObject();
            shardNode.put("name", shard.getKey());
            shardNode.put("address", shard.getValue());
        }

        if (index.getZkConnectionString() != null) {
            node.put("zkConnectionString", index.getZkConnectionString());
        }
        if (index.getSolrCollection() != null) {
            node.put("solrCollection", index.getSolrCollection());
        }

        if (index.getActiveBatchBuildInfo() != null) {
            ActiveBatchBuildInfo buildInfo = index.getActiveBatchBuildInfo();
            ObjectNode buildNode = node.putObject("activeBatchBuild");
            buildNode.put("jobId", buildInfo.getJobId());
            buildNode.put("submitTime", buildInfo.getSubmitTime());
            buildNode.put("trackingUrl", buildInfo.getTrackingUrl());
            buildNode.put("batchIndexConfiguration", deserializeByteArray(buildInfo.getBatchIndexConfiguration()));
        }

        if (index.getLastBatchBuildInfo() != null) {
            BatchBuildInfo buildInfo = index.getLastBatchBuildInfo();
            ObjectNode buildNode = node.putObject("lastBatchBuild");
            buildNode.put("jobId", buildInfo.getJobId());
            buildNode.put("submitTime", buildInfo.getSubmitTime());
            buildNode.put("success", buildInfo.getSuccess());
            buildNode.put("jobState", buildInfo.getJobState());
            if (buildInfo.getTrackingUrl() != null)
                buildNode.put("trackingUrl", buildInfo.getTrackingUrl());
            ObjectNode countersNode = buildNode.putObject("counters");
            for (Map.Entry<String, Long> counter : buildInfo.getCounters().entrySet()) {
                countersNode.put(counter.getKey(), counter.getValue());
            }
            if (buildInfo.getBatchIndexConfiguration() != null) {
              buildNode.put("batchIndexConfiguration", deserializeByteArray(buildInfo.getBatchIndexConfiguration()));
            }
        }
        if (index.getBatchIndexConfiguration() != null) {
            node.put("batchIndexConfiguration", this.deserializeByteArray(index.getBatchIndexConfiguration()));
        }
        if (index.getDefaultBatchIndexConfiguration() != null) {
            node.put("defaultBatchIndexConfiguration", this.deserializeByteArray(index.getDefaultBatchIndexConfiguration()));
        }

        node.put("maintainDerefMap", index.isEnableDerefMap());
        
        List<String> defaultBatchTables = index.getDefaultBatchTables();
        if (defaultBatchTables != null) {
            ArrayNode defaultBatchTableNode = node.putArray("defaultBatchTables");
            for (String defaultBatchTable : defaultBatchTables) {
                defaultBatchTableNode.add(defaultBatchTable);
            }
        }
        
        List<String> batchTables = index.getBatchTables();
        if (batchTables != null) {
            ArrayNode batchTableNode = node.putArray("batchTables");
            for (String batchTable : batchTables) {
                batchTableNode.add(batchTable);
            }
        }

        return node;
    }

    private JsonNode deserializeByteArray(byte[] data) {
        try {
            MappingJsonFactory JSON_FACTORY_NON_STD = new MappingJsonFactory();
            JSON_FACTORY_NON_STD.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
            JSON_FACTORY_NON_STD.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
            return JSON_FACTORY_NON_STD.createJsonParser(data).readValueAsTree();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] serializeJsonNode(JsonNode node) {
        try {
            return new ObjectMapper().writeValueAsBytes(node);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
