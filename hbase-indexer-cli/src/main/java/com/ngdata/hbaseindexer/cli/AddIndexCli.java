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
package com.ngdata.hbaseindexer.cli;

import com.google.common.collect.Maps;
import com.ngdata.hbaseindexer.SolrConnectionParams;
import com.ngdata.hbaseindexer.model.api.IndexerDefinitionBuilder;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.hadoop.hbase.util.Pair;

import java.util.Map;

/**
 * CLI tool for adding new indexes.
 */
public class AddIndexCli extends BaseIndexCli {
    private OptionSpec<String> nameOption;
    private OptionSpec<String> indexConfOption;
    private OptionSpec<Pair<String, String>> connectionParamOption;

    public static void main(String[] args) throws Exception {
        new AddIndexCli().run(args);
    }

    public void run(OptionSet options) throws Exception {

        super.run(options);

        Map<String, String> connectionParams = Maps.newHashMap();
        for (Pair<String, String> param : connectionParamOption.values(options)) {
            if (!isValidConnectionParam(param.getFirst())) {
                System.err.println("WARNING: the following is not a recognized Solr connection parameter: "
                        + param.getFirst());
            }
            connectionParams.put(param.getFirst(), param.getSecond());
        }

        if (!connectionParams.containsKey(SolrConnectionParams.ZOOKEEPER)) {
            String solrZk = getZkConnectionString() + "/solr";
            System.err.println("WARNING: no -cp solr.zk specified, will use " + solrZk);
            connectionParams.put("solr.zk", solrZk);
        }

        if (!connectionParams.containsKey(SolrConnectionParams.COLLECTION)) {
            System.err.println("ERROR: no -cp solr.collection=collectionName specified");
            System.exit(1);
        }

        if (connectionParams.containsKey(SolrConnectionParams.MODE)
                && !connectionParams.get(SolrConnectionParams.MODE).equals("cloud")) {
            System.err.println("ERROR: only 'cloud' supported for -cp solr.mode");
        }

        IndexerDefinitionBuilder builder = new IndexerDefinitionBuilder()
                .name(nameOption.value(options))
                .configuration(getIndexerConf(options, indexConfOption))
                .connectionType("solr")
                .connectionParams(connectionParams);

        model.addIndexer(builder.build());

        System.out.println("Index added");
    }

    private boolean isValidConnectionParam(String param) {
        if (SolrConnectionParams.MODE.equals(param)) {
            return true;
        } else if (SolrConnectionParams.ZOOKEEPER.equals(param)) {
            return true;
        } else if (SolrConnectionParams.COLLECTION.equals(param)) {
            return true;
        }

        return false;
    }

    @Override
    protected OptionParser setupOptionParser() {
        OptionParser parser = super.setupOptionParser();

        nameOption = addNameOption(parser).required();
        indexConfOption = addIndexConfOption(parser).required();
        connectionParamOption = addConnectionParamOption(parser);

        return parser;
    }

    @Override
    protected String getCmdName() {
        return "add-index";
    }
}
