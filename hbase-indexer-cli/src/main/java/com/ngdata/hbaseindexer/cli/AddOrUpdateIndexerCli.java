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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.ngdata.hbaseindexer.SolrConnectionParams;
import com.ngdata.hbaseindexer.conf.IndexerConfException;
import com.ngdata.hbaseindexer.conf.XmlIndexerConfReader;
import com.ngdata.hbaseindexer.model.api.IndexerDefinition;
import com.ngdata.hbaseindexer.model.api.IndexerDefinitionBuilder;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.ValueConversionException;
import joptsimple.ValueConverter;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;

import com.ngdata.hbaseindexer.util.IndexerNameValidator;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Map;

import static com.ngdata.hbaseindexer.model.api.IndexerDefinition.BatchIndexingState;
import static com.ngdata.hbaseindexer.model.api.IndexerDefinition.IncrementalIndexingState;
import static com.ngdata.hbaseindexer.model.api.IndexerDefinition.LifecycleState;

/**
 * Base class for the {@link AddIndexerCli} and {@link UpdateIndexerCli}.
 */
public abstract class AddOrUpdateIndexerCli extends BaseIndexCli {
    protected OptionSpec<String> nameOption;
    protected ArgumentAcceptingOptionSpec<String> indexerConfOption;
    protected OptionSpec<Pair<String, String>> connectionParamOption;
    protected OptionSpec<IndexerDefinition.LifecycleState> lifecycleStateOption;
    protected OptionSpec<IndexerDefinition.IncrementalIndexingState> incrementalIdxStateOption;
    protected OptionSpec<IndexerDefinition.BatchIndexingState> batchIdxStateOption;
    protected OptionSpec<String> defaultBatchIndexConfOption;
    protected OptionSpec<String> batchIndexConfOption;

    @Override
    protected OptionParser setupOptionParser() {
        OptionParser parser = super.setupOptionParser();

        nameOption = parser
                .acceptsAll(Lists.newArrayList("n", "name"), "a name for the index")
                .withRequiredArg().ofType(String.class)
                .required();

        indexerConfOption = parser
                .acceptsAll(Lists.newArrayList("c", "indexer-conf"), "Indexer configuration")
                .withRequiredArg().ofType(String.class).describedAs("indexerconf.xml");

        connectionParamOption = parser
                        .acceptsAll(Lists.newArrayList("cp", "connection-param"),
                                "A connection parameter in the form key=value. This option can be specified multiple"
                                + " times. Example: -cp solr.zk=host1,host2 -cp solr.collection=products. In case"
                                + " of update, use an empty value to remove a key: -cp solr.collection=")
                        .withRequiredArg()
                        .withValuesConvertedBy(new StringPairConverter())
                        .describedAs("key=value");

        lifecycleStateOption = parser
                        .acceptsAll(Lists.newArrayList("lifecycle"), "Lifecycle state, one of " +
                                LifecycleState.ACTIVE + ", " + LifecycleState.DELETE_REQUESTED)
                        .withRequiredArg()
                        .withValuesConvertedBy(new EnumConverter<LifecycleState>(LifecycleState.class))
                        .defaultsTo(LifecycleState.DEFAULT)
                        .describedAs("state");

        incrementalIdxStateOption = parser
                        .acceptsAll(Lists.newArrayList("incremental"), "Incremental indexing state, one of "
                                + IncrementalIndexingState.SUBSCRIBE_AND_CONSUME
                                + ", " + IncrementalIndexingState.SUBSCRIBE_DO_NOT_CONSUME
                                + ", " + IncrementalIndexingState.DO_NOT_SUBSCRIBE)
                        .withRequiredArg()
                        .withValuesConvertedBy(new EnumConverter<IncrementalIndexingState>(IncrementalIndexingState.class))
                        .defaultsTo(IncrementalIndexingState.DEFAULT)
                        .describedAs("state");

        batchIdxStateOption = parser
                        .acceptsAll(Lists.newArrayList("batch"), "Batch indexing state, can only be set to  " +
                                BatchIndexingState.BUILD_REQUESTED)
                        .withRequiredArg()
                        .withValuesConvertedBy(new EnumConverter<BatchIndexingState>(BatchIndexingState.class))
                        .defaultsTo(BatchIndexingState.DEFAULT)
                        .describedAs("state");

        defaultBatchIndexConfOption = parser
                        .acceptsAll(Lists.newArrayList("dbi", "default-batch-conf"),
                                "Default batch indexing settings for this indexer. On update, use this option without"
                                + " filename argument to remove the config.")
                        .withOptionalArg().ofType(String.class).describedAs("batchconf.xml");

        batchIndexConfOption = parser
                        .acceptsAll(Lists.newArrayList("bi", "batch-conf"),
                                "Batch indexing settings to use for the next batch index build triggered, this overrides"
                                + " the default batch index configuration (if any). On update, use this option without"
                                + " filename argument to remove the config.")
                        .withOptionalArg().ofType(String.class).describedAs("batchconf.xml");

        return parser;
    }

    /**
     * Builds an {@link IndexerDefinition} based on the CLI options provided, an optionally starting from
     * an initial state.
     */
    protected IndexerDefinitionBuilder buildIndexerDefinition(OptionSet options, IndexerDefinition oldIndexerDef)
            throws IOException {

        IndexerDefinitionBuilder builder = new IndexerDefinitionBuilder();
        if (oldIndexerDef != null)
            builder.startFrom(oldIndexerDef);

        // name option is always required, so don't need to check for nulliness
        String indexerName = nameOption.value(options);
        IndexerNameValidator.validate(indexerName);
        builder.name(indexerName);

        LifecycleState lifecycleState = lifecycleStateOption.value(options);
        if (lifecycleState != null)
            builder.lifecycleState(lifecycleState);

        IncrementalIndexingState incrementalIdxState = incrementalIdxStateOption.value(options);
        if (incrementalIdxState != null)
            builder.incrementalIndexingState(incrementalIdxState);

        BatchIndexingState batchIdxState = batchIdxStateOption.value(options);
        if (batchIdxState != null)
            builder.batchIndexingState(batchIdxState);

        // connection type is a hardcoded setting
        builder.connectionType("solr");

        Map<String, String> connectionParams = getConnectionParams(options,
                oldIndexerDef != null ? oldIndexerDef.getConnectionParams() : null);
        if (connectionParams != null)
            builder.connectionParams(connectionParams);

        byte[] indexerConf = getIndexerConf(options, indexerConfOption);
        if (indexerConf != null)
            builder.configuration(indexerConf);

        byte[] defaultBatchIndexConf = getBatchIndexingConf(options, defaultBatchIndexConfOption);
        if (defaultBatchIndexConf != null) {
            if (defaultBatchIndexConf.length == 0) {
                builder.defaultBatchIndexConfiguration(null);
            } else {
                builder.defaultBatchIndexConfiguration(defaultBatchIndexConf);
            }
        }

        byte[] batchIndexConf = getBatchIndexingConf(options, batchIndexConfOption);
        if (batchIndexConf != null) {
            if (batchIndexConf.length == 0) {
                builder.batchIndexConfiguration(null);
            } else {
                builder.batchIndexConfiguration(batchIndexConf);
            }
        }

        return builder;
    }

    protected byte[] getIndexerConf(OptionSet options, OptionSpec<String> configOption) throws IOException {
        String fileName = configOption.value(options);
        if (fileName == null) {
            return null;
        }

        File file = new File(fileName);
        if (!file.exists()) {
            StringBuilder msg = new StringBuilder();
            msg.append("Specified indexer configuration file not found:\n");
            msg.append(file.getAbsolutePath());
            throw new CliException(msg.toString());
        }

        byte[] data = ByteStreams.toByteArray(Files.newInputStreamSupplier(file));
        try {
            new XmlIndexerConfReader().validate(new ByteArrayInputStream(data));
        } catch (IndexerConfException e) {
            StringBuilder msg = new StringBuilder();
            msg.append("Failed to parse file ").append(fileName).append('\n');
            addExceptionMessages(e, msg);
            throw new CliException(msg.toString());
        } catch (SAXException e) {
            StringBuilder msg = new StringBuilder();
            msg.append("Failed to parse file ").append(fileName).append('\n');
            addExceptionMessages(e, msg);
            throw new CliException(msg.toString());
        } catch (ParserConfigurationException e) {
            throw new RuntimeException(e);
        }
        return data;
    }

    private void addExceptionMessages(Throwable throwable, StringBuilder builder) {
        Throwable cause = throwable;
        while (cause != null) {
            builder.append(cause.getMessage()).append('\n');
            cause = cause.getCause();
        }
    }

    private Map<String, String> getConnectionParams(OptionSet options, Map<String, String> oldParams) {
        Map<String, String> connectionParams = Maps.newHashMap();
        if (oldParams != null)
            connectionParams = Maps.newHashMap(oldParams);

        for (Pair<String, String> param : connectionParamOption.values(options)) {
            // An empty value indicates a request to remove the key
            if (param.getSecond().length() == 0) {
                connectionParams.remove(param.getFirst());
            } else {
                if (!isValidConnectionParam(param.getFirst())) {
                    System.err.println("WARNING: the following is not a recognized Solr connection parameter: "
                            + param.getFirst());
                }
                connectionParams.put(param.getFirst(), param.getSecond());
            }
        }

        // Validate that the minimum required connection params are present

        if (!connectionParams.containsKey(SolrConnectionParams.ZOOKEEPER)) {
            String solrZk = getZkConnectionString() + "/solr";
            System.err.println("WARNING: no -cp solr.zk specified, will use " + solrZk);
            connectionParams.put("solr.zk", solrZk);
        }

        if (!connectionParams.containsKey(SolrConnectionParams.COLLECTION)) {
            throw new CliException("ERROR: no -cp solr.collection=collectionName specified");
        }

        if (connectionParams.containsKey(SolrConnectionParams.MODE)
                && !connectionParams.get(SolrConnectionParams.MODE).equals("cloud")) {
            System.err.println("ERROR: only 'cloud' supported for -cp solr.mode");
        }

        return connectionParams;
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

    /**
     * Returns a zero-length byte array in case the configuration should be removed.
     */
    protected byte[] getBatchIndexingConf(OptionSet options, OptionSpec<String> option) throws IOException {
        String fileName = option.value(options);
        if (fileName == null) {
            return new byte[0];
        }

        File file = new File(fileName);
        if (!file.exists()) {
            StringBuilder msg = new StringBuilder();
            msg.append("Specified batch indexing configuration file not found:\n");
            msg.append(file.getAbsolutePath());
            throw new CliException(msg.toString());
        }

        return FileUtils.readFileToByteArray(file);
    }

    /**
     * Converter for jopt-simple that parses key=value pairs.
     */
    private static class StringPairConverter implements ValueConverter<Pair<String, String>> {
        @Override
        public Pair<String, String> convert(String input) {
            int eqPos = input.indexOf('=');
            if (eqPos == -1) {
                throw new ValueConversionException("Parameter should be in the form key=value, which the " +
                        "following is not: '" + input + "'.");
            }
            String key = input.substring(0, eqPos).trim();
            String value = input.substring(eqPos + 1).trim();
            return Pair.newPair(key, value);
        }

        @SuppressWarnings("unchecked")
        @Override
        public Class<Pair<String, String>> valueType() {
            Class<?> pairClass = Pair.class;
            return (Class<Pair<String, String>>)pairClass;
        }

        @Override
        public String valuePattern() {
            return "key=value";
        }
    }

    private static class EnumConverter<T extends Enum<T>> implements ValueConverter<T> {
        Class<T> enumClass;

        EnumConverter(Class<T> enumClass) {
            this.enumClass = enumClass;
        }

        @Override
        public T convert(String input) {
            try {
                return Enum.valueOf(enumClass, input.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new ValueConversionException("Unrecognized value for enum " + enumClass.getSimpleName()
                        + ": '" + input + "'.");
            }
        }

        @Override
        public Class<T> valueType() {
            return enumClass;
        }

        @Override
        public String valuePattern() {
            return null;
        }
    }
}
