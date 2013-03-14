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

import com.google.common.base.Strings;
import com.ngdata.hbaseindexer.model.api.ActiveBatchBuildInfo;
import com.ngdata.hbaseindexer.model.api.BatchBuildInfo;
import com.ngdata.hbaseindexer.model.api.IndexerDefinition;
import com.ngdata.hbaseindexer.model.api.IndexerDefinitionNameComparator;
import com.ngdata.hbaseindexer.model.api.IndexerModel;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.joda.time.DateTime;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * CLI tool that lists the {@link IndexerDefinition}s defined in the {@link IndexerModel}.
 */
public class ListIndexersCli  extends BaseIndexCli {
    public static void main(String[] args) throws Exception {
        new ListIndexersCli().run(args);
    }

    private ListIndexersCli() {
    }

    @Override
    protected String getCmdName() {
        return "list-indexers";
    }

    @Override
    protected OptionParser setupOptionParser() {
        OptionParser parser = super.setupOptionParser();

        parser.accepts("dump", "dump the various config blobs (assumes everything is UTF-8 text)");

        return parser;
    }

    public void run(OptionSet options) throws Exception {
        super.run(options);

        List<IndexerDefinition> indexers = new ArrayList<IndexerDefinition>(model.getIndexers());
        Collections.sort(indexers, IndexerDefinitionNameComparator.INSTANCE);

        PrintStream ps = System.out;

        ps.println("Number of indexes: " + indexers.size());
        ps.println();

        for (IndexerDefinition indexer : indexers) {
            ps.println(indexer.getName());
            ps.println("  + Lifecycle state: " + indexer.getLifecycleState());
            ps.println("  + Incremental indexing state: " + indexer.getIncrementalIndexingState());
            ps.println("  + Batch indexing state: " + indexer.getBatchIndexingState());
            ps.println("  + SEP subscription ID: " + indexer.getSubscriptionId());
            ps.println("  + SEP subscription timestamp: " + new DateTime(indexer.getSubscriptionTimestamp()));
            ps.println("  + Connection type: " + indexer.getConnectionType());
            ps.println("  + Connection params:");
            if (indexer.getConnectionParams() == null) {
                ps.println("    + (none)");
            } else {
                for (Map.Entry<String, String> entry : indexer.getConnectionParams().entrySet()) {
                    ps.println("    + " + entry.getKey() + " = " + entry.getValue());
                }
            }
            ps.println("  + Indexer config:");
            printConf(indexer.getConfiguration(), 6, ps, options.has("dump"));
            ps.println("  + Batch index config:");
            printConf(indexer.getBatchIndexConfiguration(), 6, ps, options.has("dump"));
            ps.println("  + Default batch index config:");
            printConf(indexer.getDefaultBatchIndexConfiguration(), 6, ps, options.has("dump"));

            ActiveBatchBuildInfo activeBatchBuild = indexer.getActiveBatchBuildInfo();
            if (activeBatchBuild != null) {
                ps.println("  + Active batch build:");
                ps.println("    + Hadoop Job ID: " + activeBatchBuild.getJobId());
                ps.println("    + Submitted at: " + new DateTime(activeBatchBuild.getSubmitTime()).toString());
                ps.println("    + Tracking URL: " + activeBatchBuild.getTrackingUrl());
                ps.println("    + Batch build config:");
                printConf(activeBatchBuild.getBatchIndexConfiguration(), 8, ps, options.has("dump"));
            }

            BatchBuildInfo lastBatchBuild = indexer.getLastBatchBuildInfo();
            if (lastBatchBuild != null) {
                ps.println("  + Last batch build:");
                ps.println("    + Hadoop Job ID: " + lastBatchBuild.getJobId());
                ps.println("    + Submitted at: " + new DateTime(lastBatchBuild.getSubmitTime()).toString());
                ps.println("    + Success: " + successMessage(lastBatchBuild));
                ps.println("    + Job state: " + lastBatchBuild.getJobState());
                ps.println("    + Tracking URL: " + lastBatchBuild.getTrackingUrl());
                Map<String, Long> counters = lastBatchBuild.getCounters();
                ps.println("    + Map input records: " + counters.get(COUNTER_MAP_INPUT_RECORDS));
                ps.println("    + Launched map tasks: " + counters.get(COUNTER_TOTAL_LAUNCHED_MAPS));
                ps.println("    + Failed map tasks: " + counters.get(COUNTER_NUM_FAILED_MAPS));
                ps.println("    + Index failures: " + counters.get(COUNTER_NUM_FAILED_RECORDS));
                ps.println("    + Batch build config:");
                printConf(lastBatchBuild.getBatchIndexConfiguration(), 8, ps, options.has("dump"));
            }
            ps.println();
        }
    }

    private String successMessage(BatchBuildInfo buildInfo) {
        StringBuilder result = new StringBuilder();
        result.append(buildInfo.getSuccess());

        Long failedRecords = buildInfo.getCounters().get(COUNTER_NUM_FAILED_RECORDS); // TODO this was for Lily, implement new equivalent
        if (failedRecords != null && failedRecords > 0) {
            result.append(", ").append(buildInfo.getSuccess() ? "but " : "").append(failedRecords)
                    .append(" index failures");
        }

        return result.toString();
    }

    /**
     * Prints out a conf stored in a byte[], under the assumption that it is UTF-8 text.
     */
    private void printConf(byte[] conf, int indent, PrintStream ps, boolean dump) throws Exception {
        String prefix = Strings.repeat(" ", indent);
        if (conf == null) {
            ps.println(prefix + "(none)");
        } else {
            if (dump) {
                String data = new String(conf, "UTF-8");
                for (String line : data.split("\n")) {
                    ps.println(prefix + line);
                }
            } else {
                ps.println(prefix + conf.length + " bytes, use -dump to see content");
            }
        }
    }

    private static final String COUNTER_MAP_INPUT_RECORDS = "org.apache.hadoop.mapred.Task$Counter:MAP_INPUT_RECORDS";
    private static final String COUNTER_TOTAL_LAUNCHED_MAPS =
            "org.apache.hadoop.mapred.JobInProgress$Counter:TOTAL_LAUNCHED_MAPS";
    private static final String COUNTER_NUM_FAILED_MAPS =
            "org.apache.hadoop.mapred.JobInProgress$Counter:NUM_FAILED_MAPS";
    private static final String COUNTER_NUM_FAILED_RECORDS =
            "org.lilyproject.indexer.batchbuild.IndexBatchBuildCounters:NUM_FAILED_RECORDS";
}
