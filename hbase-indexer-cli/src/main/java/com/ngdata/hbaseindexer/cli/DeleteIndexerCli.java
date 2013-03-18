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
import com.ngdata.hbaseindexer.model.api.IndexerDefinition;
import com.ngdata.hbaseindexer.model.api.IndexerDefinition.LifecycleState;
import com.ngdata.hbaseindexer.model.api.IndexerDefinitionBuilder;
import com.ngdata.hbaseindexer.model.api.IndexerNotFoundException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class DeleteIndexerCli extends BaseIndexCli {

    private OptionSpec<String> nameOption;

    public static void main(String[] args) throws Exception {
        new DeleteIndexerCli().run(args);
    }

    @Override
    protected String getCmdName() {
        return "delete-indexer";
    }

    @Override
    protected OptionParser setupOptionParser() {
        OptionParser parser = super.setupOptionParser();

        nameOption = parser.acceptsAll(Lists.newArrayList("n", "name"), "a name for the index").withRequiredArg().ofType(
                String.class).required();

        return parser;
    }

    private void waitForDeletion(String indexerName) throws InterruptedException {
        System.out.printf("Deleting indexer '%s'", indexerName);
        while (model.hasIndexer(indexerName)) {
            IndexerDefinition indexerDef = null;
            try {
                indexerDef = model.getIndexer(indexerName);
            } catch (IndexerNotFoundException e) {
                // The indexer was deleted between the call to hasIndexer and getIndexer, that's ok
                break;
            }

            switch (indexerDef.getLifecycleState()) {
            case DELETE_FAILED:
                System.err.println("\nDelete failed");
                return;
            case DELETE_REQUESTED:
            case DELETING:
                System.out.print(".");
                Thread.sleep(500);
                continue;
            default:
                throw new IllegalStateException("Illegal lifecycle state while deleting: "
                        + indexerDef.getLifecycleState());
            }
        }
        System.out.printf("\nDeleted indexer '%s'\n", indexerName);
    }

    @Override
    protected void run(OptionSet options) throws Exception {
        super.run(options);

        String indexerName = nameOption.value(options);

        if (!model.hasIndexer(indexerName)) {
            throw new CliException("Indexer does not exist: " + indexerName);
        }

        IndexerDefinition indexerDef = model.getIndexer(indexerName);

        if (indexerDef.getLifecycleState() == LifecycleState.DELETE_REQUESTED
                || indexerDef.getLifecycleState() == LifecycleState.DELETING) {
            System.err.printf("Delete of '%s' is already in progress\n", indexerName);
            return;
        }

        IndexerDefinitionBuilder builder = new IndexerDefinitionBuilder();
        builder.startFrom(indexerDef);
        builder.lifecycleState(LifecycleState.DELETE_REQUESTED);

        model.updateIndexerInternal(builder.build());

        waitForDeletion(indexerName);

    }

}
