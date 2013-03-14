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

import com.ngdata.hbaseindexer.model.api.IndexerDefinition;
import com.ngdata.hbaseindexer.model.api.IndexerDefinitionBuilder;
import com.ngdata.hbaseindexer.model.api.IndexerModel;
import joptsimple.OptionSet;

import static com.ngdata.hbaseindexer.model.api.IndexerDefinition.LifecycleState;

/**
 * CLI tool to update an existing {@link IndexerDefinition}s in the {@link IndexerModel}.
 */
public class UpdateIndexerCli extends AddOrUpdateIndexerCli {

    public static void main(String[] args) throws Exception {
        new UpdateIndexerCli().run(args);
    }

    @Override
    protected String getCmdName() {
        return "update-indexer";
    }

    public void run(OptionSet options) throws Exception {
        super.run(options);

        String indexName = nameOption.value(options);

        if (!model.hasIndexer(indexName)) {
            throw new CliException("Indexer does not exist: " + indexName);
        }

        IndexerDefinition newIndexer = null;
        String lock = model.lockIndexer(indexName);
        try {
            IndexerDefinition indexer = model.getFreshIndexer(indexName);

            IndexerDefinitionBuilder builder = buildIndexerDefinition(options, indexer);
            newIndexer = builder.build();

            if (newIndexer.equals(indexer)) {
                System.out.println("Index already matches the specified settings, did not update it.");
            } else {
                model.updateIndexer(newIndexer, lock);
                System.out.println("Index updated: " + indexName);
            }
        } finally {
            // In case we requested deletion of an index, it might be that the lock is already removed
            // by the time we get here as part of the index deletion.
            boolean ignoreMissing = newIndexer != null
                    && newIndexer.getLifecycleState() == LifecycleState.DELETE_REQUESTED;
            model.unlockIndexer(lock, ignoreMissing);
        }
    }
}