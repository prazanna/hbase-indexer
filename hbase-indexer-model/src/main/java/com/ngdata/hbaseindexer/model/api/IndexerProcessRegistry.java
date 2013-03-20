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

import java.util.List;

/**
 * Registry of all current indexer processes, including their possible error status.
 */
public interface IndexerProcessRegistry {

    /**
     * Register an indexer process for a specific host.
     * 
     * @param indexerName name of the indexer
     * @param hostName name of the host where the indexer is running
     * @return identifier for the registered process
     */
    String registerIndexerProcess(String indexerName, String hostName);

    /**
     * Set an error status for an indexer process.
     * 
     * @param indexerProcessId identifier of the indexer process
     * @param error error that prevents the indexer process from running
     */
    void setErrorStatus(String indexerProcessId, Throwable error);

    /**
     * Unregister an indexer process. This method has no effect on the
     * indexer process itself, it only unregisters it from the registry.
     * 
     * @param indexerProcessId identifier of the indexer process
     */
    void unregisterIndexerProcess(String indexerProcessId);
    
    /**
     * Get the list of currently instantiated indexer processes for a given indexer.
     * @param indexerName name of the indexer for which processes are to be fetched
     */
    public List<IndexerProcess> getIndexerProcesses(String indexerName);

}