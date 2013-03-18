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
package com.ngdata.hbaseindexer.parse;

import com.ngdata.hbaseindexer.conf.IndexerConf;
import com.ngdata.hbaseindexer.indexer.Indexer;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.solr.common.SolrInputDocument;

public interface HBaseToSolrMapper {
    /**
     * Should return true if the given key-value would be used by the mapping.
     *
     * <p>The implementation should just look at the coordinates (row/family/column), not at the
     * type (put, delete, ...).</p>
     */
    boolean isRelevantKV(KeyValue kv);

    /**
     * Creates the Get object used to retrieve the row from HBase with the data needed by this mapper.
     *
     * <p>In its simplest case, this could just return "new Get(row)", but it could be further tuned to
     * only read column families or columns that will be used by the mapping.</p>
     *
     * <p>This call only applies to row-based indexing ({@link IndexerConf.MappingType#ROW}).</p>
     */
    Get getGet(byte[] row);
    
    /**
     * Determine if the given Result object contains sufficient information to perform indexing.
     * <p>
     * This method is needed in order to determine whether or not to re-read an updated row when performing
     * row-based indexing.
     * 
     * @param result contains all KeyValues of the updated row
     * @return true if all data required for indexing is included in the row, otherwise false
     */
    boolean containsRequiredData(Result result);

    /**
     * Creates a Solr document out of the supplied HBase row.
     *
     * <p>The document does not need to contain the ID (unique key) for Solr, this will be added by the
     * {@link Indexer}.</p>
     *
     * <p>If the mapping would result in nothing, either null or an empty SolrInputDocument can be returned.
     * TODO this is not yet implemented, also does Solr allow adding empty documents and does this have an
     * interesting semantic?</p>
     */
    SolrInputDocument map(Result result);
}
