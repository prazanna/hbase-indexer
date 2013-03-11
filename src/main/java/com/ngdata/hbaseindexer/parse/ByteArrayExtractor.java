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

import java.util.Collection;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

/**
 * Extracts a collection of byte-array values from a Result object.
 */
public interface ByteArrayExtractor {

    /**
     * Extract the byte-array values from a {@code Result} object.
     * 
     * @param result source of the extraction
     * @return extracted values, can be an empty collection
     */
    Collection<byte[]> extract(Result result);
    
    
    /**
     * Determine if the given {@code Result} contains all target information required by this extractor.
     * <p>
     * This method is required in order to perform optimizations in terms of re-reading rows during row-based indexing.
     * 
     * @param result the {@code Result} object to be checked for inclusion of target information
     * @return true if all cases of this extractor can be satisfied by the data contained in the given result, otherwise false
     */
    boolean containsTarget(Result result);

    /**
     * Returns the column family used by this extractor.
     * <p>
     * If there are multiple column families used, or the choice of column family is dynamic, this method should return
     * null.
     * 
     * @return the column family, or null if a single complete value cannot be returned
     */
    byte[] getColumnFamily();

    /**
     * Returns the column qualifier used by this extractor.
     * <p>
     * If there are multiple column qualifiers used, or dynamic matching is performed on column qualifier by this
     * extractor, this method should return null.
     * 
     * @return the column qualifier, or null if a single complete value cannot be returned
     */
    byte[] getColumnQualifier();
    
    /**
     * Determine if this extractor can be applied to a given KeyValue.
     * <p>
     * This functionality is used to determine if an individual KeyValue should be used for extraction.
     * 
     * @param keyValue the {@code KeyValue} to be checked for applicability
     * @return true if the given {@code KeyValue} is applicable, otherwise false
     */
    boolean isApplicable(KeyValue keyValue);

}
