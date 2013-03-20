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
package com.ngdata.hbaseindexer.uniquekey;

import com.ngdata.hbaseindexer.conf.IndexerConf;
import org.apache.hadoop.hbase.KeyValue;

/**
 * Format row keys or {@code KeyValue}s into a human-readable form. The reverse
 * encoding is also provided.
 */
public interface UniqueKeyFormatter {
    /**
     * Format a row key into a human-readable form.
     * 
     * @param row row key to be formatted
     */
    String formatRow(byte[] row);
    
    /**
     * Format a column family value into a human-readable form.
     * <p>
     * Called as part of column-based mapping, {@link IndexerConf.MappingType#COLUMN}.
     * 
     * @param family family bytes to be formatted
     */
    String formatFamily(byte[] family);

    /**
     * Format a {@code KeyValue} into a human-readable form. Only the row, column family, and qualifier
     * of the {@code KeyValue} will be encoded.
     * <p>
     * Called in case of column-based mapping, {@link IndexerConf.MappingType#COLUMN}.
     * 
     * @param keyValue value to be formatted
     */
    String formatKeyValue(KeyValue keyValue);
    
    /**
     * Perform the reverse formatting of a row key.
     * 
     * @param keyString the formatted row key
     * @return the unformatted row key
     */
    byte[] unformatRow(String keyString);
    
    /**
     * Perform the reverse formatting of a column family value.
     * 
     * @param familyString the formatted column family string
     * @return the unformatted column family value
     */
    byte[] unformatFamily(String familyString);
    
    /**
     * Perform the reverse formatting of a {@code KeyValue}.
     * <p>
     * The returned KeyValue will only have the row key, column family, and column qualifier filled in.
     * 
     * @param keyValueString the formatted {@code KeyValue}
     * @return the unformatted {@code KeyValue}
     */
    KeyValue unformatKeyValue(String keyValueString);
}
