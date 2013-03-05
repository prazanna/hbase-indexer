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
package com.ngdata.hbasesearch.parse.extract;

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.ngdata.hbasesearch.parse.ByteArrayExtractor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Abstract base class for prefix-matching {@link ByteArrayExtractor}s.
 */
public abstract class AbstractPrefixMatchingExtractor implements ByteArrayExtractor {

    private byte[] columnFamily;
    private byte[] prefix;

    AbstractPrefixMatchingExtractor(byte[] columnFamily, byte[] prefix) {
        this.columnFamily = columnFamily;
        this.prefix = prefix;
    }

    @VisibleForTesting
    byte[] getPrefix() {
        return prefix;
    }

    /**
     * Extract a value from a {@code KeyValue}.
     * 
     * @param keyValue source for the extraction
     * @return extracted value, or null if no value can be extracted
     */
    protected abstract byte[] extract(byte[] qualifier, byte[] value);

    @Override
    public Collection<byte[]> extract(Result result) {
        List<byte[]> values = Lists.newArrayList();
        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(columnFamily);
        if (familyMap != null) {
            // TODO Make more efficient use of built-in functionality of NavigableMap
            for (Entry<byte[], byte[]> entry : familyMap.entrySet()) {
                if (Bytes.startsWith(entry.getKey(), prefix)) {
                    values.add(extract(entry.getKey(), entry.getValue()));
                }
            }
        }
        return values;
    }
}
