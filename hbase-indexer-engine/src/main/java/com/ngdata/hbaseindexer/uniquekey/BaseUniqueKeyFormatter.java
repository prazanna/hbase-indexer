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

import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.KeyValue;

public abstract class BaseUniqueKeyFormatter implements UniqueKeyFormatter {

    private static final char SEPARATOR = '-';
    private static final Splitter SPLITTER = Splitter.onPattern("(?<!\\\\)-");
    private static final Joiner JOINER = Joiner.on(SEPARATOR);
    
    /**
     * Encode a byte array as a String in an implementation-specific encoding.
     * 
     * @param bytes bytes to be encoded
     * @return encoded String
     */
    protected abstract String encodeAsString(byte[] bytes);

    /**
     * Decode a String from a byte array in an implementation-specific decoding.
     * 
     * @param value value to be decoded
     * @return decoded byte array
     */
    protected abstract byte[] decodeFromString(String value);

    @Override
    public String formatRow(byte[] row) {
        Preconditions.checkNotNull(row, "row");
        return encodeAsString(row);
    }
    
    @Override
    public String formatFamily(byte[] family) {
        Preconditions.checkNotNull(family, "family");
        return encodeAsString(family);
    }

    @Override
    public String formatKeyValue(KeyValue keyValue) {
        return JOINER.join(encodeAsString(keyValue.getRow()), encodeAsString(keyValue.getFamily()),
                encodeAsString(keyValue.getQualifier()));
    }

    @Override
    public byte[] unformatRow(String keyString) {
        return decodeFromString(keyString);
    }
    
    @Override
    public byte[] unformatFamily(String familyString) {
        return decodeFromString(familyString);
    }

    @Override
    public KeyValue unformatKeyValue(String keyValueString) {
        List<String> parts = Lists.newArrayList(SPLITTER.split(keyValueString));
        if (parts.size() != 3) {
            throw new IllegalArgumentException("Value cannot be split into row, column family, qualifier: "
                    + keyValueString);
        }
        byte[] rowKey = decodeFromString(parts.get(0));
        byte[] columnFamily = decodeFromString(parts.get(1));
        byte[] columnQualifier = decodeFromString(parts.get(2));
        return new KeyValue(rowKey, columnFamily, columnQualifier);
    }

}
