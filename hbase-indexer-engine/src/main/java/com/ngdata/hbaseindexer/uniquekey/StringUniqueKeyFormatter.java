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

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Performs encoding and decoding to and from simple strings representations of byte arrays.
 */
public class StringUniqueKeyFormatter extends BaseUniqueKeyFormatter implements UniqueKeyFormatter {

    private static final HyphenEscapingUniqueKeyFormatter hyphenEscapingFormatter = new HyphenEscapingUniqueKeyFormatter();

    @Override
    public String formatKeyValue(KeyValue keyValue) {
        return hyphenEscapingFormatter.formatKeyValue(keyValue);
    }

    @Override
    public KeyValue unformatKeyValue(String keyValueString) {
        return hyphenEscapingFormatter.unformatKeyValue(keyValueString);
    }

    @Override
    protected String encodeAsString(byte[] bytes) {
        return Bytes.toString(bytes);
    }

    @Override
    protected byte[] decodeFromString(String value) {
        return Bytes.toBytes(value);
    }

    private static class HyphenEscapingUniqueKeyFormatter  extends BaseUniqueKeyFormatter {
        @Override
        protected String encodeAsString(byte[] bytes) {
            String encoded = Bytes.toString(bytes);
            if (encoded.indexOf('-') > -1) {
                encoded = encoded.replace("-", "\\-");
            }
            return encoded;
        }
        
        @Override
        protected byte[] decodeFromString(String value) {
            if (value.contains("\\-")) {
                value = value.replace("\\-", "-");
            }
            return Bytes.toBytes(value);
        }

    }

}
