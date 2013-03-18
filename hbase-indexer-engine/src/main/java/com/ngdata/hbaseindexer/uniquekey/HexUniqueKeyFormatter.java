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

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

public class HexUniqueKeyFormatter extends BaseUniqueKeyFormatter implements UniqueKeyFormatter {

    @Override
    protected String encodeAsString(byte[] bytes) {
        return Hex.encodeHexString(bytes);
    }

    @Override
    protected byte[] decodeFromString(String value) {
        try {
            return Hex.decodeHex(value.toCharArray());
        } catch (DecoderException e) {
            throw new IllegalArgumentException("Value '" + value + "' can't be decoded as hex", e);
        }
    }
}
