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
package com.ngdata.hbaseindexer.parse.extract;

import static org.junit.Assert.assertEquals;

import java.util.Collection;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;

public class ExtractTestUtil {

    public static void assertByteArraysEquals(Collection<byte[]> expected, Collection<byte[]> actual) {
        Function<byte[], String> byteToStringFn = new Function<byte[], String>() {

            @Override
            public String apply(@Nullable byte[] input) {
                return Bytes.toStringBinary(input);
            }
        };
        assertEquals(Lists.newArrayList(Collections2.transform(expected, byteToStringFn)),
                Lists.newArrayList(Collections2.transform(actual, byteToStringFn)));

    }
}
