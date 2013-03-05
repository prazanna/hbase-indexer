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
package com.ngdata.hbasesearch.parse;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Collections;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class ByteArrayValueMappersTest {

    @Test
    public void testGetValueMapper_Int() {
        ByteArrayValueMapper mapper = ByteArrayValueMappers.getMapper("int");
        assertEquals(Lists.newArrayList(42), mapper.map(Bytes.toBytes(42)));
    }

    @Test
    public void testMapValue_InvalidEncoding() {
        ByteArrayValueMapper mapper = ByteArrayValueMappers.getMapper("long");
        assertEquals(Collections.emptyList(), mapper.map(Bytes.toBytes(42)));
    }

    @Test
    public void testGetValueMapper_Long() {
        ByteArrayValueMapper mapper = ByteArrayValueMappers.getMapper("long");
        assertEquals(Lists.newArrayList(Long.MAX_VALUE), mapper.map(Bytes.toBytes(Long.MAX_VALUE)));
    }

    @Test
    public void testGetValueMapper_String() {
        ByteArrayValueMapper mapper = ByteArrayValueMappers.getMapper("string");
        assertEquals(Lists.newArrayList("forty-two"), mapper.map(Bytes.toBytes("forty-two")));
    }

    @Test
    public void testGetValueMapper_Boolean() {
        ByteArrayValueMapper mapper = ByteArrayValueMappers.getMapper("boolean");
        assertEquals(Lists.newArrayList(true), mapper.map(Bytes.toBytes(true)));
    }

    @Test
    public void testGetValueMapper_Float() {
        ByteArrayValueMapper mapper = ByteArrayValueMappers.getMapper("float");
        assertEquals(Lists.newArrayList(4.2f), mapper.map(Bytes.toBytes(4.2f)));
    }

    @Test
    public void testGetValueMapper_Double() {
        ByteArrayValueMapper mapper = ByteArrayValueMappers.getMapper("double");
        assertEquals(Lists.newArrayList(Math.PI), mapper.map(Bytes.toBytes(Math.PI)));
    }

    @Test
    public void testGetValueMapper_Short() {
        ByteArrayValueMapper mapper = ByteArrayValueMappers.getMapper("short");
        assertEquals(Lists.newArrayList((short)42), mapper.map(Bytes.toBytes((short)42)));
    }

    @Test
    public void testGetValueMapper_BigDecimal() {
        ByteArrayValueMapper mapper = ByteArrayValueMappers.getMapper("bigdecimal");
        assertEquals(Lists.newArrayList(new BigDecimal("1.234")), mapper.map(Bytes.toBytes(new BigDecimal("1.234"))));
    }

    @Test
    public void testGetValueMapper_CustomMapperClass() {
        ByteArrayValueMapper mapper = ByteArrayValueMappers.getMapper(MockValueMapper.class.getName());
        assertEquals(Lists.newArrayList("A", "B", "C"), mapper.map("dummy value".getBytes()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetValueMapper_CustomMapperClass_NotMapperImplementation() {
        ByteArrayValueMappers.getMapper(String.class.getName());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetValueMapper_InvalidName() {
        ByteArrayValueMappers.getMapper("not.a.classname.or.primitive.Name");
    }

    public static class MockValueMapper implements ByteArrayValueMapper {
        @Override
        public Collection<Object> map(byte[] input) {
            return Lists.<Object> newArrayList("A", "B", "C");
        }
    }

}
