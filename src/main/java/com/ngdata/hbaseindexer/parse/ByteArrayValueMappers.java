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

import java.math.BigDecimal;
import java.util.Collection;

import com.google.common.collect.ImmutableList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Contains factory methods for {@link ByteArrayValueMapper}s.
 */
public class ByteArrayValueMappers {

    private static Log log = LogFactory.getLog(ByteArrayValueMappers.class);

    private static final ByteArrayValueMapper INT_MAPPER = new AbstractByteValueMapper(int.class) {

        @Override
        protected Object mapInternal(byte[] input) {
            return Bytes.toInt(input);
        }
    };

    private static final ByteArrayValueMapper LONG_MAPPER = new AbstractByteValueMapper(long.class) {

        @Override
        protected Object mapInternal(byte[] input) {
            return Bytes.toLong(input);
        }
    };

    private static final ByteArrayValueMapper STRING_MAPPER = new AbstractByteValueMapper(String.class) {

        @Override
        protected Object mapInternal(byte[] input) {
            return Bytes.toString(input);
        }

    };

    private static final ByteArrayValueMapper BOOLEAN_MAPPER = new AbstractByteValueMapper(boolean.class) {

        @Override
        protected Object mapInternal(byte[] input) {
            return Bytes.toBoolean(input);
        }
    };

    private static final ByteArrayValueMapper FLOAT_MAPPER = new AbstractByteValueMapper(float.class) {

        @Override
        protected Object mapInternal(byte[] input) {
            return Bytes.toFloat(input);
        }
    };

    private static final ByteArrayValueMapper DOUBLE_MAPPER = new AbstractByteValueMapper(double.class) {

        @Override
        protected Object mapInternal(byte[] input) {
            return Bytes.toDouble(input);
        }
    };

    private static final ByteArrayValueMapper SHORT_MAPPER = new AbstractByteValueMapper(short.class) {

        @Override
        protected Object mapInternal(byte[] input) {
            return Bytes.toShort(input);
        }
    };

    private static final ByteArrayValueMapper BIG_DECIMAL_MAPPER = new AbstractByteValueMapper(BigDecimal.class) {

        @Override
        protected Object mapInternal(byte[] input) {
            return Bytes.toBigDecimal(input);
        }
    };

    /**
     * Get a {@link ByteArrayValueMapper} for a given type. The type can be the name of a type that is supported by
     * org.apache.hadoop.hbase.util.Bytes.toXXX (e.g. long, int, double), or it can be the name of a class that
     * implements the {@link ByteArrayValueMapper} interface.
     * 
     * @param mapperType name of the mapper type
     * @return the requested mapper
     */
    public static ByteArrayValueMapper getMapper(String mapperType) {
        if ("int".equals(mapperType)) {
            return INT_MAPPER;
        } else if ("long".equals(mapperType)) {
            return LONG_MAPPER;
        } else if ("string".equals(mapperType)) {
            return STRING_MAPPER;
        } else if ("boolean".equals(mapperType)) {
            return BOOLEAN_MAPPER;
        } else if ("float".equals(mapperType)) {
            return FLOAT_MAPPER;
        } else if ("double".equals(mapperType)) {
            return DOUBLE_MAPPER;
        } else if ("short".equals(mapperType)) {
            return SHORT_MAPPER;
        } else if ("bigdecimal".equals(mapperType)) {
            return BIG_DECIMAL_MAPPER;
        } else {
            return instantiateCustomMapper(mapperType);
        }
    }

    private static ByteArrayValueMapper instantiateCustomMapper(String className) {
        Object obj;
        try {
            obj = Class.forName(className).newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException("Can't instantiate custom mapper class '" + className + "'", e);
        }

        if (obj instanceof ByteArrayValueMapper) {
            return (ByteArrayValueMapper)obj;
        } else {
            throw new IllegalArgumentException(obj.getClass() + " does not implement "
                    + ByteArrayValueMapper.class.getName());
        }
    }

    private static abstract class AbstractByteValueMapper implements ByteArrayValueMapper {

        private Class<?> targetType;

        public AbstractByteValueMapper(Class<?> targetType) {
            this.targetType = targetType;
        }

        protected abstract Object mapInternal(byte[] input);

        @Override
        public Collection<Object> map(byte[] input) {
            try {
                return ImmutableList.of(mapInternal(input));
            } catch (IllegalArgumentException e) {
                log.warn(
                        String.format("Error mapping byte value %s to %s", Bytes.toStringBinary(input),
                                targetType.getName()), e);
                return ImmutableList.of();
            }
        }
    }

}
