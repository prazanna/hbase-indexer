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

import com.ngdata.hbaseindexer.parse.ByteArrayExtractor;
import org.apache.hadoop.hbase.util.Bytes;

import static com.ngdata.hbaseindexer.conf.FieldDefinition.ValueSource;

/**
 * Contains factory methods for {@link ByteArrayExtractor}s.
 */
public class ByteArrayExtractors {

    /**
     * Get a {@code ByteArrayExtractor} for a given field value expression and source.
     * <p>
     * The supplied fieldValueExpression can either be an absolute column family and qualifier string, such as
     * "colfam:qualifier", or the qualifier can be a wildcard prefix expression, such as "colfam:qualifi*".
     * 
     * @param fieldValueExpression a column family and column qualifier (or prefix with wildcard), separated by a colon
     * @param valueSource the source of the value to be extracted
     * @return the appropriate ByteArrayValueMapper
     */
    public static ByteArrayExtractor getExtractor(String fieldValueExpression, ValueSource valueSource) {
        byte[] columnFamily = Bytes.toBytes(getFamily(fieldValueExpression));
        String qualifierString = getQualifier(fieldValueExpression);
        boolean wildcard = isWildcard(qualifierString);
        if (wildcard) {
            qualifierString = qualifierString.substring(0, qualifierString.length() - 1);
        }
        byte[] qualifier = Bytes.toBytes(qualifierString);

        if (valueSource == ValueSource.VALUE) {
            if (wildcard) {
                return new PrefixMatchingCellExtractor(columnFamily, qualifier);
            } else {
                return new SingleCellExtractor(columnFamily, qualifier);
            }
        } else {
            if (wildcard) {
                return new PrefixMatchingQualifierExtractor(columnFamily, qualifier);
            } else {
                throw new IllegalArgumentException("Can't create a non-prefix-based qualifier extrator");
            }
        }
    }

    private static String[] splitFamilyAndQualifier(String fieldValueExpression) {
        String[] splits = fieldValueExpression.split(":", 2);
        if (splits.length != 2) {
            throw new IllegalArgumentException("Invalid field value expression: " + fieldValueExpression);
        }
        return splits;
    }

    /**
     * Extract the column family from a columnFamily:qualifier expression.
     */
    static String getFamily(String valueExpression) {
        return splitFamilyAndQualifier(valueExpression)[0];
    }

    /**
     * Extract the qualifier from a columnFamily:qualifier expression.
     */
    static String getQualifier(String valueExpression) {
        return splitFamilyAndQualifier(valueExpression)[1];
    }

    static boolean isWildcard(String columnQualifierExpression) {
        return columnQualifierExpression.endsWith("*");
    }

}
