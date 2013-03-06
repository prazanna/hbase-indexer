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
package com.ngdata.hbasesearch.conf;

import com.ngdata.hbasesearch.parse.ByteArrayValueMapper;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Defines a single field in an index definition;
 */
public class FieldDefinition {

    /**
     * Specifies where values to index should be extracted from.
     */
    public static enum ValueSource {
        /**
         * Extract values to index from the column qualifier of a {@code KeyValue}.
         */
        QUALIFIER,

        /**
         * Extract values to index from the cell value of a {@code KeyValue}.
         */
        VALUE
    };

    private final String name;

    private final String valueExpression;

    private final ValueSource valueSource;

    private final String typeName;

    public static final ValueSource DEFAULT_VALUE_SOURCE = ValueSource.VALUE;

    public static final String DEFAULT_TYPE_NAME = "string";

    public FieldDefinition(String name, String valueExpression, ValueSource valueSource, String typeName) {
        checkNotNull(name, "name");
        checkNotNull(name, "valueExpression");
        this.name = name;
        this.valueExpression = valueExpression;
        this.valueSource = valueSource != null ? valueSource : DEFAULT_VALUE_SOURCE;
        this.typeName = typeName != null ? typeName : DEFAULT_TYPE_NAME;
    }

    /**
     * Get the name of the field name to be used in a Solr index.
     * 
     * @return the Solr field name
     */
    public String getName() {
        return name;
    }

    /**
     * Get the HBase columnfamily:columnqualifier expression to be used for extracting data from HBase {@code Result}
     * objects.
     */
    public String getValueExpression() {
        return valueExpression;
    }

    /**
     * Get the source (i.e. HBase column qualifier or HBase cell) from which data is to be extracted.
     */
    public ValueSource getValueSource() {
        return valueSource;
    }

    /**
     * Get the name of the type to which extracted data is to be mapped. The name can be either a Java primitive name or
     * any other type that is supported by org.apache.hadoop.hbase.util.Bytes.toXXX, or the name of a class that
     * implements the {@link ByteArrayValueMapper} interface.
     */
    public String getTypeName() {
        return typeName;
    }

}
