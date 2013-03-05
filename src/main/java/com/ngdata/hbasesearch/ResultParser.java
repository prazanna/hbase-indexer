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
package com.ngdata.hbasesearch;

import java.util.List;

import com.ngdata.hbasesearch.parse.ByteArrayExtractor;
import com.ngdata.hbasesearch.parse.ByteArrayValueMapper;
import com.ngdata.hbasesearch.parse.ByteArrayValueMappers;
import com.ngdata.hbasesearch.parse.IndexValueTransformer;
import com.ngdata.hbasesearch.parse.ResultIndexValueTransformer;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.ngdata.hbasesearch.parse.extract.ByteArrayExtractors;
import org.apache.hadoop.hbase.client.Result;

/**
 * Parses HBase {@code Result} objects into a structure of fields and values.
 */
public class ResultParser {

    /**
     * Map of Solr field names to transformers for extracting data from HBase {@code Result} objects.
     */
    private List<IndexValueTransformer<Result>> valueTransformers;

    /**
     * Instantiate based on a list of {@link FieldDefinition}s.
     */
    public static ResultParser newInstance(List<FieldDefinition> fieldDefinitions) {
        List<IndexValueTransformer<Result>> valueTransformers = Lists.newArrayList();
        for (FieldDefinition fieldDefinition : fieldDefinitions) {
            ByteArrayExtractor byteArrayExtractor = ByteArrayExtractors.getExtractor(
                    fieldDefinition.getValueExpression(), fieldDefinition.getValueSource());
            ByteArrayValueMapper valueMapper = ByteArrayValueMappers.getMapper(fieldDefinition.getTypeName());
            valueTransformers.add(new ResultIndexValueTransformer(fieldDefinition.getName(), byteArrayExtractor,
                    valueMapper));

        }
        return new ResultParser(valueTransformers);
    }

    public ResultParser(List<IndexValueTransformer<Result>> valueTransformers) {
        this.valueTransformers = valueTransformers;
    }

    public Multimap<String, Object> parse(Result result) {
        Multimap<String, Object> parsedValueMap = ArrayListMultimap.create();
        for (IndexValueTransformer<Result> valueTransformer : valueTransformers) {
            parsedValueMap.putAll(valueTransformer.extractAndTransform(result));
        }
        return parsedValueMap;
    }

}
