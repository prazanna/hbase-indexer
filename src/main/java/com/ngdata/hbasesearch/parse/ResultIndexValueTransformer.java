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

import java.util.List;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.apache.hadoop.hbase.client.Result;

/**
 * Extracts a value or collection of values from a {@code Result} object and transforms them into an indexable form.
 */
public class ResultIndexValueTransformer implements IndexValueTransformer<Result> {

    private String fieldName;
    private ByteArrayExtractor valueExtractor;
    private ByteArrayValueMapper valueMapper;

    public ResultIndexValueTransformer(String fieldName, ByteArrayExtractor valueExtractor,
            ByteArrayValueMapper valueMapper) {
        this.fieldName = fieldName;
        this.valueExtractor = valueExtractor;
        this.valueMapper = valueMapper;
    }

    /**
     * Extracts byte arrays from the given result, and transforms them into indexable values.
     * 
     * @param result source of byte array data
     * @return indexable values
     */
    @Override
    public Multimap<String, Object> extractAndTransform(Result result) {
        List<Object> values = Lists.newArrayList();
        for (byte[] bytes : valueExtractor.extract(result)) {
            values.addAll(valueMapper.map(bytes));
        }
        Multimap<String, Object> mapped = ArrayListMultimap.create(1, values.size());
        mapped.putAll(fieldName, values);
        return mapped;
    }

}
