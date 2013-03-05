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

import com.google.common.collect.Multimap;

/**
 * Extracts a value or collection of values from an input object and transforms them into an indexable form.
 */
public interface IndexValueTransformer<T> {

    /**
     * Extract fields and values from the input value. The key of the output {@code Multimap} is the name of the field
     * to be used in the index.
     * 
     * @param input to be mapped to an indexable form
     * @return multimap keyed on field name, with indexable values
     */
    Multimap<String, Object> extractAndTransform(T input);
}
