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

import org.apache.solr.common.SolrInputDocument;

/**
 * Extracts a value or collection of values from an input object and transforms them into a {@code SolrInputDocument}.
 */
public interface SolrDocumentExtractor<T> {

    /**
     * Extract fields and values from the input value and puts them into a SolrInputDocument.
     * 
     * @param input to be mapped to an indexable form
     * @return SolrInputDocument containing extracted fields
     */
    SolrInputDocument extractFields(T input);
}
