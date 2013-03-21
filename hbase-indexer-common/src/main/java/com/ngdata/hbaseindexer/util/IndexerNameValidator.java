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
package com.ngdata.hbaseindexer.util;

import java.util.regex.Pattern;

/**
 * Perform validation on Indexer names.
 */
public class IndexerNameValidator {

    private static final Pattern INDEXER_NAME_REGEX = Pattern.compile("^\\w+$");

    /**
     * Validate an indexer name.
     * 
     * If there is an issue with the indexer name, an IllegalArgumentException will be thrown, with a description of
     * what the issue is.
     * 
     * @param indexerName name to be validated
     */
    public static void validate(String indexerName) {
        if (indexerName == null || "".equals(indexerName)) {
            throw new IllegalArgumentException("Indexer name may not be empty");
        }

        if (!INDEXER_NAME_REGEX.matcher(indexerName).matches()) {
            throw new IllegalArgumentException(String.format("Invalid indexer name '%s', must be alpha-numeric",
                    indexerName));
        }
    }

}
