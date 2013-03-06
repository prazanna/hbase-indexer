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

import com.ngdata.hbasesearch.conf.IndexConf;

/**
 *
 */
public interface UniqueKeyFormatter {
    /**
     * Called in case of row-based mapping, {@link IndexConf.MappingType#ROW}.
     */
    String format(byte[] row);

    /**
     * Called in case of column-based mapping, {@link IndexConf.MappingType#COLUMN}.
     */
    String format(byte[] row, byte[] family, byte[] qualifier);
}
