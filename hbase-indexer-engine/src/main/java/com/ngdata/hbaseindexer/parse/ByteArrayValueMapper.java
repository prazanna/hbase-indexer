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

import java.util.Collection;

/**
 * Maps a byte array to a collection of values to be included in an index.
 */
public interface ByteArrayValueMapper {

    /**
     * Map a byte array to a collection of values. The returned collection can be empty.
     * <p>
     * If a value cannot be mapped as requested, it should log the error and return an empty collection.
     * 
     * @param input byte array to be mapped
     * @return mapped values
     */
    Collection<? extends Object> map(byte[] input);

}
