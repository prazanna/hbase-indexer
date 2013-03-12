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
package com.ngdata.hbaseindexer.parse.tika;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tika.detect.Detector;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;

/**
 * Tika {@code Detector} that just pulls the MIME type directly from the included metadata.
 * <p>
 * If for some reason there is no MIME type included in the metadata, detection will be passed off to an included
 * delegate {@code Detector}.
 */
public class LiteralMimeDetector implements Detector {

    /**
     * Metadata key under which the literal MIME type is stored.
     * <p>
     * This key starts with the string "ignored_", as this matches a default ignore field rule in the default Solr
     * configuration.
     */
    public static final String MIME_TYPE = "ignored_hbaseindexer_mime_type";

    private Log log = LogFactory.getLog(getClass());

    private Detector delegate;

    /**
     * Instantiate around a delegate that will be used if a MIME type cannot be directly extracted from incoming
     * metadata.
     * 
     * @param delegate default detector to delegate to
     */
    public LiteralMimeDetector(Detector delegate) {
        this.delegate = delegate;
    }

    @Override
    public MediaType detect(InputStream input, Metadata metadata) throws IOException {
        String mimeType = metadata.get(MIME_TYPE);
        if (mimeType != null) {
            MediaType mediaType = MediaType.parse(mimeType);
            if (mediaType == null) {
                log.warn("Couldn't parse MIME type '" + mimeType + "'");
            } else {
                return mediaType;
            }
        }
        return delegate.detect(input, metadata);
    }

}
