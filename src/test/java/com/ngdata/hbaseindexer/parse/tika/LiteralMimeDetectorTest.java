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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.tika.detect.Detector;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.junit.Before;
import org.junit.Test;

public class LiteralMimeDetectorTest {
    
    private Detector delegate;
    private LiteralMimeDetector mimeDetector;
    private InputStream inputStream;

    @Before
    public void setUp() {
        delegate = mock(Detector.class);
        mimeDetector = new LiteralMimeDetector(delegate);
        inputStream = new ByteArrayInputStream("dummy input".getBytes());
    }
    
    @Test
    public void testDetect() throws IOException {
        Metadata metadata = new Metadata();
        metadata.add(LiteralMimeDetector.MIME_TYPE, "application/dummy");
        MediaType detected = mimeDetector.detect(inputStream, metadata);
        
        assertEquals(MediaType.application("dummy"), detected);
    }
    
    @Test
    public void testDetect_NotParseableMimeType() throws IOException {
        MediaType delegateMediaType = MediaType.application("delegate");
        Metadata metadata = new Metadata();
        when(delegate.detect(inputStream, metadata)).thenReturn(delegateMediaType);
        
        metadata.add(LiteralMimeDetector.MIME_TYPE, "not a parseable mime type");
        MediaType detected = mimeDetector.detect(inputStream, metadata);
        
        assertEquals(delegateMediaType, detected);
    }
    
    @Test
    public void testDetect_NoMimeTypeInMetadata() throws IOException {
        MediaType delegateMediaType = MediaType.application("delegate");
        Metadata metadata = new Metadata();
        when(delegate.detect(inputStream, metadata)).thenReturn(delegateMediaType);
        
        assertEquals(delegateMediaType, mimeDetector.detect(inputStream, metadata));
    }

}
