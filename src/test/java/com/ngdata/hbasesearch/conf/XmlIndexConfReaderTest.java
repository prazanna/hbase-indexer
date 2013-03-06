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

import com.ngdata.hbasesearch.HexUniqueKeyFormatter;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class XmlIndexConfReaderTest {
    @Test
    public void testValid() throws Exception {
        new XmlIndexConfReader().read(asStream("<index table='foo'/>"));
    }

    @Test(expected = IndexConfException.class)
    public void testInvalid() throws Exception {
        new XmlIndexConfReader().read(asStream("<foo/>"));
    }

    private InputStream asStream(String data) {
        return new ByteArrayInputStream(data.getBytes());
    }

    @Test
    public void testFullIndexConf() throws Exception {
        IndexConf conf = new XmlIndexConfReader().read(getClass().getResourceAsStream("indexconf_full.xml"));

        assertEquals("table1", conf.getTable());
        assertEquals(IndexConf.MappingType.COLUMN, conf.getMappingType());
        assertEquals(IndexConf.RowReadMode.NEVER, conf.getRowReadMode());
        assertEquals("key", conf.getUniqueKeyField());
        assertEquals(HexUniqueKeyFormatter.class, conf.getUniqueKeyFormatterClass());

        List<FieldDefinition> fieldDefs = conf.getFieldDefinitions();
        assertEquals(2, fieldDefs.size());
        assertEquals("field1", fieldDefs.get(0).getName());
        assertEquals("col:qual1", fieldDefs.get(0).getValueExpression());
        assertEquals(ValueSource.QUALIFIER, fieldDefs.get(0).getValueSource());
        assertEquals("float", fieldDefs.get(0).getTypeName());

        assertEquals(ValueSource.VALUE, fieldDefs.get(1).getValueSource());
    }

    @Test
    public void testDefaults() throws Exception {
        IndexConf conf = new XmlIndexConfReader().read(getClass().getResourceAsStream("indexconf_defaults.xml"));

        assertEquals("table1", conf.getTable());
        assertEquals(IndexConf.DEFAULT_MAPPING_TYPE, conf.getMappingType());
        assertEquals(IndexConf.DEFAULT_ROW_READ_MODE, conf.getRowReadMode());
        assertEquals(IndexConf.DEFAULT_UNIQUE_KEY_FIELD, conf.getUniqueKeyField());
        assertEquals(IndexConf.DEFAULT_UNIQUE_KEY_FORMATTER, conf.getUniqueKeyFormatterClass());

        List<FieldDefinition> fieldDefs = conf.getFieldDefinitions();
        assertEquals(1, fieldDefs.size());
        assertEquals("field1", fieldDefs.get(0).getName());
        assertEquals("col:qual1", fieldDefs.get(0).getValueExpression());
        assertEquals(FieldDefinition.DEFAULT_VALUE_SOURCE, fieldDefs.get(0).getValueSource());
        assertEquals(FieldDefinition.DEFAULT_TYPE_NAME, fieldDefs.get(0).getTypeName());
    }
}
