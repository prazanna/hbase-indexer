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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;

public class SolrInputDocumentBuilderTest {

    @Test
    public void testAdd() {
        SolrInputDocumentBuilder builder = new SolrInputDocumentBuilder();
        
        SolrInputDocument docA = new SolrInputDocument();
        SolrInputDocument docB = new SolrInputDocument();
        
        docA.addField("fieldA", "valueA1");
        docA.addField("fieldA", "valueA2");
        docB.addField("fieldB", "valueB");
        
        builder.add(docA);
        builder.add(docB);
        
        SolrInputDocument merged = builder.getDocument();
        
        assertEquals(Sets.newHashSet("fieldA", "fieldB"), merged.keySet());
        
        assertEquals(Lists.newArrayList("valueA1", "valueA2"), merged.getField("fieldA").getValues());
        assertEquals(Lists.newArrayList("valueB"), merged.getField("fieldB").getValues());
    }
    
    @Test
    public void testAdd_WithBaseDocument() {
        SolrInputDocument baseDocument = new SolrInputDocument();
        baseDocument.addField("baseField", "baseValue");
        
        SolrInputDocumentBuilder builder = new SolrInputDocumentBuilder(baseDocument);
        
        SolrInputDocument additionalDocument = new SolrInputDocument();
        additionalDocument.addField("additionalField", "additionalValue");
        builder.add(additionalDocument);
        
        SolrInputDocument merged = builder.getDocument();
        assertSame(merged, baseDocument);
        assertEquals(Sets.newHashSet("baseField", "additionalField"), merged.keySet());
        
    }
    
    @Test
    public void testAdd_WithPrefix() {
        SolrInputDocumentBuilder builder = new SolrInputDocumentBuilder();
        
        SolrInputDocument docA = new SolrInputDocument();
        SolrInputDocument docB = new SolrInputDocument();
        
        docA.addField("fieldA", "valueA");
        docB.addField("fieldB", "valueB");
        
        builder.add(docA, "A_");
        builder.add(docB);
        
        SolrInputDocument merged = builder.getDocument();
        
        assertEquals(Sets.newHashSet("A_fieldA", "fieldB"), merged.keySet());
        
        assertEquals(Lists.newArrayList("valueA"), merged.getField("A_fieldA").getValues());
        assertEquals(Lists.newArrayList("valueB"), merged.getField("fieldB").getValues());
    }
    
    @Test
    public void testAdd_OverlappingFields() {
        SolrInputDocumentBuilder builder = new SolrInputDocumentBuilder();
        
        SolrInputDocument docA = new SolrInputDocument();
        SolrInputDocument docB = new SolrInputDocument();
        
        docA.addField("field", "A1", 0.5f);
        docA.addField("field", "A2", 0.5f);
        docB.addField("field", "B1", 1.5f);
        docB.addField("field", "B2", 1.5f);
        
        builder.add(docA);
        builder.add(docB);
        
        SolrInputDocument merged = builder.getDocument();
        
        assertEquals(Sets.newHashSet("field"), merged.keySet());
        assertEquals(Lists.newArrayList("A1", "A2", "B1", "B2"), merged.get("field").getValues());
        
        // The boost of the first-added definition of a field is the definitive version
        assertEquals(0.5f * 0.5f * 1.5f * 1.5f, merged.getField("field").getBoost(), 0f);
    }


}
