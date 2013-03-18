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
package com.ngdata.hbaseindexer.conf;

import com.ngdata.hbaseindexer.uniquekey.UniqueKeyFormatter;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.dom.DOMSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static com.ngdata.hbaseindexer.conf.IndexerConf.MappingType;
import static com.ngdata.hbaseindexer.conf.IndexerConf.RowReadMode;

/**
 * Constructs an {@link IndexerConf} from an XML file.
 */
public class XmlIndexerConfReader {
    public IndexerConf read(InputStream is) throws IOException, SAXException, ParserConfigurationException {
        Document document = parse(is);
        return read(document);
    }

    public void validate(InputStream is) throws IOException, SAXException, ParserConfigurationException {
        Document document = parse(is);
        validate(document);
    }

    public IndexerConf read(Document document) {
        validate(document);

        IndexerConfBuilder builder = new IndexerConfBuilder();

        Element indexEl = document.getDocumentElement();

        builder.table(getAttribute(indexEl, "table", true));
        builder.mappingType(getEnumAttribute(MappingType.class, indexEl, "mapping-type", null));
        builder.rowReadMode(getEnumAttribute(RowReadMode.class, indexEl, "read-row", null));
        builder.uniqueyKeyField(getAttribute(indexEl, "unique-key-field", false));

        String uniqueKeyFormatterName = getAttribute(indexEl, "unique-key-formatter", false);
        if (uniqueKeyFormatterName != null) {
            builder.uniqueKeyFormatterClass(loadClass(uniqueKeyFormatterName, UniqueKeyFormatter.class));
        }

        List<Element> fieldEls = evalXPathAsElementList("field", indexEl);
        for (Element fieldEl : fieldEls) {
            String name = getAttribute(fieldEl, "name", true);
            String value = getAttribute(fieldEl, "value", true);
            ValueSource source = getEnumAttribute(ValueSource.class, fieldEl, "source", null);
            String type = getAttribute(fieldEl, "type", false);

            builder.addFieldDefinition(name, value, source, type);
        }
        
        List<Element> extractEls = evalXPathAsElementList("extract", indexEl);
        for (Element extractEl : extractEls) {
            String prefix = getAttribute(extractEl, "prefix", false);
            String value = getAttribute(extractEl, "value", true);
            ValueSource source = getEnumAttribute(ValueSource.class, extractEl, "source", null);
            String type = getAttribute(extractEl, "type", false);
            
            builder.addDocumentExtractDefinition(prefix, value, source, type);
        }

        return builder.create();
    }

    private static Document parse(InputStream is) throws ParserConfigurationException, IOException, SAXException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        return factory.newDocumentBuilder().parse(is);
    }

    private void validate(Document document) {
        try {
            SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            URL url = getClass().getResource("indexerconf.xsd");
            Schema schema = factory.newSchema(url);
            Validator validator = schema.newValidator();
            validator.validate(new DOMSource(document));
        } catch (Exception e) {
            throw new IndexerConfException("Error validating index configuration against XML Schema.", e);
        }
    }

    private List<Element> evalXPathAsElementList(String expression, Node node) {
        try {
            XPathExpression expr = XPathFactory.newInstance().newXPath().compile(expression);
            NodeList list = (NodeList)expr.evaluate(node, XPathConstants.NODESET);
            List<Element> newList = new ArrayList<Element>(list.getLength());
            for (int i = 0; i < list.getLength(); i++) {
                newList.add((Element)list.item(i));
            }
            return newList;
        } catch (XPathExpressionException e) {
            throw new IndexerConfException("Error evaluating XPath expression '" + expression + "'.", e);
        }
    }

    public static String getAttribute(Element element, String name, boolean required) {
        if (!element.hasAttribute(name)) {
            if (required)
                throw new IndexerConfException("Missing attribute " + name + " on element " + element.getLocalName());
            else
                return null;
        }

        return element.getAttribute(name);
    }

    private <T extends Enum> T getEnumAttribute(Class<T> enumClass, Element element, String attribute, T defaultValue) {
        if (!element.hasAttribute(attribute)) {
            return defaultValue;
        }
        String value = element.getAttribute(attribute);
        try {
            return (T)Enum.valueOf(enumClass, value.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IndexerConfException("Illegal value in attribute " + attribute + " on element "
                    + element.getLocalName() + ": '" + value);
        }
    }

    private <T> Class<T> loadClass(String className, Class<T> baseType) {
        try {
            Class<T> clazz = (Class<T>)getClass().getClassLoader().loadClass(className);
            if (!baseType.isAssignableFrom(clazz)) {
                throw new IndexerConfException("Expected a class which inherits from " + baseType.getName()
                        + ", which the following does not: " + clazz.getName());
            }
            return clazz;
        } catch (ClassNotFoundException e) {
            throw new IndexerConfException("Could not load class: '" + className + "'", e);
        }
    }
}
