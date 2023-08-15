/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.standard.merge;

import com.google.common.base.Strings;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.xml.processing.ProcessingException;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AttributeDecoratorUtil {

    public static final String NAME_FIELD = "name";
    public static final String VALUE_FIELD = "value";
    public static final RecordSchema ATTRIBUTE_RECORD_SCHEMA = buildAttributeRecordSchema();

    private static final String PERIOD = ".";

    private static RecordSchema buildAttributeRecordSchema() {
        final List<RecordField> recordFields = new ArrayList<>();

        recordFields.add(new RecordField(NAME_FIELD, RecordFieldType.STRING.getDataType(), false));
        recordFields.add(new RecordField(VALUE_FIELD, RecordFieldType.STRING.getDataType(), true));

        return new SimpleRecordSchema(recordFields);
    }

    /**
     * Gets the entry name of an attribute file
     * @param entryName Base filename
     * @param fileExtension Attribute file extension, without a period (.) prefix
     */
    public static String getAttributeEntryName(final String entryName, final String fileExtension) {
        return entryName + PERIOD + fileExtension;
    }

    /**
     * Determines whether the actual attribute file entry name matches the name of the base file plus the extension.
     * If not, it's possible the attribute file is out of order or missing.
     */
    public static void validateAttributeEntryName(final String attributeFileName, final String fileName, final String fileExtension) throws FileNotFoundException, InvalidObjectException {
        if (Strings.isNullOrEmpty(fileExtension)) {
            throw new InvalidObjectException("Attribute file extension blank or empty");
        }

        final String expectedAttributeFileName = getAttributeEntryName(fileName, fileExtension);
        if (!expectedAttributeFileName.equals(attributeFileName)) {
            throw new FileNotFoundException(String.format("Unexpected file in archive. Expected attribute file %s, got %s instead", expectedAttributeFileName, attributeFileName));
        }
    }

    /**
     * Read the next file in the archive, assuming it is named after the FlowFile before it, e.g. file1.txt -> file1.txt.attributes.
     * Use the configured RecordReader to parse it into an attribute `Map`.
     * Once the FlowFile attribute records are read from the file, merge them with the input attributes.
     * @param attributes Attributes contained in the file are merged with these
     * @param inFile Compressed archive to read file from
     * @param fileSize Attribute file length
     * @param readerFactory Reader to use for parsing
     * @param logger Component logger
     * @return Collection of FlowFile attributes
     */
    public static void decorateAttributesFromFile(final Map<String, String> attributes, final InputStream inFile, final long fileSize,
                                                  final RecordReaderFactory readerFactory, final ComponentLog logger) {
        try {
            // Read the entire attribute file, as RecordReader implementations require mark/seek support, which is
            // unavailable with ZipArchiveInputStream.
            final ByteArrayOutputStream attributeData = new ByteArrayOutputStream();
            StreamUtils.copy(inFile, attributeData);

            final RecordReader attributeReader = readerFactory.createRecordReader(null, new ByteArrayInputStream(attributeData.toByteArray()), fileSize, logger);

            Record record;
            while ((record = attributeReader.nextRecord()) != null) {
                final String attributeName = record.getAsString(AttributeDecoratorUtil.NAME_FIELD);
                final String attributeValue = record.getAsString(AttributeDecoratorUtil.VALUE_FIELD);

                // Ignore the UUID attribute as we don't want to potentially have multiple FlowFiles with the same UUID
                if (!CoreAttributes.UUID.key().equals(attributeName)) {
                    attributes.put(attributeName, attributeValue);
                }
            }
        } catch (SchemaNotFoundException | IOException | MalformedRecordException e) {
            throw new ProcessingException("Error occurred when parsing FlowFile attribute file", e);
        }
    }
}
