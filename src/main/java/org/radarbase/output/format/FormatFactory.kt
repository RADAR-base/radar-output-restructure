/*
 * Copyright 2018 The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarbase.output.format

import java.io.IOException

class FormatFactory : FormatProvider<RecordConverterFactory> {
    override val formats: List<RecordConverterFactory> = listOf(
        CsvAvroConverter.factory,
        JsonAvroConverter.factory,
        OdmAvroConverter.factory,
    )

    @Throws(IOException::class)
    override fun init(properties: Map<String, String>) {
        val odmFactory = OdmAvroConverter.factory as OdmAvroConverterFactory
        odmFactory.metadataVersionOid =
            properties[OdmFieldConfig.PROP_METADATA_VERSION_OID]
                ?: OdmFieldConfig.DEFAULT_METADATA_VERSION_OID
        odmFactory.fieldConfig = OdmFieldConfig.fromProperties(properties)
    }
}
