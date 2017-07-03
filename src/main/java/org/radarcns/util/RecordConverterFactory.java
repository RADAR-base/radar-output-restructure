/*
 * Copyright 2017 The Hyve
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

package org.radarcns.util;

import java.io.IOException;
import java.io.Writer;
import org.apache.avro.generic.GenericRecord;

public interface RecordConverterFactory {
    /**
     * Create a converter to write records of given type to given writer. A header is needed only
     * in certain converters. The given record is not converted yet, it is only used as an example.
     * @param writer to write data to
     * @param record to generate the headers and schemas from.
     * @param writeHeader whether to write a header, if applicable
     * @return RecordConverter that is ready to be used
     * @throws IOException if the converter could not be created
     */
    RecordConverter converterFor(Writer writer, GenericRecord record, boolean writeHeader) throws IOException;
}
