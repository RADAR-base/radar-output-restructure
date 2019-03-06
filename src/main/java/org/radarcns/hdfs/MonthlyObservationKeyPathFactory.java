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

package org.radarcns.hdfs;

import static java.time.ZoneOffset.UTC;

import java.time.format.DateTimeFormatter;

public class MonthlyObservationKeyPathFactory extends ObservationKeyPathFactory {
    private static final DateTimeFormatter PER_MONTH_HOURLY_TIME_BIN_FORMAT = DateTimeFormatter.ofPattern("yyyyMM/yyyyMMdd_HH'00'")
            .withZone(UTC);


    public DateTimeFormatter getTimeBinFormat() {
        return PER_MONTH_HOURLY_TIME_BIN_FORMAT;
    }
}
