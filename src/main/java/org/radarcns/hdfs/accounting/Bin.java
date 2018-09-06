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

package org.radarcns.hdfs.accounting;

import java.util.Objects;
import javax.annotation.Nonnull;

public class Bin {
    private final String topic;
    private final String category;
    private final String time;
    private final int hash;

    public Bin(@Nonnull String topic, @Nonnull String category, @Nonnull String time) {
        this.topic = topic;
        this.category = category;
        this.time = time;
        this.hash = Objects.hash(topic, category, time);
    }

    public String getTopic() {
        return topic;
    }

    public String getCategory() {
        return category;
    }

    public String getTime() {
        return time;
    }

    @Override
    public String toString() {
        return topic + ',' + category + ',' + time;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Bin bin = (Bin) o;
        return topic.equals(bin.topic) &&
                category.equals(bin.category) &&
                time.equals(bin.time);
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
