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

package org.radarcns.hdfs.util;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Progress bar.
 * Based on https://stackoverflow.com/a/43381186/574082.
 */
public class ProgressBar {
    private final long total;
    private final int numStripes;
    private final long startTime;
    private final AtomicBoolean isDone;

    public ProgressBar(long total, int numStripes) {
        if (total < 0) {
            throw new IllegalArgumentException("Total of progress bar must be positive");
        }
        if (numStripes <= 0) {
            throw new IllegalArgumentException("Number of stripes in progress bar must be positive");
        }
        this.total = total;
        this.numStripes = numStripes;
        this.startTime = System.currentTimeMillis();
        this.isDone = new AtomicBoolean(false);
    }

    public synchronized void update(long remain) {
        if (remain > total || remain < 0) {
            throw new IllegalArgumentException(
                    "Update value " + remain + " out of range [0, " + total + "].");
        }

        if (remain == total && !isDone.compareAndSet(false, true)) {
            return;
        }

        StringBuilder builder = new StringBuilder(numStripes + 25);

        float remainPercent;
        if (total > 0) {
            remainPercent = Math.min(((100f * remain) / total), 100f);
        } else {
            remainPercent = 100f;
        }

        bar(builder, remainPercent);
        builder.append(' ');
        percentage(builder, remainPercent);
        builder.append(" - ");
        eta(builder, remain);

        if (remain >= total) {
            builder.append('\n');
        }

        System.out.print(builder.toString());
    }

    private void percentage(StringBuilder builder, float remainPercent) {
        builder.append((int)remainPercent).append('%');
    }

    private void bar(StringBuilder builder, float remainPercent) {
        int stripesFilled = (int) (numStripes * remainPercent / 100);
        char notFilled = '-';
        char filled = '*';
        // 2 init + numStripes + 2 end + 4 percentage
        builder.append("\r[");
        for (int i = 0; i < stripesFilled; i++) {
            builder.append(filled);
        }
        for (int i = stripesFilled; i < numStripes; i++) {
            builder.append(notFilled);
        }
        builder.append(']');
    }

    private void eta(StringBuilder builder, long remain) {
        builder.append("ETA ");
        if (remain > 0) {
            long duration = (System.currentTimeMillis() - startTime);
            formatTime(builder,duration * (total - remain) / (remain * 1000L));
        } else {
            builder.append('-');
        }
    }

    public static StringBuilder formatTime(StringBuilder builder, long seconds) {
        long minutes = (seconds / 60) % 60;
        long sec = seconds % 60;
        builder.append(seconds / 3600).append(':');
        if (minutes < 10) {
            builder.append('0');
        }
        builder.append(minutes).append(':');
        if (sec < 10) {
            builder.append('0');
        }
        builder.append(sec);
        return builder;
    }

    public static String formatTime(Duration duration) {
        long millis = duration.toMillis();
        StringBuilder builder = new StringBuilder(16);
        formatTime(builder, millis / 1000)
                .append('.');
        long millisLast = (int)(millis % 1000L);
        if (millisLast < 100) {
            builder.append('0');
        }
        if (millisLast < 10) {
            builder.append('0');
        }
        return builder.append(millisLast).toString();
    }
}
