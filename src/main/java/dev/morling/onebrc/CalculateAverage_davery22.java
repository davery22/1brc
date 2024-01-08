/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import sun.misc.Unsafe;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

public class CalculateAverage_davery22 {
    static final String FILE = "./measurements.txt";
    static final int MAP_SIZE = 1 << 16; // Must be a power of two > 10000
    static final int MAP_MASK = MAP_SIZE - 1;
    static final boolean IS_LITTLE_ENDIAN = ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);
    static final Unsafe UNSAFE;

    static {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            UNSAFE = (Unsafe) theUnsafe.get(Unsafe.class);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        FileChannel in = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ);
        int concurrency = Runtime.getRuntime().availableProcessors();
        Thread[] threads = new Thread[concurrency - 1];
        Worker[] workers = new Worker[concurrency - 1];
        long fileSize = in.size();
        long segmentSize = fileSize / concurrency;
        long start = in.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.global()).address();

        // Process each segment in its own thread
        for (int i = 0; i < concurrency - 1; i++) {
            long end = start + segmentSize;
            for (; end > start && UNSAFE.getByte(end - 1) != '\n'; end--) {
            }
            Thread t = threads[i] = new Thread(workers[i] = new Worker(start, end));
            t.start();
            start = end;
        }

        // Process last segment on main thread
        Worker main = new Worker(start, fileSize);
        main.run();

        for (Thread t : threads) {
            t.join();
        }

        // Merge maps
        for (int i = 0; i < concurrency - 1; i++) {
            long[][] entries = workers[i].entries;
            for (int j = 0; entries[j] != null; j++) {
                main.mergeEntry(entries[j]);
            }
        }

        // Estimate size of output buffer - okay to overestimate, not underestimate
        long[][] entries = main.entries;
        int platformCR = System.lineSeparator().length() > 1 ? 1 : 0; // May be different from file
        int bufferLen = 3 + platformCR; // '{' and '}' and '\n' (and '\r' if detected)
        int entriesLen = 0;
        for (; entries[entriesLen] != null; entriesLen++) {
            // Needs enough space for: '<city_name>=<min>/<mean>/<max>, ' where the stats are up to 5 bytes each
            bufferLen += (entries[entriesLen].length - 4) * 8 + 20;
        }

        // Sort by city name
        Arrays.sort(entries, 0, entriesLen, (a, b) -> {
            int n = Math.min(a.length, b.length) - 4;
            for (int i = 0; i < n; i++) {
                int cmp = Long.compareUnsigned(a[i], b[i]);
                if (cmp != 0) {
                    return cmp;
                }
            }
            return a.length - b.length;
        });

        // Fill the output buffer
        byte[] toPrint = new byte[bufferLen];
        bufferLen = 0;
        toPrint[bufferLen++] = '{';
        for (int i = 0; i < entriesLen; i++) {
            // Delimiter
            if (i > 0) {
                toPrint[bufferLen++] = ',';
                toPrint[bufferLen++] = ' ';
            }
            // Name
            long[] entry = entries[i];
            int j = 0;
            for (; j < entry.length - 5; j++) {
                long word = entry[j];
                for (int k = 0; k < 8; k++) {
                    toPrint[bufferLen++] = (byte) ((word >>> (56 - k * 8)) & 0xFF);
                }
            }
            long last = entry[j++];
            int lastLen = 8 - (Long.numberOfTrailingZeros(last) >>> 3);
            for (int k = 0; k < lastLen; k++) {
                toPrint[bufferLen++] = (byte) ((last >>> (56 - k * 8)) & 0xFF);
            }
            // Stats
            long min = entry[j++];
            long max = entry[j++];
            long sum = entry[j++];
            long count = entry[j++];
            long mean = mean(sum, count);
            toPrint[bufferLen++] = '=';
            bufferLen = statToBytes(min, toPrint, bufferLen);
            toPrint[bufferLen++] = '/';
            bufferLen = statToBytes(mean, toPrint, bufferLen);
            toPrint[bufferLen++] = '/';
            bufferLen = statToBytes(max, toPrint, bufferLen);
        }
        toPrint[bufferLen++] = '}';
        if (platformCR == 1) {
            toPrint[bufferLen++] = '\r';
        }
        toPrint[bufferLen++] = '\n';

        // Print
        FileOutputStream out = new FileOutputStream(FileDescriptor.out);
        out.write(toPrint, 0, bufferLen);
    }

    static class Worker implements Runnable {
        final long start, end;
        final int[] indexes = new int[MAP_SIZE];
        final long[][] entries = new long[MAP_SIZE][];
        int lastIndex;

        Worker(long start, long end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public void run() {
            // Big enough for max city bytes (100 bytes) + temperature value (1 long)
            long[] item = new long[14];
            int itemLen = 0;
            long bufCursor = start, bufLimit = end;
            int lineSeparatorLen = (bufLimit - bufCursor > 1 && UNSAFE.getByte(bufLimit - 2) == '\r') ? 2 : 1;

            while (bufCursor < bufLimit) {
                // Parse next long of bytes, until we see a semicolon
                long word = IS_LITTLE_ENDIAN ? Long.reverseBytes(UNSAFE.getLong(bufCursor)) : UNSAFE.getLong(bufCursor);
                int idx = indexOfSemicolon(word);
                if (idx < 0) {
                    item[itemLen++] = word;
                    bufCursor += 8;
                    continue;
                }
                // Zero-out everything after city bytes
                if (idx > 0) {
                    item[itemLen++] = word & (-1L << (64 - idx * 8));
                }
                // Parse the temperature value
                 bufCursor += idx + 1;
                 long sign = 1, magnitude;
                 byte b1, b2;
                 if ((b1 = UNSAFE.getByte(bufCursor)) == '-') {
                     sign = -1;
                     bufCursor += 1;
                     b1 = UNSAFE.getByte(bufCursor);
                 }
                 if ((b2 = UNSAFE.getByte(bufCursor + 1)) == '.') {
                     magnitude = 10 * b1 + UNSAFE.getByte(bufCursor + 2) - 528;
                     bufCursor += 3 + lineSeparatorLen;
                 }
                 else {
                     magnitude = 100 * b1 + 10 * b2 + UNSAFE.getByte(bufCursor + 3) - 5328;
                     bufCursor += 4 + lineSeparatorLen;
                 }

                // Merge to map
                item[itemLen++] = sign * magnitude;
                mergeItem(item, itemLen);
                itemLen = 0;
            }
        }

        static int hash(long word) {
            return (int) (word ^ (word >>> 16) * (word >>> 32) ^ (word >>> 48)) & MAP_MASK;
        }

        void mergeItem(long[] item, int len) { // format [n longs for key, 1 long for value]
            loop: for (int hash = hash(item[0]);; hash = (hash + 1) & MAP_MASK) { // Linear probing
                int index = indexes[hash];
                // Check if new
                if (index == 0) {
                    long[] entry = new long[len + 3];
                    System.arraycopy(item, 0, entry, 0, len);
                    entry[len] = item[len - 1]; // initial max
                    entry[len + 1] = item[len - 1]; // initial sum
                    entry[len + 2] = 1; // initial count
                    indexes[hash] = ++lastIndex;
                    entries[lastIndex - 1] = entry;
                    return;
                }
                // Check if equal or conflict
                long[] entry = entries[index - 1];
                if (entry.length != len + 3) {
                    continue;
                }
                int i = 0;
                for (; i < len - 1; i++) {
                    if (entry[i] != item[i]) {
                        continue loop;
                    }
                }
                // Equal - update stats
                entry[i] = Math.min(entry[i], item[i]); // min
                entry[i + 1] = Math.max(entry[i + 1], item[i]); // max
                entry[i + 2] += item[i]; // sum
                entry[i + 3] += 1; // count
                return;
            }
        }

        void mergeEntry(long[] item) { // format: [n longs for key, 4 longs for min/max/sum/count]
            loop: for (int hash = hash(item[0]);; hash = (hash + 1) & MAP_MASK) { // Linear probing
                int index = indexes[hash];
                // Check if new
                if (index == 0) {
                    indexes[hash] = ++lastIndex;
                    entries[lastIndex - 1] = item;
                    return;
                }
                // Check if equal or conflict
                long[] entry = entries[index - 1];
                if (entry.length != item.length) {
                    continue;
                }
                int i = 0;
                for (; i < item.length - 4; i++) {
                    if (entry[i] != item[i]) {
                        continue loop;
                    }
                }
                // Equal - update stats
                entry[i] = Math.min(entry[i], item[i]); // min
                entry[i + 1] = Math.max(entry[i + 1], item[i + 1]); // max
                entry[i + 2] += item[i + 2]; // sum
                entry[i + 3] += item[i + 3]; // count
                return;
            }
        }
    }

    // SWAR search based on royvanrijn's code
    static final long SEMICOLON_PATTERN = ((long) ';' << 56) | ((long) ';' << 48) | ((long) ';' << 40) | ((long) ';' << 32) |
            ((long) ';' << 24) | ((long) ';' << 16) | ((long) ';' << 8) | ((long) ';');

    static int indexOfSemicolon(long word) {
        long match = word ^ SEMICOLON_PATTERN; // Only matching bytes are zero
        long mask = ((match - 0x0101010101010101L) & ~match) & 0x8080808080808080L; // Only matching bytes are non-zero (leftmost bit is 1)
        return mask == 0 ? -1 : Long.numberOfLeadingZeros(mask) >>> 3; // Return byte index of first 1 bit
    }

    static int statToBytes(long stat, byte[] buf, int pos) {
        // stat is fixed point, so the original guaranteed range of [-99.9, 99.9] becomes [-999, 999]
        if (stat < 0) {
            stat = -stat;
            buf[pos++] = '-';
        }
        int digits = stat > 99 ? 3 : 2;
        buf[pos + digits] = (byte) ((stat % 10) + '0');
        buf[pos + digits - 1] = '.';
        stat /= 10;
        for (int i = digits - 2; i >= 0; i--) {
            buf[pos + i] = (byte) ((stat % 10) + '0');
            stat /= 10;
        }
        return pos + digits + 1;
    }

    static long mean(long sum, long count) {
        long mean = sum / count;
        long remainder = sum % count;
        if (remainder < 0) {
            // Match the rounding behavior of the baseline, which uses Math.round(number * 10.0) / 10.0.
            // For positive numbers we need to round up between [.5, 1).
            // For negative numbers we need to round up between [-.5, 0).
            // Interestingly this differs from the rounding behavior of String.format("%.1f", number),
            // which would round -8.55 to -8.6, rather than -8.5.
            if (count < (-remainder << 1)) {
                return mean - 1;
            }
        }
        else if (count <= (remainder << 1)) {
            return mean + 1;
        }
        return mean;
    }
}
