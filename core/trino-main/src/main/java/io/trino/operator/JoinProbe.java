/*
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
package io.trino.operator;

import io.trino.spi.Page;
import io.trino.spi.block.Block;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Arrays.stream;

public class JoinProbe
{
    public static class JoinProbeFactory
    {
        private final int[] probeOutputChannels;
        private final List<Integer> probeJoinChannels;
        private final OptionalInt probeHashChannel;

        public JoinProbeFactory(int[] probeOutputChannels, List<Integer> probeJoinChannels, OptionalInt probeHashChannel)
        {
            this.probeOutputChannels = probeOutputChannels;
            this.probeJoinChannels = probeJoinChannels;
            this.probeHashChannel = probeHashChannel;
        }

        public JoinProbe createJoinProbe(Page page)
        {
            return new JoinProbe(probeOutputChannels, page, probeJoinChannels, probeHashChannel);
        }
    }

    private static final int CACHE_THRESHOLD = 16384;

    private final int[] probeOutputChannels;
    private final int positionCount;
    private final Block[] nullableProbeBlocks;
    private final Page page;
    private final Page probePage;
    private final Optional<Block> probeHashBlock;
    @Nullable
    private long[] joinPositionsCache;
    private LookupSource lookupSource;

    private int position = -1;

    private JoinProbe(int[] probeOutputChannels, Page page, List<Integer> probeJoinChannels, OptionalInt probeHashChannel)
    {
        this.probeOutputChannels = probeOutputChannels;
        this.positionCount = page.getPositionCount();
        Block[] probeBlocks = new Block[probeJoinChannels.size()];

        for (int i = 0; i < probeJoinChannels.size(); i++) {
            probeBlocks[i] = page.getBlock(probeJoinChannels.get(i)).getLoadedBlock();
        }
        nullableProbeBlocks = stream(probeBlocks).filter(Block::mayHaveNull).toArray(Block[]::new);
        this.page = page;
        this.probePage = new Page(page.getPositionCount(), probeBlocks);
        this.probeHashBlock = probeHashChannel.isPresent() ? Optional.of(page.getBlock(probeHashChannel.getAsInt()).getLoadedBlock()) : Optional.empty();
    }

    public void setLookupSource(LookupSource lookupSource)
    {
        this.lookupSource = lookupSource;
        if (lookupSource.supportsCaching() && lookupSource.getJoinPositionCount() > CACHE_THRESHOLD) {
            fillJoinPositionCache(lookupSource);
        }
    }

    public int[] getOutputChannels()
    {
        return probeOutputChannels;
    }

    public boolean advanceNextPosition()
    {
        verify(position < positionCount, "already finished");
        position++;
        return !isFinished();
    }

    public boolean isFinished()
    {
        return position == positionCount;
    }

    public long getCurrentJoinPosition()
    {
        if (joinPositionsCache == null) {
            return getJoinPosition(position, lookupSource);
        }
        return joinPositionsCache[position];
    }

    private void fillJoinPositionCache(LookupSource lookupSource)
    {
        if (mayContainNullRows()) {
            int[] positions = nonNullPositions(positionCount);
            long[] result;
            if (probeHashBlock.isPresent()) {
                long[] rawHashes = new long[positions.length];
                for (int i = 0; i < positions.length; ++i) {
                    rawHashes[i] = BIGINT.getLong(probeHashBlock.get(), positions[i]);
                }
                result = lookupSource.getJoinPositions(positions, probePage, page, rawHashes);
            }
            else {
                result = lookupSource.getJoinPositions(positions, probePage, page);
            }

            long[] cache = initCache();
            for (int i = 0; i < positions.length; i++) {
                cache[positions[i]] = result[i];
            }
        }
        else {
            if (probeHashBlock.isPresent()) {
                long[] rawHashes = new long[positionCount];
                for (int i = 0; i < positionCount; ++i) {
                    rawHashes[i] = BIGINT.getLong(probeHashBlock.get(), i);
                }
                joinPositionsCache = lookupSource.getJoinPositions(consecutivePositions(), probePage, page, rawHashes);
            }
            else {
                joinPositionsCache = lookupSource.getJoinPositions(consecutivePositions(), probePage, page);
            }
        }
    }

    private long[] initCache()
    {
        joinPositionsCache = new long[positionCount];
        Arrays.fill(joinPositionsCache, 0, positionCount, -1);

        return joinPositionsCache;
    }

    private int[] nonNullPositions(int limit)
    {
        // Loop split into two for performance reasons
        int nullPositions = 0;
        for (int i = 0; i < limit; ++i) {
            if (rowContainsNull(i)) {
                nullPositions++;
            }
        }
        int[] positions = new int[limit - nullPositions];
        int count = 0;
        for (int i = 0; i < limit; ++i) {
            if (!rowContainsNull(i)) {
                positions[count++] = i;
            }
        }
        return positions;
    }

    private int[] consecutivePositions()
    {
        int[] result = new int[positionCount];
        for (int i = 0; i < positionCount; i++) {
            result[i] = i;
        }
        return result;
    }

    private long getJoinPosition(int position, LookupSource lookupSource)
    {
        if (rowContainsNull(position)) {
            return -1;
        }
        if (probeHashBlock.isPresent()) {
            long rawHash = BIGINT.getLong(probeHashBlock.get(), position);
            return lookupSource.getJoinPosition(position, probePage, page, rawHash);
        }
        return lookupSource.getJoinPosition(position, probePage, page);
    }

    public int getPosition()
    {
        return position;
    }

    public Page getPage()
    {
        return page;
    }

    private boolean rowContainsNull(int position)
    {
        for (Block probeBlock : nullableProbeBlocks) {
            if (probeBlock.isNull(position)) {
                return true;
            }
        }
        return false;
    }

    private boolean mayContainNullRows()
    {
        return nullableProbeBlocks.length != 0;
    }
}
