/**
 * Copyright 2016-2018 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.nukleus.sse.internal.stream;

import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

public final class ServerHandshake
{
    private final long networkId;
    private final String networkName;
    private final long correlationId;
    private final LongSupplier readFrameCounter;
    private final LongConsumer readBytesAccumulator;
    private final boolean timestampRequested;

    public ServerHandshake(
        long networkId,
        String networkName,
        long correlationId,
        LongSupplier readFrameCounter,
        LongConsumer readBytesAccumulator,
        boolean timestampRequested)
    {
        this.networkId = networkId;
        this.networkName = networkName;
        this.correlationId = correlationId;
        this.readFrameCounter = readFrameCounter;
        this.readBytesAccumulator = readBytesAccumulator;
        this.timestampRequested = timestampRequested;
    }

    public long networkId()
    {
        return networkId;
    }

    public String networkName()
    {
        return networkName;
    }

    public long correlationId()
    {
        return correlationId;
    }

    public LongSupplier readFrameCounter()
    {
        return readFrameCounter;
    }

    public LongConsumer readBytesAccumulator()
    {
        return readBytesAccumulator;
    }

    public boolean timestampRequested()
    {
        return timestampRequested;
    }
}
