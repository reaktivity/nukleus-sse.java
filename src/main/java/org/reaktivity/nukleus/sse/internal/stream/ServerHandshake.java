/**
 * Copyright 2016-2017 The Reaktivity Project
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
    private final String networkName;
    private final long correlationId;
    private final LongSupplier countReadFrames;
    private final LongConsumer countReadBytes;

    public ServerHandshake(
        String networkName,
        long correlationId,
        LongSupplier readFrameCounter,
        LongConsumer readBytesAccumulator)
    {
        this.networkName = networkName;
        this.correlationId = correlationId;
        this.countReadFrames = readFrameCounter;
        this.countReadBytes = readBytesAccumulator;
    }

    public String networkName()
    {
        return networkName;
    }

    public long correlationId()
    {
        return correlationId;
    }

    public LongSupplier countReadFrames()
    {
        return countReadFrames;
    }

    public LongConsumer countReadBytes()
    {
        return countReadBytes;
    }
}
