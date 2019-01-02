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

import org.reaktivity.nukleus.function.MessageConsumer;

public final class ServerHandshake
{
    private final MessageConsumer networkReply;
    private final long networkRouteId;
    private final long networkInitialId;
    private final long correlationId;
    private final long applicationRouteId;
    private final boolean timestampRequested;

    public ServerHandshake(
        MessageConsumer networkReply,
        long networkRouteId,
        long networkInitialId,
        long correlationId,
        long applicationRouteId,
        boolean timestampRequested)
    {
        this.networkReply = networkReply;
        this.networkRouteId = networkRouteId;
        this.networkInitialId = networkInitialId;
        this.correlationId = correlationId;
        this.applicationRouteId = applicationRouteId;
        this.timestampRequested = timestampRequested;
    }

    public MessageConsumer networkReply()
    {
        return networkReply;
    }

    public long networkRouteId()
    {
        return networkRouteId;
    }

    public long networkId()
    {
        return networkInitialId;
    }

    public long correlationId()
    {
        return correlationId;
    }

    public long applicationRouteId()
    {
        return applicationRouteId;
    }

    public boolean timestampRequested()
    {
        return timestampRequested;
    }
}
