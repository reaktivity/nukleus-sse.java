/**
 * Copyright 2016-2021 The Reaktivity Project
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

import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.budget.BudgetDebitor;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.sse.internal.SseConfiguration;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.nukleus.stream.StreamFactoryBuilder;

public final class SseServerFactoryBuilder implements StreamFactoryBuilder
{
    private final SseConfiguration config;

    private RouteManager router;
    private MutableDirectBuffer writeBuffer;
    private LongUnaryOperator supplyInitialId;
    private LongUnaryOperator supplyReplyId;
    private LongSupplier supplyTraceId;
    private ToIntFunction<String> supplyTypeId;
    private Supplier<BufferPool> supplyBufferPool;
    private LongFunction<BudgetDebitor> supplyDebitor;

    public SseServerFactoryBuilder(
        SseConfiguration config)
    {
        this.config = config;
    }

    @Override
    public SseServerFactoryBuilder setRouteManager(
        RouteManager router)
    {
        this.router = router;
        return this;
    }

    @Override
    public SseServerFactoryBuilder setWriteBuffer(
        MutableDirectBuffer writeBuffer)
    {
        this.writeBuffer = writeBuffer;
        return this;
    }

    @Override
    public SseServerFactoryBuilder setInitialIdSupplier(
        LongUnaryOperator supplyInitialId)
    {
        this.supplyInitialId = supplyInitialId;
        return this;
    }

    @Override
    public StreamFactoryBuilder setReplyIdSupplier(
        LongUnaryOperator supplyReplyId)
    {
        this.supplyReplyId = supplyReplyId;
        return this;
    }

    @Override
    public StreamFactoryBuilder setTraceIdSupplier(
        LongSupplier supplyTraceId)
    {
        this.supplyTraceId = supplyTraceId;
        return this;
    }

    @Override
    public StreamFactoryBuilder setTypeIdSupplier(
        ToIntFunction<String> supplyTypeId)
    {
        this.supplyTypeId = supplyTypeId;
        return this;
    }

    @Override
    public StreamFactoryBuilder setBufferPoolSupplier(
        Supplier<BufferPool> supplyBufferPool)
    {
        this.supplyBufferPool = supplyBufferPool;
        return this;
    }

    @Override
    public StreamFactoryBuilder setBudgetDebitorSupplier(
        LongFunction<BudgetDebitor> supplyDebitor)
    {
        this.supplyDebitor = supplyDebitor;
        return this;
    }

    @Override
    public StreamFactory build()
    {
        return new SseServerFactory(
                config,
                router,
                writeBuffer,
                supplyBufferPool.get(),
                supplyInitialId,
                supplyReplyId,
                supplyTraceId,
                supplyTypeId,
                supplyDebitor);
    }
}
