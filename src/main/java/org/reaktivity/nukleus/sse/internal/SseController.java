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
package org.reaktivity.nukleus.sse.internal;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.nativeOrder;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.ToIntFunction;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.ControllerSpi;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.sse.internal.types.Flyweight;
import org.reaktivity.nukleus.sse.internal.types.control.Role;
import org.reaktivity.nukleus.sse.internal.types.control.RouteFW;
import org.reaktivity.nukleus.sse.internal.types.control.UnrouteFW;
import org.reaktivity.nukleus.sse.internal.types.control.SseRouteExFW;

public final class SseController implements Controller
{
    private static final int MAX_SEND_LENGTH = 1024; // TODO: Configuration and Context

    // TODO: thread-safe flyweights or command queue from public methods
    private final RouteFW.Builder routeRW = new RouteFW.Builder();
    private final UnrouteFW.Builder unrouteRW = new UnrouteFW.Builder();

    private final SseRouteExFW.Builder routeExRW = new SseRouteExFW.Builder();

    private final ControllerSpi controllerSpi;
    private final MutableDirectBuffer writeBuffer;

    public SseController(
        ControllerSpi controllerSpi)
    {
        this.controllerSpi = controllerSpi;
        this.writeBuffer = new UnsafeBuffer(allocateDirect(MAX_SEND_LENGTH).order(nativeOrder()));
    }

    @Override
    public int process()
    {
        return controllerSpi.doProcess();
    }

    @Override
    public void close() throws Exception
    {
        controllerSpi.doClose();
    }

    @Override
    public Class<SseController> kind()
    {
        return SseController.class;
    }

    @Override
    public String name()
    {
        return "sse";
    }

    public <T> T supplySource(
        String source,
        BiFunction<MessagePredicate, ToIntFunction<MessageConsumer>, T> factory)
    {
        return controllerSpi.doSupplySource(source, factory);
    }

    public <T> T supplyTarget(
        String target,
        BiFunction<ToIntFunction<MessageConsumer>, MessagePredicate, T> factory)
    {
        return controllerSpi.doSupplyTarget(target, factory);
    }

    public CompletableFuture<Long> routeServer(
        String source,
        long sourceRef,
        String target,
        long targetRef,
        String pathInfo)
    {
        long correlationId = controllerSpi.nextCorrelationId();

        RouteFW route = routeRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .correlationId(correlationId)
                .role(b -> b.set(Role.SERVER))
                .source(source)
                .sourceRef(sourceRef)
                .target(target)
                .targetRef(targetRef)
                .extension(b -> b.set(visitRouteEx(pathInfo)))
                .build();

        return controllerSpi.doRoute(route.typeId(), route.buffer(), route.offset(), route.sizeof());
    }

    public CompletableFuture<Long> routeClient(
        String source,
        long sourceRef,
        String target,
        long targetRef,
        String pathInfo)
    {
        long correlationId = controllerSpi.nextCorrelationId();

        RouteFW route = routeRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .correlationId(correlationId)
                .role(b -> b.set(Role.CLIENT))
                .source(source)
                .sourceRef(sourceRef)
                .target(target)
                .targetRef(targetRef)
                .extension(b -> b.set(visitRouteEx(pathInfo)))
                .build();

        return controllerSpi.doRoute(route.typeId(), route.buffer(), route.offset(), route.sizeof());
    }

    public CompletableFuture<Void> unrouteServer(
        String source,
        long sourceRef,
        String target,
        long targetRef,
        String pathInfo)
    {
        long correlationId = controllerSpi.nextCorrelationId();

        UnrouteFW unroute = unrouteRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .correlationId(correlationId)
                                     .role(b -> b.set(Role.SERVER))
                                     .source(source)
                                     .sourceRef(sourceRef)
                                     .target(target)
                                     .targetRef(targetRef)
                                     .extension(b -> b.set(visitRouteEx(pathInfo)))
                                     .build();

        return controllerSpi.doUnroute(unroute.typeId(), unroute.buffer(), unroute.offset(), unroute.sizeof());
    }

    public CompletableFuture<Void> unrouteClient(
        String source,
        long sourceRef,
        String target,
        long targetRef,
        String pathInfo)
    {
        long correlationId = controllerSpi.nextCorrelationId();

        UnrouteFW unroute = unrouteRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .correlationId(correlationId)
                                     .role(b -> b.set(Role.CLIENT))
                                     .source(source)
                                     .sourceRef(sourceRef)
                                     .target(target)
                                     .targetRef(targetRef)
                                     .extension(b -> b.set(visitRouteEx(pathInfo)))
                                     .build();

        return controllerSpi.doUnroute(unroute.typeId(), unroute.buffer(), unroute.offset(), unroute.sizeof());
    }

    private Flyweight.Builder.Visitor visitRouteEx(
        String pathInfo)
    {
        return (buffer, offset, limit) ->
            routeExRW.wrap(buffer, offset, limit)
                     .pathInfo(pathInfo)
                     .build()
                     .sizeof();
    }

    public long count(String name)
    {
        return controllerSpi.doCount(name);
    }

}
