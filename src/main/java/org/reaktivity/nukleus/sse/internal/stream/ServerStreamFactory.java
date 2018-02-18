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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.sse.internal.types.stream.Flag.RST;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.buffer.MemoryManager;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.sse.internal.types.Flyweight;
import org.reaktivity.nukleus.sse.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.sse.internal.types.ListFW;
import org.reaktivity.nukleus.sse.internal.types.ListFW.Builder;
import org.reaktivity.nukleus.sse.internal.types.OctetsFW;
import org.reaktivity.nukleus.sse.internal.types.control.RouteFW;
import org.reaktivity.nukleus.sse.internal.types.control.SseRouteExFW;
import org.reaktivity.nukleus.sse.internal.types.stream.AckFW;
import org.reaktivity.nukleus.sse.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.sse.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.sse.internal.types.stream.RegionFW;
import org.reaktivity.nukleus.sse.internal.types.stream.SseBeginExFW;
import org.reaktivity.nukleus.sse.internal.types.stream.SseDataExFW;
import org.reaktivity.nukleus.sse.internal.types.stream.TransferFW;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class ServerStreamFactory implements StreamFactory
{
    private static final byte[] DATA_BYTES = "data:".getBytes(UTF_8);
    private static final byte[] LF_ID_BYTES = "\nid:".getBytes(UTF_8);
    private static final byte[] LF_TYPE_BYTES = "\nevent:".getBytes(UTF_8);
    private static final byte[] LF_LF_BYTES = "\n\n".getBytes(UTF_8);

    private static final Pattern QUERY_PARAMS_PATTERN = Pattern.compile("(?<path>[^?]*)(?<query>[\\?].*)");
    private static final Pattern LAST_EVENT_ID_PATTERN = Pattern.compile("(\\?|&)lastEventId=(?<lastEventId>[^&]*)(&|$)");

    private final RouteFW routeRO = new RouteFW();
    private final SseRouteExFW sseRouteExRO = new SseRouteExFW();

    private final BeginFW beginRO = new BeginFW();
    private final TransferFW transferRO = new TransferFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final TransferFW.Builder transferRW = new TransferFW.Builder();

    private final AckFW ackRO = new AckFW();

    private final SseBeginExFW.Builder sseBeginExRW = new SseBeginExFW.Builder();

    private final AckFW.Builder ackRW = new AckFW.Builder();

    private final HttpBeginExFW httpBeginExRO = new HttpBeginExFW();
    private final HttpBeginExFW.Builder httpBeginExRW = new HttpBeginExFW.Builder();

    private final SseDataExFW sseDataExRO = new SseDataExFW();

    private final MutableDirectBuffer sseEventRW = new UnsafeBuffer(new byte[0]);

    private final RouteManager router;
    private final MemoryManager memory;
    private final MutableDirectBuffer writeBuffer;
    private final LongSupplier supplyStreamId;
    private final LongSupplier supplyCorrelationId;

    private final Long2ObjectHashMap<ServerHandshake> correlations;
    private final MessageFunction<RouteFW> wrapRoute;

    private final Function<RouteFW, LongSupplier> supplyWriteFrameCounter;
    private final Function<RouteFW, LongSupplier> supplyReadFrameCounter;
    private final Function<RouteFW, LongConsumer> supplyWriteBytesAccumulator;
    private final Function<RouteFW, LongConsumer> supplyReadBytesAccumulator;

    public ServerStreamFactory(
        Configuration config,
        RouteManager router,
        MemoryManager memory,
        MutableDirectBuffer writeBuffer,
        LongSupplier supplyStreamId,
        LongSupplier supplyCorrelationId,
        Long2ObjectHashMap<ServerHandshake> correlations,
        Function<RouteFW, LongSupplier> supplyWriteFrameCounter,
        Function<RouteFW, LongSupplier> supplyReadFrameCounter,
        Function<RouteFW, LongConsumer> supplyWriteBytesAccumulator,
        Function<RouteFW, LongConsumer> supplyReadBytesAccumulator)
    {
        this.router = requireNonNull(router);
        this.memory = requireNonNull(memory);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.supplyStreamId = requireNonNull(supplyStreamId);
        this.supplyCorrelationId = requireNonNull(supplyCorrelationId);
        this.correlations = requireNonNull(correlations);
        this.wrapRoute = this::wrapRoute;
        this.supplyWriteFrameCounter = supplyWriteFrameCounter;
        this.supplyReadFrameCounter = supplyReadFrameCounter;
        this.supplyWriteBytesAccumulator = supplyWriteBytesAccumulator;
        this.supplyReadBytesAccumulator = supplyReadBytesAccumulator;
    }

    @Override
    public MessageConsumer newStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length,
        MessageConsumer throttle)
    {
        final BeginFW begin = beginRO.wrap(buffer, index, index + length);
        final long sourceRef = begin.sourceRef();

        MessageConsumer newStream;

        if (sourceRef == 0L)
        {
            newStream = newConnectReplyStream(begin, throttle);
        }
        else
        {
            newStream = newAcceptStream(begin, throttle);
        }

        return newStream;
    }

    private MessageConsumer newAcceptStream(
        final BeginFW begin,
        final MessageConsumer acceptThrottle)
    {
        final long acceptRef = begin.sourceRef();
        final String acceptName = begin.source().asString();

        final MessagePredicate filter = (t, b, o, l) ->
        {
            final RouteFW route = routeRO.wrap(b, o, l);
            return acceptRef == route.sourceRef() &&
                    acceptName.equals(route.source().asString());
        };

        final RouteFW route = router.resolve(begin.authorization(), filter, this::wrapRoute);

        MessageConsumer newStream = null;

        if (route != null)
        {
            final long acceptId = begin.streamId();
            final LongSupplier countReadFrames = supplyReadFrameCounter.apply(route);
            final LongConsumer countReadBytes = supplyReadBytesAccumulator.apply(route);
            newStream = new ServerAcceptStream(
                    acceptThrottle,
                    acceptId,
                    acceptRef,
                    countReadFrames,
                    countReadBytes)::handleStream;
        }

        return newStream;
    }

    private MessageConsumer newConnectReplyStream(
        final BeginFW begin,
        final MessageConsumer connectReplyThrottle)
    {
        final long connectReplyId = begin.streamId();

        return new ServerConnectReplyStream(connectReplyThrottle, connectReplyId)::handleStream;
    }

    private RouteFW wrapRoute(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        return routeRO.wrap(buffer, index, index + length);
    }

    private final class ServerAcceptStream
    {
        private final MessageConsumer acceptThrottle;
        private final long acceptId;
        private final LongSupplier countReadFrames;
        private final LongConsumer countReadBytes;

        private MessageConsumer connectTarget;
        private long connectId;

        private MessageConsumer streamState;

        private ServerAcceptStream(
            MessageConsumer acceptThrottle,
            long acceptId,
            long acceptRef,
            LongSupplier countReadFrames,
            LongConsumer countReadBytes)
        {
            this.acceptThrottle = acceptThrottle;
            this.acceptId = acceptId;
            this.countReadFrames = countReadFrames;
            this.countReadBytes = countReadBytes;

            this.streamState = this::beforeBegin;
        }

        private void handleStream(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            streamState.accept(msgTypeId, buffer, index, length);
        }

        private void beforeBegin(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            if (msgTypeId == BeginFW.TYPE_ID)
            {
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                handleBegin(begin);
            }
            else
            {
                doAck(acceptThrottle, acceptId, RST.flag());
            }
        }

        private void afterBegin(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case TransferFW.TYPE_ID:
                final TransferFW transfer = transferRO.wrap(buffer, index, index + length);
                onTransfer(transfer);
                break;
            default:
                doAck(acceptThrottle, acceptId, RST.flag());
                break;
            }
        }

        private void handleBegin(
            BeginFW begin)
        {
            final String acceptName = begin.source().asString();
            final long acceptRef = begin.sourceRef();
            final long correlationId = begin.correlationId();
            final OctetsFW extension = begin.extension();

            // TODO: need lightweight approach (start)
            final HttpBeginExFW httpBeginEx = extension.get(httpBeginExRO::wrap);
            final Map<String, String> headers = new LinkedHashMap<>();
            httpBeginEx.headers().forEach(header ->
            {
                final String name = header.name().asString();
                final String value = header.value().asString();
                headers.merge(name, value, (v1, v2) -> String.format("%s, %s", v1, v2));
            });

            String pathInfo = headers.get(":path"); // TODO: ":pathinfo" ?
            String lastEventId = headers.get("last-event-id");

            // extract lastEventId query parameter from pathInfo
            // use query parameter value as default for missing Last-Event-ID header
            if (pathInfo != null)
            {
                Matcher matcher = QUERY_PARAMS_PATTERN.matcher(pathInfo);
                if (matcher.matches())
                {
                    String path = matcher.group("path");
                    String query = matcher.group("query");

                    matcher = LAST_EVENT_ID_PATTERN.matcher(query);
                    StringBuffer builder = new StringBuffer(path);
                    while (matcher.find())
                    {
                        if (lastEventId == null)
                        {
                            lastEventId = matcher.group("lastEventId");
                        }

                        String replacement = matcher.group(3).isEmpty() ? "$3" : "$1";
                        matcher.appendReplacement(builder, replacement);
                    }
                    matcher.appendTail(builder);
                    pathInfo = builder.toString();
                }
            }

            // TODO: need lightweight approach (end)

            final MessagePredicate filter = (t, b, o, l) ->
            {
                final RouteFW route = routeRO.wrap(b, o, l);
                final SseRouteExFW routeEx = route.extension().get(sseRouteExRO::wrap);
                final String routePathInfo = routeEx.pathInfo().asString();

                return acceptRef == route.sourceRef() &&
                        acceptName.equals(route.source().asString());

                // TODO: process pathInfo matching
                //       && acceptPathInfo.startsWith(pathInfo);
            };

            final RouteFW route = router.resolve(begin.authorization(), filter, wrapRoute);

            if (route != null)
            {
                final SseRouteExFW sseRouteEx = route.extension().get(sseRouteExRO::wrap);

                final String connectName = route.target().asString();
                final MessageConsumer connectTarget = router.supplyTarget(connectName);

                final long connectRef = route.targetRef();
                final long newConnectId = supplyStreamId.getAsLong();
                final long newCorrelationId = supplyCorrelationId.getAsLong();

                ServerHandshake handshake = new ServerHandshake(
                    acceptName,
                    correlationId,
                    countReadFrames,
                    countReadBytes);

                correlations.put(newCorrelationId, handshake);

                doSseBegin(connectTarget, newConnectId, connectRef, newCorrelationId, pathInfo, lastEventId);
                router.setThrottle(connectName, newConnectId, this::handleThrottle);

                this.connectTarget = connectTarget;
                this.connectId = newConnectId;
            }
            else
            {
                doAck(acceptThrottle, acceptId, RST.flag()); // 4xx
            }

            this.streamState = this::afterBegin;
        }

        private void onTransfer(
            TransferFW transfer)
        {
            final int flags = transfer.flags();
            doSseTransfer(connectTarget, connectId, flags);
        }

        private void handleThrottle(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case AckFW.TYPE_ID:
                final AckFW ack = ackRO.wrap(buffer, index, index + length);
                onAck(ack);
                break;
            default:
                // ignore
                break;
            }
        }

        private void onAck(
            AckFW ack)
        {
            final int flags = ack.flags();

            assert ack.regions().isEmpty();

            doAck(acceptThrottle, acceptId, flags);
        }
    }

    private final class ServerConnectReplyStream
    {
        private final MessageConsumer applicationReplyThrottle;
        private final long applicationReplyId;

        private MessageConsumer networkReply;
        private long networkReplyId;

        private MessageConsumer streamState;

        private LongConsumer countReadBytes;
        private LongSupplier countReadFrames;
        private int ackProgress;

        private ServerConnectReplyStream(
            MessageConsumer applicationReplyThrottle,
            long applicationReplyId)
        {
            this.applicationReplyThrottle = applicationReplyThrottle;
            this.applicationReplyId = applicationReplyId;
            this.streamState = this::beforeBegin;
        }

        private void handleStream(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            streamState.accept(msgTypeId, buffer, index, length);
        }

        private void beforeBegin(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            if (msgTypeId == BeginFW.TYPE_ID)
            {
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onBegin(begin);
            }
            else
            {
                doAck(applicationReplyThrottle, applicationReplyId, RST.flag());
            }
        }

        private void afterBeginOrData(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case TransferFW.TYPE_ID:
                final TransferFW transfer = transferRO.wrap(buffer, index, index + length);
                onTransfer(transfer);
                break;
            default:
                doAck(applicationReplyThrottle, applicationReplyId, RST.flag());
                break;
            }
        }

        private void onBegin(
            BeginFW begin)
        {
            final long applicationReplyRef = begin.sourceRef();
            final long correlationId = begin.correlationId();

            final ServerHandshake handshake = correlations.remove(correlationId);

            if (applicationReplyRef == 0L && handshake != null)
            {
                final String networkReplyName = handshake.networkName();

                final MessageConsumer newNetworkReply = router.supplyTarget(networkReplyName);
                final long newNetworkReplyId = supplyStreamId.getAsLong();
                final long newCorrelationId = handshake.correlationId();

                doHttpBegin(newNetworkReply, newNetworkReplyId, 0L, newCorrelationId, this::setHttpResponseHeaders);
                router.setThrottle(networkReplyName, newNetworkReplyId, this::handleThrottle);

                this.networkReply = newNetworkReply;
                this.networkReplyId = newNetworkReplyId;
                this.countReadFrames = handshake.countReadFrames();
                this.countReadBytes = handshake.countReadBytes();

                this.streamState = this::afterBeginOrData;
            }
            else
            {
                doAck(applicationReplyThrottle, applicationReplyId, RST.flag());
            }
        }

        private void onTransfer(
            TransferFW transfer)
        {
            final int flags = transfer.flags();
            final ListFW<RegionFW> regions = transfer.regions();
            final OctetsFW extension = transfer.extension();

            countReadFrames.getAsLong();

            if (regions.isEmpty() && extension.sizeof() == 0)
            {
                doHttpTransfer(networkReply, networkReplyId, flags);
            }
            else
            {
                regions.forEach(r -> countReadBytes.accept(r.length()));
                doHttpTransfer(networkReply, networkReplyId, flags, builder -> transferSseEvent(builder, transfer));
            }
        }

        private void transferSseEvent(
            Builder<RegionFW.Builder, RegionFW> builder,
            TransferFW transfer)
        {
            final ListFW<RegionFW> regions = transfer.regions();
            final OctetsFW extension = transfer.extension();

            String id = null;
            String type = null;
            if (extension.sizeof() > 0)
            {
                final SseDataExFW sseDataEx = extension.get(sseDataExRO::wrap);
                id = sseDataEx.id().asString();
                type = sseDataEx.type().asString();
            }

            int allocation = DATA_BYTES.length + LF_LF_BYTES.length +
                             LF_ID_BYTES.length + LF_TYPE_BYTES.length +
                             extension.sizeof();

            // TODO: handle address == -1L
            final long address = memory.acquire(allocation);
            sseEventRW.wrap(memory.resolve(address), allocation);

            // "data:[data]\n"  TODO: split on \n
            // "id:<id>\n"
            // "type:<type>\n"
            // "\n"
            int indexOfHeader = 0;
            int limitOfHeader = indexOfHeader;
            if (!regions.isEmpty())
            {
                sseEventRW.putBytes(limitOfHeader, DATA_BYTES);
                limitOfHeader += DATA_BYTES.length;

                long addressOfHeader = address + indexOfHeader;
                int sizeOfHeader = limitOfHeader - indexOfHeader;
                builder.item(r -> r.address(addressOfHeader)
                                   .length(sizeOfHeader)
                                   .streamId(networkReplyId));
                regions.forEach(r -> builder.item(i -> i.address(r.address())
                                                        .length(r.length())
                                                        .streamId(r.streamId())));
            }

            int indexOfTrailer = limitOfHeader;
            int limitOfTrailer = indexOfTrailer;
            if (id != null)
            {
                sseEventRW.putBytes(limitOfTrailer, LF_ID_BYTES);
                limitOfTrailer += LF_ID_BYTES.length;
                limitOfTrailer += sseEventRW.putStringWithoutLengthUtf8(limitOfTrailer, id);
            }
            if (type != null)
            {
                sseEventRW.putBytes(limitOfTrailer, LF_TYPE_BYTES);
                limitOfTrailer += LF_TYPE_BYTES.length;
                limitOfTrailer += sseEventRW.putStringWithoutLengthUtf8(limitOfTrailer, type);
            }
            sseEventRW.putBytes(limitOfTrailer, LF_LF_BYTES);
            limitOfTrailer += LF_LF_BYTES.length;

            long addressOfTrailer = address + indexOfTrailer;
            int sizeOfTrailer = limitOfTrailer - indexOfTrailer;

            builder.item(r -> r.address(addressOfTrailer)
                               .length(sizeOfTrailer)
                               .streamId(networkReplyId));

        }

        private void setHttpResponseHeaders(
            ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW> headers)
        {
            headers.item(h -> h.name(":status").value("200"));
            headers.item(h -> h.name("content-type").value("text/event-stream"));
        }

        private void handleThrottle(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case AckFW.TYPE_ID:
                final AckFW ack = ackRO.wrap(buffer, index, index + length);
                onAck(ack);
                break;
            default:
                // ignore
                break;
            }
        }

        private void onAck(
            AckFW ack)
        {
            final int flags = ack.flags();
            final ListFW<RegionFW> regions = ack.regions();

            if (regions.isEmpty())
            {
                doAck(applicationReplyThrottle, applicationReplyId, flags);
            }
            else
            {
                doAck(applicationReplyThrottle, applicationReplyId, flags, builder -> ackSseEvent(builder, ack));
            }
        }

        private void ackSseEvent(
            ListFW.Builder<RegionFW.Builder, RegionFW> builder,
            AckFW ack)
        {
            final ListFW<RegionFW> regions = ack.regions();
            if (!regions.isEmpty())
            {
                regions.forEach(r -> ackSseEventRegion(builder, r));
            }
        }

        private void ackSseEventRegion(
            ListFW.Builder<RegionFW.Builder, RegionFW> builder,
            RegionFW region)
        {
            if (region.streamId() == networkReplyId)
            {
                if (ackProgress == 0)
                {
                    ackProgress = region.length();
                }
                else
                {
                    memory.release(region.address(), ackProgress + region.length());
                    ackProgress = 0;
                }
            }
            else
            {
                builder.item(i -> i.address(region.address())
                                   .length(region.length())
                                   .streamId(region.streamId()));
            }
        }
    }

    private void doHttpBegin(
        MessageConsumer stream,
        long targetId,
        long targetRef,
        long correlationId,
        Consumer<ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> mutator)
    {
        BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .source("sse")
                .sourceRef(targetRef)
                .correlationId(correlationId)
                .extension(e -> e.set(visitHttpBeginEx(mutator)))
                .build();

        stream.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    private Flyweight.Builder.Visitor visitHttpBeginEx(
        Consumer<ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> headers)
    {
        return (buffer, offset, limit) ->
            httpBeginExRW.wrap(buffer, offset, limit)
                         .headers(headers)
                         .build()
                         .sizeof();
    }

    private void doHttpTransfer(
        MessageConsumer stream,
        long targetId,
        int flags,
        Consumer<Builder<RegionFW.Builder, RegionFW>> regions)
    {
        TransferFW transfer = transferRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .flags(flags)
                .regions(regions)
                .build();

        stream.accept(transfer.typeId(), transfer.buffer(), transfer.offset(), transfer.sizeof());
    }

    private void doHttpTransfer(
        MessageConsumer stream,
        long targetId,
        int flags)
    {
        TransferFW transfer = transferRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .flags(flags)
                .build();

        stream.accept(transfer.typeId(), transfer.buffer(), transfer.offset(), transfer.sizeof());
    }

    private void doSseBegin(
        MessageConsumer stream,
        long streamId,
        long streamRef,
        long correlationId,
        String pathInfo,
        String lastEventId)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .streamId(streamId)
                                     .source("sse")
                                     .sourceRef(streamRef)
                                     .correlationId(correlationId)
                                     .extension(e -> e.set(visitSseBeginEx(pathInfo, lastEventId)))
                                     .build();

        stream.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    private void doSseTransfer(
        MessageConsumer stream,
        long streamId,
        int flags)
    {
        final TransferFW transfer = transferRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                              .streamId(streamId)
                                              .flags(flags)
                                              .build();

        stream.accept(transfer.typeId(), transfer.buffer(), transfer.offset(), transfer.sizeof());
    }

    private Flyweight.Builder.Visitor visitSseBeginEx(
        String pathInfo,
        String lastEventId)
    {
        return (buffer, offset, limit) ->
            sseBeginExRW.wrap(buffer, offset, limit)
                       .pathInfo(pathInfo)
                       .lastEventId(lastEventId)
                       .build()
                       .sizeof();
    }

    private void doAck(
        MessageConsumer throttle,
        long throttleId,
        int flags,
        Consumer<ListFW.Builder<RegionFW.Builder, RegionFW>> regions)
    {
        final AckFW ack = ackRW.wrap(writeBuffer, 0, writeBuffer.capacity())
               .streamId(throttleId)
               .flags(flags)
               .regions(regions)
               .build();

        throttle.accept(ack.typeId(), ack.buffer(), ack.offset(), ack.sizeof());
    }

    private void doAck(
        MessageConsumer throttle,
        long throttleId,
        int flags)
    {
        final AckFW reset = ackRW.wrap(writeBuffer, 0, writeBuffer.capacity())
               .streamId(throttleId)
               .flags(flags)
               .build();

        throttle.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }
}
