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

import static java.util.Objects.requireNonNull;

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
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.sse.internal.types.Flyweight;
import org.reaktivity.nukleus.sse.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.sse.internal.types.ListFW;
import org.reaktivity.nukleus.sse.internal.types.OctetsFW;
import org.reaktivity.nukleus.sse.internal.types.OctetsFW.Builder;
import org.reaktivity.nukleus.sse.internal.types.codec.SseEventFW;
import org.reaktivity.nukleus.sse.internal.types.control.RouteFW;
import org.reaktivity.nukleus.sse.internal.types.control.SseRouteExFW;
import org.reaktivity.nukleus.sse.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.sse.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.sse.internal.types.stream.DataFW;
import org.reaktivity.nukleus.sse.internal.types.stream.EndFW;
import org.reaktivity.nukleus.sse.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.sse.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.sse.internal.types.stream.SseBeginExFW;
import org.reaktivity.nukleus.sse.internal.types.stream.SseDataExFW;
import org.reaktivity.nukleus.sse.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class ServerStreamFactory implements StreamFactory
{
    private static final Pattern QUERY_PARAMS_PATTERN = Pattern.compile("(?<path>[^?]*)(?<query>[\\?].*)");
    private static final Pattern LAST_EVENT_ID_PATTERN = Pattern.compile("(\\?|&)lastEventId=(?<lastEventId>[^&]*)(&|$)");

    private static final int MAXIMUM_HEADER_SIZE =
            5 +         // data:
            3 +         // id:
            255 +       // id string
            6 +         // event:
            16 +        // event string
            3;          // \n for data:, id:, event

    private final RouteFW routeRO = new RouteFW();
    private final SseRouteExFW sseRouteExRO = new SseRouteExFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final SseBeginExFW.Builder sseBeginExRW = new SseBeginExFW.Builder();

    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();

    private final HttpBeginExFW httpBeginExRO = new HttpBeginExFW();
    private final HttpBeginExFW.Builder httpBeginExRW = new HttpBeginExFW.Builder();

    private final SseDataExFW sseDataExRO = new SseDataExFW();

    private final SseEventFW.Builder sseEventRW = new SseEventFW.Builder();

    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final LongSupplier supplyStreamId;
    private final LongSupplier supplyTrace;
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
        MutableDirectBuffer writeBuffer,
        LongSupplier supplyStreamId,
        LongSupplier supplyTrace,
        LongSupplier supplyCorrelationId,
        Long2ObjectHashMap<ServerHandshake> correlations,
        Function<RouteFW, LongSupplier> supplyWriteFrameCounter,
        Function<RouteFW, LongSupplier> supplyReadFrameCounter,
        Function<RouteFW, LongConsumer> supplyWriteBytesAccumulator,
        Function<RouteFW, LongConsumer> supplyReadBytesAccumulator)
    {
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.supplyStreamId = requireNonNull(supplyStreamId);
        this.supplyTrace = requireNonNull(supplyTrace);
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
            final LongSupplier readFrameCounter = supplyReadFrameCounter.apply(route);
            final LongConsumer readBytesAccumulator = supplyReadBytesAccumulator.apply(route);
            newStream = new ServerAcceptStream(
                    acceptThrottle,
                    acceptId,
                    acceptRef,
                    readFrameCounter,
                    readBytesAccumulator)::handleStream;
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
        private final LongSupplier readFrameCounter;
        private final LongConsumer readBytesAccumulator;

        private MessageConsumer connectTarget;
        private long connectId;

        private MessageConsumer streamState;

        private ServerAcceptStream(
            MessageConsumer acceptThrottle,
            long acceptId,
            long acceptRef,
            LongSupplier readFrameCounter,
            LongConsumer readBytesAccumulator)
        {
            this.acceptThrottle = acceptThrottle;
            this.acceptId = acceptId;
            this.readFrameCounter = readFrameCounter;
            this.readBytesAccumulator = readBytesAccumulator;

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
                doReset(acceptThrottle, acceptId);
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
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                handleEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                handleAbort(abort);
                break;
            default:
                doReset(acceptThrottle, acceptId);
                break;
            }
        }

        private void handleBegin(
            BeginFW begin)
        {
            final long traceId = begin.trace();
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

                final boolean timestampRequested = httpBeginEx.headers().anyMatch(header ->
                    "accept".equals(header.name().asString()) && header.value().asString().contains("ext=timestamp"));

                ServerHandshake handshake = new ServerHandshake(
                    acceptName,
                    correlationId,
                    readFrameCounter,
                    readBytesAccumulator,
                    timestampRequested);

                correlations.put(newCorrelationId, handshake);

                router.setThrottle(connectName, newConnectId, this::handleThrottle);
                doSseBegin(connectTarget, newConnectId, traceId, connectRef, newCorrelationId, pathInfo, lastEventId);

                this.connectTarget = connectTarget;
                this.connectId = newConnectId;
            }
            else
            {
                doReset(acceptThrottle, acceptId); // 4xx
            }

            this.streamState = this::afterBegin;
        }

        private void handleEnd(
            EndFW end)
        {
            final long traceId = end.trace();
            doSseEnd(connectTarget, connectId, traceId);
        }

        private void handleAbort(
            AbortFW abort)
        {
            final long traceId = abort.trace();
            // TODO: SseAbortEx
            doSseAbort(connectTarget, connectId, traceId);
        }

        private void handleThrottle(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                handleReset(reset);
                break;
            default:
                // ignore
                break;
            }
        }

        private void handleReset(
            ResetFW reset)
        {
            doReset(acceptThrottle, acceptId);
        }
    }

    private final class ServerConnectReplyStream
    {
        private final MessageConsumer applicationReplyThrottle;
        private final long applicationReplyId;

        private MessageConsumer networkReply;
        private long networkReplyId;

        private MessageConsumer streamState;

        private int networkReplyBudget;
        private int networkReplyPadding;

        private int applicationReplyBudget;
        private LongConsumer readBytesAccumulator;
        private LongSupplier readFrameCounter;
        private boolean timestampRequested;

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
                handleBegin(begin);
            }
            else
            {
                doReset(applicationReplyThrottle, applicationReplyId);
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
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                handleData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                handleEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                handleAbort(abort);
                break;
            default:
                doReset(applicationReplyThrottle, applicationReplyId);
                break;
            }
        }

        private void handleBegin(
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
                this.timestampRequested = handshake.timestampRequested();

                router.setThrottle(networkReplyName, newNetworkReplyId, this::handleThrottle);
                if (timestampRequested)
                {
                    doHttpBegin(
                        newNetworkReply,
                        newNetworkReplyId,
                        0L,
                        newCorrelationId,
                        this::setHttpResponseHeadersWithTimestampExt);
                }
                else
                {
                    doHttpBegin(
                        newNetworkReply,
                        newNetworkReplyId,
                        0L,
                        newCorrelationId,
                        this::setHttpResponseHeaders);
                }

                this.networkReply = newNetworkReply;
                this.networkReplyId = newNetworkReplyId;
                this.readBytesAccumulator = handshake.readBytesAccumulator();
                this.readFrameCounter = handshake.readFrameCounter();

                this.streamState = this::afterBeginOrData;
            }
            else
            {
                doReset(applicationReplyThrottle, applicationReplyId);
            }
        }

        private void handleData(
            DataFW data)
        {
            final long traceId = data.trace();
            final int dataLength = Math.max(data.length(), 0);

            this.readFrameCounter.getAsLong();
            this.readBytesAccumulator.accept(dataLength);
            applicationReplyBudget -= dataLength + data.padding();

            if (applicationReplyBudget < 0)
            {
                doReset(applicationReplyThrottle, applicationReplyId);
                doSseAbort(networkReply, networkReplyId, supplyTrace.getAsLong());
            }
            else
            {
                final OctetsFW payload = data.payload();
                final OctetsFW extension = data.extension();

                DirectBuffer id = null;
                DirectBuffer type = null;
                long timestamp = 0;
                if (extension.sizeof() > 0)
                {
                    final SseDataExFW sseDataEx = extension.get(sseDataExRO::wrap);
                    id = sseDataEx.id().value();
                    type = sseDataEx.type().value();
                    timestamp = sseDataEx.timestamp();
                }

                final int bytesWritten = doHttpData(
                    networkReply,
                    networkReplyId,
                    traceId,
                    networkReplyPadding,
                    payload,
                    id,
                    type,
                    timestampRequested,
                    timestamp);
                networkReplyBudget -= bytesWritten + networkReplyPadding;
            }
        }

        private void handleEnd(
            EndFW end)
        {
            final long traceId = end.trace();
            doHttpEnd(networkReply, networkReplyId, traceId);
        }

        private void handleAbort(
            AbortFW abort)
        {
            final long traceId = abort.trace();
            doHttpAbort(networkReply, networkReplyId, traceId);
        }

        private void setHttpResponseHeaders(
            ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW> headers)
        {
            headers.item(h -> h.name(":status").value("200"));
            headers.item(h -> h.name("content-type").value("text/event-stream"));
        }

        private void setHttpResponseHeadersWithTimestampExt(
            ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW> headers)
        {
            headers.item(h -> h.name(":status").value("200"));
            headers.item(h -> h.name("content-type").value("text/event-stream;ext=timestamp"));
        }

        private void handleThrottle(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                handleWindow(window);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                handleReset(reset);
                break;
            default:
                // ignore
                break;
            }
        }

        private void handleWindow(
            WindowFW window)
        {
            networkReplyBudget += window.credit();
            networkReplyPadding = window.padding();

            int applicationReplyPadding = networkReplyPadding + MAXIMUM_HEADER_SIZE;
            final int applicationReplyCredit = networkReplyBudget - applicationReplyBudget;
            if (applicationReplyCredit > 0)
            {
                final long traceId = window.trace();
                doWindow(applicationReplyThrottle, applicationReplyId, traceId,
                         applicationReplyCredit, applicationReplyPadding, 0);
                applicationReplyBudget += applicationReplyCredit;
            }
        }

        private void handleReset(
            ResetFW reset)
        {
            final long traceId = reset.trace();
            doReset(applicationReplyThrottle, applicationReplyId, traceId);
        }
    }

    private void doHttpBegin(
        MessageConsumer stream,
        long streamId,
        long referenceId,
        long correlationId,
        Consumer<ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> mutator)
    {
        BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(streamId)
                .source("sse")
                .sourceRef(referenceId)
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

    private int doHttpData(
        MessageConsumer stream,
        long streamId,
        long traceId,
        int padding,
        OctetsFW eventData,
        DirectBuffer id,
        DirectBuffer type,
        boolean timestampRequested,
        long timestamp)
    {
        final Consumer<Builder> payloadMutator = timestampRequested ?
                p -> p.set(visitSseEvent(eventData, id, type, timestamp)):
                p -> p.set(visitSseEvent(eventData, id, type));

        DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
            .streamId(streamId)
            .trace(traceId)
            .groupId(0)
            .padding(padding)
            .payload(payloadMutator)
            .build();

        stream.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());

        return data.length();
    }

    private void doHttpEnd(
        MessageConsumer stream,
        long streamId,
        long traceId)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                               .streamId(streamId)
                               .trace(traceId)
                               .build();

        stream.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    private void doHttpAbort(
        MessageConsumer stream,
        long streamId,
        long traceId)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .streamId(streamId)
                                     .trace(traceId)
                                     .build();

        stream.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    private void doSseBegin(
        MessageConsumer stream,
        long streamId,
        long traceId,
        long streamRef,
        long correlationId,
        String pathInfo,
        String lastEventId)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .streamId(streamId)
                                     .trace(traceId)
                                     .source("sse")
                                     .sourceRef(streamRef)
                                     .correlationId(correlationId)
                                     .extension(e -> e.set(visitSseBeginEx(pathInfo, lastEventId)))
                                     .build();

        stream.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    private void doSseAbort(
        MessageConsumer stream,
        long streamId,
        long traceId)
    {
        // TODO: SseAbortEx
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(streamId)
                .trace(traceId)
                .build();

        stream.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    private void doSseEnd(
        MessageConsumer stream,
        long streamId,
        long traceId)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                               .streamId(streamId)
                               .trace(traceId)
                               .build();

        stream.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
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

    private Flyweight.Builder.Visitor visitSseEvent(
        OctetsFW data,
        DirectBuffer id,
        DirectBuffer type)
    {
        // TODO: verify valid UTF-8 and no LF chars in payload
        //       would require multiple "data:" lines in event

        return (buffer, offset, limit) ->
            sseEventRW.wrap(buffer, offset, limit)
                      .data(data)
                      .id(id)
                      .type(type)
                      .build()
                      .sizeof();
    }

    private Flyweight.Builder.Visitor visitSseEvent(
        OctetsFW data,
        DirectBuffer id,
        DirectBuffer type,
        long timestamp)
    {
        // TODO: verify valid UTF-8 and no LF chars in payload
        //       would require multiple "data:" lines in event

        return (buffer, offset, limit) ->
            sseEventRW.wrap(buffer, offset, limit)
                      .data(data)
                      .timestamp(timestamp)
                      .id(id)
                      .type(type)
                      .build()
                      .sizeof();
    }

    private void doWindow(
        final MessageConsumer throttle,
        final long throttleId,
        final long traceId,
        final int credit,
        final int padding,
        final int groupId)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(throttleId)
                .trace(traceId)
                .credit(credit)
                .padding(padding)
                .groupId(groupId)
                .build();

        throttle.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    private void doReset(
        final MessageConsumer throttle,
        final long throttleId,
        final long traceId)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
               .streamId(throttleId)
               .trace(traceId)
               .build();

        throttle.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

    private void doReset(
        final MessageConsumer throttle,
        final long throttleId)
    {
        doReset(throttle, throttleId, supplyTrace.getAsLong());
    }
}
