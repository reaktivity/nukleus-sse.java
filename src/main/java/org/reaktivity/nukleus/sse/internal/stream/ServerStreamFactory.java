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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.agrona.LangUtil.rethrowUnchecked;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.sse.internal.util.Flags.FIN;
import static org.reaktivity.nukleus.sse.internal.util.Flags.INIT;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.sse.internal.SseConfiguration;
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
import org.reaktivity.nukleus.sse.internal.types.stream.SseEndExFW;
import org.reaktivity.nukleus.sse.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class ServerStreamFactory implements StreamFactory
{
    private static final Pattern QUERY_PARAMS_PATTERN = Pattern.compile("(?<path>[^?]*)(?<query>[\\?].*)");
    private static final Pattern LAST_EVENT_ID_PATTERN = Pattern.compile("(\\?|&)lastEventId=(?<lastEventId>[^&]*)(&|$)");

    public static final int MAXIMUM_HEADER_SIZE =
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
    private final SseEndExFW sseEndExRO = new SseEndExFW();

    private final SseEventFW.Builder sseEventRW = new SseEventFW.Builder();

    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final BufferPool bufferPool;
    private final LongSupplier supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final LongSupplier supplyTrace;
    private final LongSupplier supplyCorrelationId;
    private final DirectBuffer initialComment;

    private final Long2ObjectHashMap<ServerHandshake> correlations;
    private final MessageFunction<RouteFW> wrapRoute;

    public ServerStreamFactory(
        SseConfiguration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongSupplier supplyInitialId,
        LongUnaryOperator supplyReplyId,
        LongSupplier supplyTrace,
        LongSupplier supplyCorrelationId,
        Long2ObjectHashMap<ServerHandshake> correlations)
    {
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.bufferPool = requireNonNull(bufferPool);
        this.supplyInitialId = requireNonNull(supplyInitialId);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.supplyTrace = requireNonNull(supplyTrace);
        this.supplyCorrelationId = requireNonNull(supplyCorrelationId);
        this.correlations = requireNonNull(correlations);
        this.wrapRoute = this::wrapRoute;
        this.initialComment = config.initialComment();
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
        final long streamId = begin.streamId();

        MessageConsumer newStream;

        if ((streamId & 0x8000_0000_0000_0000L) == 0L)
        {
            newStream = newAcceptStream(begin, throttle);
        }
        else
        {
            newStream = newConnectReplyStream(begin, throttle);
        }

        return newStream;
    }

    private MessageConsumer newAcceptStream(
        final BeginFW begin,
        final MessageConsumer acceptThrottle)
    {
        final long acceptRouteId = begin.routeId();

        final MessagePredicate filter = (t, b, o, l) -> true;
        final RouteFW route = router.resolve(acceptRouteId, begin.authorization(), filter, this::wrapRoute);

        MessageConsumer newStream = null;

        if (route != null)
        {
            final long acceptId = begin.streamId();
            newStream = new ServerAcceptStream(
                    acceptThrottle,
                    acceptRouteId,
                    acceptId)::handleStream;
        }

        return newStream;
    }

    private MessageConsumer newConnectReplyStream(
        final BeginFW begin,
        final MessageConsumer applicationReplyThrottle)
    {
        final long applicationRouteId = begin.routeId();
        final long applicationReplyId = begin.streamId();

        return new ServerConnectReplyStream(applicationReplyThrottle, applicationRouteId, applicationReplyId)::handleStream;
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
        private final long acceptRouteId;
        private final long acceptId;

        private MessageConsumer connectTarget;
        private long connectRouteId;
        private long connectId;

        private MessageConsumer streamState;

        private ServerAcceptStream(
            MessageConsumer acceptThrottle,
            long acceptRouteId,
            long acceptId)
        {
            this.acceptThrottle = acceptThrottle;
            this.acceptRouteId = acceptRouteId;
            this.acceptId = acceptId;

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
                doReset(acceptThrottle, acceptRouteId, acceptId);
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
                doReset(acceptThrottle, acceptRouteId, acceptId);
                break;
            }
        }

        private void handleBegin(
            BeginFW begin)
        {
            final long acceptRouteId = begin.routeId();
            final long correlationId = begin.correlationId();
            final OctetsFW extension = begin.extension();
            final long traceId = begin.trace();

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
                            lastEventId = decodeLastEventId(matcher.group("lastEventId"));
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
                final RouteFW route = routeRO.wrap(b, o, o + l);
                final SseRouteExFW routeEx = route.extension().get(sseRouteExRO::wrap);
                final String routePathInfo = routeEx.pathInfo().asString();

                // TODO: process pathInfo matching
                //       && acceptPathInfo.startsWith(pathInfo);
                return true;
            };

            final RouteFW route = router.resolve(acceptRouteId, begin.authorization(), filter, wrapRoute);

            if (route != null)
            {
                final SseRouteExFW sseRouteEx = route.extension().get(sseRouteExRO::wrap);

                final long connectRouteId = route.correlationId();
                final long newConnectId = supplyInitialId.getAsLong();
                final long newCorrelationId = supplyCorrelationId.getAsLong();
                final MessageConsumer connectTarget = router.supplyReceiver(connectRouteId);

                final boolean timestampRequested = httpBeginEx.headers().anyMatch(header ->
                    "accept".equals(header.name().asString()) && header.value().asString().contains("ext=timestamp"));

                ServerHandshake handshake = new ServerHandshake(
                    acceptRouteId,
                    acceptId,
                    correlationId,
                    connectRouteId,
                    timestampRequested);

                correlations.put(newCorrelationId, handshake);

                router.setThrottle(newConnectId, this::handleThrottle);
                doSseBegin(connectTarget, connectRouteId, newConnectId, traceId,
                        newCorrelationId, pathInfo, lastEventId);

                this.connectRouteId = connectRouteId;
                this.connectTarget = connectTarget;
                this.connectId = newConnectId;
            }
            else
            {
                doReset(acceptThrottle, acceptRouteId, acceptId); // 4xx
            }

            this.streamState = this::afterBegin;
        }

        private void handleEnd(
            EndFW end)
        {
            final long traceId = end.trace();
            doSseEnd(connectTarget, connectRouteId, connectId, traceId);
        }

        private void handleAbort(
            AbortFW abort)
        {
            final long traceId = abort.trace();
            // TODO: SseAbortEx
            doSseAbort(connectTarget, connectRouteId, connectId, traceId);
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
            final long traceId = reset.trace();
            doReset(acceptThrottle, acceptRouteId, acceptId, traceId);
        }
    }

    private final class ServerConnectReplyStream
    {
        private final MessageConsumer applicationReplyThrottle;
        private final long applicationRouteId;
        private final long applicationReplyId;

        private MessageConsumer networkReply;
        private long networkRouteId;
        private long networkReplyId;
        private int networkSlot = NO_SLOT;
        int networkSlotOffset;
        boolean deferredEnd;

        private MessageConsumer streamState;

        private int minimumNetworkReplyBudget = -1;
        private int networkReplyBudget;
        private int networkReplyPadding;

        private int applicationReplyBudget;
        private boolean timestampRequested;

        private ServerConnectReplyStream(
            MessageConsumer applicationReplyThrottle,
            long acceptRouteId,
            long applicationReplyId)
        {
            this.applicationReplyThrottle = applicationReplyThrottle;
            this.applicationRouteId = acceptRouteId;
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
                doReset(applicationReplyThrottle, applicationRouteId, applicationReplyId);
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
                doReset(applicationReplyThrottle, applicationRouteId, applicationReplyId);
                break;
            }
        }

        private void handleBegin(
            BeginFW begin)
        {
            final long applicationReplyTraceId = begin.trace();
            final long correlationId = begin.correlationId();

            final ServerHandshake handshake = correlations.remove(correlationId);

            if (handshake != null)
            {
                final long networkRouteId = handshake.networkRouteId();

                final MessageConsumer newNetworkReply = router.supplySender(networkRouteId);
                final long newNetworkReplyId = supplyReplyId.applyAsLong(handshake.networkId());
                final long newCorrelationId = handshake.correlationId();
                this.timestampRequested = handshake.timestampRequested();

                router.setThrottle(newNetworkReplyId, this::handleThrottle);
                if (timestampRequested)
                {
                    doHttpBegin(
                        newNetworkReply,
                        networkRouteId,
                        newNetworkReplyId,
                        newCorrelationId,
                        applicationReplyTraceId,
                        this::setHttpResponseHeadersWithTimestampExt);
                }
                else
                {
                    doHttpBegin(
                        newNetworkReply,
                        networkRouteId,
                        newNetworkReplyId,
                        newCorrelationId,
                        applicationReplyTraceId,
                        this::setHttpResponseHeaders);
                }

                this.networkReply = newNetworkReply;
                this.networkRouteId = networkRouteId;
                this.networkReplyId = newNetworkReplyId;

                this.streamState = this::afterBeginOrData;
            }
            else
            {
                doReset(applicationReplyThrottle, applicationRouteId, applicationReplyId);
            }
        }

        private void handleData(
            DataFW data)
        {
            final long traceId = data.trace();
            final int dataLength = Math.max(data.length(), 0);

            applicationReplyBudget -= dataLength + data.padding();

            if (applicationReplyBudget < 0)
            {
                doReset(applicationReplyThrottle, applicationRouteId, applicationReplyId);
                doSseAbort(networkReply, networkRouteId, networkReplyId, supplyTrace.getAsLong());
            }
            else
            {
                final int flags = data.flags();
                final OctetsFW payload = data.payload();
                final OctetsFW extension = data.extension();

                DirectBuffer id = null;
                DirectBuffer type = null;
                long timestamp = 0L;
                if (extension.sizeof() > 0)
                {
                    final SseDataExFW sseDataEx = extension.get(sseDataExRO::wrap);
                    id = sseDataEx.id().value();
                    type = sseDataEx.type().value();

                    if (timestampRequested)
                    {
                        timestamp = sseDataEx.timestamp();
                    }
                }

                final int bytesWritten = doHttpData(
                    networkReply,
                    networkRouteId,
                    networkReplyId,
                    traceId,
                    flags,
                    networkReplyPadding,
                    payload,
                    id,
                    type,
                    timestamp,
                    null);

                networkReplyBudget -= bytesWritten + networkReplyPadding;
            }
        }

        private void handleEnd(
            EndFW end)
        {
            final long traceId = end.trace();
            final OctetsFW extension = end.extension();

            if (extension.sizeof() > 0)
            {
                final SseEndExFW sseEndEx = extension.get(sseEndExRO::wrap);
                final DirectBuffer id = sseEndEx.id().value();

                int flags = FIN | INIT;
                final Consumer<Builder> payloadMutator = p -> p.set(visitSseEvent(flags, null, id, null, -1L, null));

                final DataFW frame = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                    .routeId(networkRouteId)
                    .streamId(networkReplyId)
                    .trace(traceId)
                    .flags(flags)
                    .groupId(0)
                    .padding(networkReplyPadding)
                    .payload(payloadMutator)
                    .build();

                if (networkReplyBudget >= frame.sizeof() + networkReplyPadding)
                {
                    networkReply.accept(frame.typeId(), frame.buffer(), frame.offset(), frame.sizeof());
                    doHttpEnd(networkReply, networkRouteId, networkReplyId, traceId);
                }
                else
                {
                    // Rare condition where there is insufficient window to write id: last_event_id\n\n
                    networkSlot = bufferPool.acquire(networkReplyId);
                    MutableDirectBuffer buffer = bufferPool.buffer(networkSlot);
                    buffer.putBytes(0, frame.buffer(), frame.offset(), frame.sizeof());
                    networkSlotOffset = frame.sizeof();
                    deferredEnd = true;
                }
            }
            else
            {
                doHttpEnd(networkReply, networkRouteId, networkReplyId, traceId);
            }
        }

        private void handleAbort(
            AbortFW abort)
        {
            final long traceId = abort.trace();
            doHttpAbort(networkReply, networkRouteId, networkReplyId, traceId);
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

            if (minimumNetworkReplyBudget == -1)
            {
                minimumNetworkReplyBudget = window.credit();

                if (initialComment != null)
                {
                    final int bytesWritten = doHttpData(
                            networkReply,
                            networkRouteId,
                            networkReplyId,
                            supplyTrace.getAsLong(),
                            FIN | INIT,
                            networkReplyPadding,
                            null,
                            null,
                            null,
                            0L,
                            initialComment);

                    networkReplyBudget -= bytesWritten + networkReplyPadding;
                    assert networkReplyBudget >= 0;
                }
            }

            if (networkReplyBudget < minimumNetworkReplyBudget)
            {
                // Not sending WINDOW to application side as group budget expects full initial window first time
                // Wait until it builds up to full initial window
                return;
            }
            minimumNetworkReplyBudget = 0;

            if (networkSlot != NO_SLOT && networkReplyBudget >= networkSlotOffset + networkReplyPadding)
            {
                MutableDirectBuffer buffer = bufferPool.buffer(networkSlot);
                DataFW frame = dataRO.wrap(buffer,  0,  networkSlotOffset);
                networkReply.accept(frame.typeId(), frame.buffer(), frame.offset(), frame.sizeof());
                networkReplyBudget -= frame.sizeof() + networkReplyPadding;
                bufferPool.release(networkSlot);
                networkSlot = NO_SLOT;
                if (deferredEnd)
                {
                    doHttpEnd(networkReply, networkRouteId, networkReplyId, frame.trace());
                    deferredEnd = false;
                }
            }

            int applicationReplyPadding = networkReplyPadding + MAXIMUM_HEADER_SIZE;
            final int applicationReplyCredit = networkReplyBudget - applicationReplyBudget;
            if (applicationReplyCredit > 0)
            {
                final long traceId = window.trace();
                doWindow(applicationReplyThrottle, applicationRouteId, applicationReplyId, traceId,
                         applicationReplyCredit, applicationReplyPadding, window.groupId());
                applicationReplyBudget += applicationReplyCredit;
            }
        }

        private void handleReset(
            ResetFW reset)
        {
            final long traceId = reset.trace();
            doReset(applicationReplyThrottle, applicationRouteId, applicationReplyId, traceId);
        }
    }

    private void doHttpBegin(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long correlationId,
        long traceId,
        Consumer<ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> mutator)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .correlationId(correlationId)
                .extension(e -> e.set(visitHttpBeginEx(mutator)))
                .build();

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
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
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        int flags,
        int padding,
        OctetsFW data,
        DirectBuffer id,
        DirectBuffer type,
        long timestamp,
        DirectBuffer comment)
    {
        final Consumer<Builder> payloadMutator = p -> p.set(visitSseEvent(flags, data, id, type, timestamp, comment));

        final DataFW frame = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .flags(flags)
                .groupId(0)
                .padding(padding)
                .payload(payloadMutator)
                .build();

        receiver.accept(frame.typeId(), frame.buffer(), frame.offset(), frame.sizeof());

        return frame.length();
    }

    private void doHttpEnd(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .build();

        receiver.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    private void doHttpAbort(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .build();

        receiver.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    private void doSseBegin(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long correlationId,
        String pathInfo,
        String lastEventId)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .correlationId(correlationId)
                .extension(e -> e.set(visitSseBeginEx(pathInfo, lastEventId)))
                .build();

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    private void doSseAbort(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId)
    {
        // TODO: SseAbortEx
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .build();

        receiver.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    private void doSseEnd(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .build();

        receiver.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
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
        int flags,
        OctetsFW data,
        DirectBuffer id,
        DirectBuffer type,
        long timestamp,
        DirectBuffer comment)
    {
        // TODO: verify valid UTF-8 and no LF chars in payload
        //       would require multiple "data:" lines in event

        return (buffer, offset, limit) ->
            sseEventRW.wrap(buffer, offset, limit)
                      .flags(flags)
                      .timestamp(timestamp)
                      .id(id)
                      .type(type)
                      .data(data)
                      .comment(comment)
                      .build()
                      .sizeof();
    }

    private void doWindow(
        final MessageConsumer sender,
        final long routeId,
        final long streamId,
        final long traceId,
        final int credit,
        final int padding,
        final long groupId)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .credit(credit)
                .padding(padding)
                .groupId(groupId)
                .build();

        sender.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    private void doReset(
        final MessageConsumer sender,
        final long routeId,
        final long streamId,
        final long traceId)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
               .routeId(routeId)
               .streamId(streamId)
               .trace(traceId)
               .build();

        sender.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

    private void doReset(
        final MessageConsumer sender,
        final long routeId,
        final long streamId)
    {
        doReset(sender, routeId, streamId, supplyTrace.getAsLong());
    }

    private static String decodeLastEventId(
        String lastEventId)
    {
        if (lastEventId != null && lastEventId.indexOf('%') != -1)
        {
            try
            {
                lastEventId = URLDecoder.decode(lastEventId, UTF_8.toString());
            }
            catch (UnsupportedEncodingException ex)
            {
                // unexpected, UTF-8 is a supported character set
                rethrowUnchecked(ex);
            }
        }

        return lastEventId;
    }
}
