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
import java.util.function.LongSupplier;

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
    private static final int MAXIMUM_DATA_LENGTH = (1 << Short.SIZE) - 1;
    private static final int MAXIMUM_HEADER_SIZE =
            5 +         // data:
            3 +         // id:
            256 +       // id string
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
    private final LongSupplier supplyCorrelationId;

    private final Long2ObjectHashMap<ServerHandshake> correlations;
    private final MessageFunction<RouteFW> wrapRoute;

    public ServerStreamFactory(
        Configuration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        LongSupplier supplyStreamId,
        LongSupplier supplyCorrelationId,
        Long2ObjectHashMap<ServerHandshake> correlations)
    {
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.supplyStreamId = requireNonNull(supplyStreamId);
        this.supplyCorrelationId = requireNonNull(supplyCorrelationId);
        this.correlations = requireNonNull(correlations);
        this.wrapRoute = this::wrapRoute;
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

            newStream = new ServerAcceptStream(acceptThrottle, acceptId, acceptRef)::handleStream;
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

        private MessageConsumer connectTarget;
        private long connectId;

        private MessageConsumer streamState;

        private ServerAcceptStream(
            MessageConsumer acceptThrottle,
            long acceptId,
            long acceptRef)
        {
            this.acceptThrottle = acceptThrottle;
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

            final String pathInfo = headers.get(":path"); // TODO: ":pathinfo" ?
            final String lastEventId = headers.get("last-event-id");
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

                ServerHandshake handshake = new ServerHandshake(acceptName, correlationId);

                correlations.put(newCorrelationId, handshake);

                // TODO: begin - support optional lastEventId string in sse.idl
                String lastEventIdNonNull = lastEventId;
                if (lastEventIdNonNull == null)
                {
                    lastEventIdNonNull = "";
                }
                // TODO: end

                doSseBegin(connectTarget, newConnectId, connectRef, newCorrelationId, pathInfo, lastEventIdNonNull);
                router.setThrottle(connectName, newConnectId, this::handleThrottle);

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
            doSseEnd(connectTarget, connectId);
        }

        private void handleAbort(
            AbortFW abort)
        {
            // TODO: SseAbortEx
            doSseAbort(connectTarget, connectId);
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
        private final MessageConsumer connectReplyThrottle;
        private final long connectReplyId;

        private MessageConsumer acceptReply;
        private long acceptReplyId;

        private MessageConsumer streamState;

        private int targetWindowBudget;
        private int targetWindowBudgetAdjustment;
        private int targetWindowPadding;

        private ServerConnectReplyStream(
            MessageConsumer connectReplyThrottle,
            long connectReplyId)
        {
            this.connectReplyThrottle = connectReplyThrottle;
            this.connectReplyId = connectReplyId;
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
                doReset(connectReplyThrottle, connectReplyId);
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
                doReset(connectReplyThrottle, connectReplyId);
                break;
            }
        }

        private void handleBegin(
            BeginFW begin)
        {
            final long connectRef = begin.sourceRef();
            final long correlationId = begin.correlationId();

            final ServerHandshake handshake = correlations.remove(correlationId);

            if (connectRef == 0L && handshake != null)
            {
                final String acceptReplyName = handshake.acceptName();

                final MessageConsumer newAcceptReply = router.supplyTarget(acceptReplyName);
                final long newAcceptReplyId = supplyStreamId.getAsLong();
                final long newCorrelationId = handshake.correlationId();

                doHttpBegin(newAcceptReply, newAcceptReplyId, 0L, newCorrelationId, this::setHttpResponseHeaders);
                router.setThrottle(acceptReplyName, newAcceptReplyId, this::handleThrottle);

                this.acceptReply = newAcceptReply;
                this.acceptReplyId = newAcceptReplyId;

                this.streamState = this::afterBeginOrData;
            }
            else
            {
                doReset(connectReplyThrottle, connectReplyId);
            }
        }

        private void handleData(
            DataFW data)
        {
            targetWindowBudget -= data.length() + targetWindowPadding;

            if (targetWindowBudget < 0)
            {
                doReset(connectReplyThrottle, connectReplyId);
            }
            else
            {
                final OctetsFW payload = data.payload();
                final OctetsFW extension = data.extension();

                String id = null;
                if (extension.sizeof() > 0)
                {
                    final SseDataExFW sseDataEx = extension.get(sseDataExRO::wrap);
                    id = sseDataEx.id().asString();
                }

                final int sseBytesConsumed = payload.sizeof();
                final int sseBytesProduced = doHttpData(acceptReply, acceptReplyId, payload, id);

                targetWindowBudgetAdjustment += MAXIMUM_HEADER_SIZE - (sseBytesProduced - sseBytesConsumed);
            }
        }

        private void handleEnd(
            EndFW end)
        {
            doHttpEnd(acceptReply, acceptReplyId);
        }

        private void handleAbort(
            AbortFW abort)
        {
            doHttpAbort(acceptReply, acceptReplyId);
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

        private void handleWindow(WindowFW window)
        {
            final int targetWindowCredit = window.credit() + targetWindowBudgetAdjustment;
            targetWindowPadding = window.padding() + MAXIMUM_HEADER_SIZE;
            targetWindowBudget += targetWindowCredit;
            targetWindowBudgetAdjustment = 0;
            doWindow(connectReplyThrottle, connectReplyId, targetWindowCredit, targetWindowPadding);
        }

        private void handleReset(
            ResetFW reset)
        {
            doReset(connectReplyThrottle, connectReplyId);
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

    private int doHttpData(
        MessageConsumer stream,
        long targetId,
        OctetsFW eventOctets,
        String eventId)
    {
        String eventData = eventOctets.buffer().getStringWithoutLengthUtf8(eventOctets.offset(), eventOctets.sizeof());

        DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .payload(p -> p.set(visitSseEvent(eventData, eventId)))
                .build();

        stream.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());

        return data.sizeof();
    }

    private void doHttpEnd(
        MessageConsumer stream,
        long streamId)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                               .streamId(streamId)
                               .build();

        stream.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    private void doHttpAbort(
        MessageConsumer stream,
        long streamId)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .streamId(streamId)
                                     .build();

        stream.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
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

    private void doSseAbort(
        MessageConsumer stream,
        long streamId)
    {
        // TODO: SseAbortEx
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(streamId)
                .build();

        stream.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    private void doSseEnd(
        MessageConsumer stream,
        long streamId)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                               .streamId(streamId)
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
        String data,
        String id)
    {
        // TODO: verify valid UTF-8 and no LF chars in payload
        //       would require multiple "data:" lines in event

        return (buffer, offset, limit) ->
            sseEventRW.wrap(buffer, offset, limit)
                      .data(data)
                      .id(id)
                      .build()
                      .sizeof();
    }

    private void doWindow(
        final MessageConsumer throttle,
        final long throttleId,
        final int credit,
        final int padding)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(throttleId)
                .credit(credit)
                .padding(padding)
                .build();

        throttle.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    private void doReset(
        final MessageConsumer throttle,
        final long throttleId)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
               .streamId(throttleId)
               .build();

        throttle.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }
}
