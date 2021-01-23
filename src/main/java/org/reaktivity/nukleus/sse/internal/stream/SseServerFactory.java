/**
 * Copyright 2016-2020 The Reaktivity Project
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
import static org.agrona.BitUtil.SIZE_OF_BYTE;
import static org.agrona.LangUtil.rethrowUnchecked;
import static org.reaktivity.nukleus.budget.BudgetDebitor.NO_DEBITOR_INDEX;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.sse.internal.util.Flags.FIN;
import static org.reaktivity.nukleus.sse.internal.util.Flags.INIT;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.ToIntFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.budget.BudgetDebitor;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.sse.internal.SseConfiguration;
import org.reaktivity.nukleus.sse.internal.SseNukleus;
import org.reaktivity.nukleus.sse.internal.types.Array32FW;
import org.reaktivity.nukleus.sse.internal.types.Flyweight;
import org.reaktivity.nukleus.sse.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.sse.internal.types.OctetsFW;
import org.reaktivity.nukleus.sse.internal.types.String16FW;
import org.reaktivity.nukleus.sse.internal.types.String8FW;
import org.reaktivity.nukleus.sse.internal.types.codec.SseEventFW;
import org.reaktivity.nukleus.sse.internal.types.control.Capability;
import org.reaktivity.nukleus.sse.internal.types.control.RouteFW;
import org.reaktivity.nukleus.sse.internal.types.control.SseRouteExFW;
import org.reaktivity.nukleus.sse.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.sse.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.sse.internal.types.stream.ChallengeFW;
import org.reaktivity.nukleus.sse.internal.types.stream.DataFW;
import org.reaktivity.nukleus.sse.internal.types.stream.EndFW;
import org.reaktivity.nukleus.sse.internal.types.stream.FlushFW;
import org.reaktivity.nukleus.sse.internal.types.stream.FrameFW;
import org.reaktivity.nukleus.sse.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.sse.internal.types.stream.HttpChallengeExFW;
import org.reaktivity.nukleus.sse.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.sse.internal.types.stream.SseBeginExFW;
import org.reaktivity.nukleus.sse.internal.types.stream.SseDataExFW;
import org.reaktivity.nukleus.sse.internal.types.stream.SseEndExFW;
import org.reaktivity.nukleus.sse.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.sse.internal.util.Flags;
import org.reaktivity.nukleus.stream.StreamFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public final class SseServerFactory implements StreamFactory
{
    private static final String HTTP_TYPE_NAME = "http";

    private static final String8FW HEADER_NAME_METHOD = new String8FW(":method");
    private static final String8FW HEADER_NAME_PATH = new String8FW(":path");
    private static final String8FW HEADER_NAME_STATUS = new String8FW(":status");
    private static final String8FW HEADER_NAME_ACCEPT = new String8FW("accept");
    private static final String8FW HEADER_NAME_ACCESS_CONTROL_ALLOW_METHODS = new String8FW("access-control-allow-methods");
    private static final String8FW HEADER_NAME_ACCESS_CONTROL_REQUEST_METHOD = new String8FW("access-control-request-method");
    private static final String8FW HEADER_NAME_ACCESS_CONTROL_REQUEST_HEADERS = new String8FW("access-control-request-headers");
    private static final String8FW HEADER_NAME_LAST_EVENT_ID = new String8FW("last-event-id");

    private static final String16FW HEADER_VALUE_STATUS_204 = new String16FW("204");
    private static final String16FW HEADER_VALUE_STATUS_405 = new String16FW("405");
    private static final String16FW HEADER_VALUE_STATUS_400 = new String16FW("400");
    private static final String16FW HEADER_VALUE_METHOD_GET = new String16FW("GET");
    private static final String16FW HEADER_VALUE_METHOD_OPTIONS = new String16FW("OPTIONS");

    private static final String16FW CORS_PREFLIGHT_METHOD = HEADER_VALUE_METHOD_OPTIONS;
    private static final String16FW CORS_ALLOWED_METHODS = HEADER_VALUE_METHOD_GET;

    private static final Pattern QUERY_PARAMS_PATTERN = Pattern.compile("(?<path>[^?]*)(?<query>[\\?].*)");
    private static final Pattern LAST_EVENT_ID_PATTERN = Pattern.compile("(\\?|&)lastEventId=(?<lastEventId>[^&]*)(&|$)");

    private static final String8FW LAST_EVENT_ID_NULL = new String8FW(null);

    private static final byte ASCII_COLON = 0x3a;
    private static final String METHOD_PROPERTY = "method";
    private static final String HEADERS_PROPERTY = "headers";

    private static final int MAXIMUM_LAST_EVENT_ID_SIZE = 254;

    public static final int MAXIMUM_HEADER_SIZE =
            5 +         // data:
            3 +         // id:
            255 +       // id string
            6 +         // event:
            16 +        // event string
            3;          // \n for data:, id:, event

    private static final int CHALLENGE_CAPABILITIES_MASK = 1 << Capability.CHALLENGE.ordinal();

    private final RouteFW routeRO = new RouteFW();
    private final SseRouteExFW sseRouteExRO = new SseRouteExFW();

    private final FrameFW frameRO = new FrameFW();
    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();
    private final FlushFW flushRO = new FlushFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();
    private final FlushFW.Builder flushRW = new FlushFW.Builder();

    private final ChallengeFW challengeRO = new ChallengeFW();
    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final SseBeginExFW.Builder sseBeginExRW = new SseBeginExFW.Builder();

    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();

    private final HttpBeginExFW httpBeginExRO = new HttpBeginExFW();
    private final HttpBeginExFW.Builder httpBeginExRW = new HttpBeginExFW.Builder();

    private final HttpChallengeExFW httpChallengeExRO = new HttpChallengeExFW();

    private final SseDataExFW sseDataExRO = new SseDataExFW();
    private final SseEndExFW sseEndExRO = new SseEndExFW();

    private final SseEventFW.Builder sseEventRW = new SseEventFW.Builder();

    private final HttpDecodeHelper httpHelper = new HttpDecodeHelper();

    private final String8FW challengeEventType;

    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final MutableDirectBuffer challengeBuffer;
    private final BufferPool bufferPool;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final LongSupplier supplyTraceId;
    private final LongFunction<BudgetDebitor> supplyDebitor;
    private final DirectBuffer initialComment;
    private final int httpTypeId;
    private final int sseTypeId;

    private final Gson gson = new Gson();

    private final Long2ObjectHashMap<SseServerReply> correlations;
    private final MessageFunction<RouteFW> wrapRoute;
    private final Consumer<Array32FW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> setHttpResponseHeaders;
    private final Consumer<Array32FW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> setHttpResponseHeadersWithTimestampExt;

    public SseServerFactory(
        SseConfiguration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongUnaryOperator supplyInitialId,
        LongUnaryOperator supplyReplyId,
        LongSupplier supplyTraceId,
        ToIntFunction<String> supplyTypeId,
        LongFunction<BudgetDebitor> supplyDebitor)
    {
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.challengeBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.bufferPool = requireNonNull(bufferPool);
        this.supplyInitialId = requireNonNull(supplyInitialId);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.supplyTraceId = requireNonNull(supplyTraceId);
        this.supplyDebitor = requireNonNull(supplyDebitor);
        this.correlations = new Long2ObjectHashMap<>();
        this.wrapRoute = this::wrapRoute;
        this.initialComment = config.initialComment();
        this.httpTypeId = supplyTypeId.applyAsInt(HTTP_TYPE_NAME);
        this.sseTypeId = supplyTypeId.applyAsInt(SseNukleus.NAME);
        this.setHttpResponseHeaders = this::setHttpResponseHeaders;
        this.setHttpResponseHeadersWithTimestampExt = this::setHttpResponseHeadersWithTimestampExt;
        this.challengeEventType = new String8FW(config.getChallengeEventType());
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

        if ((streamId & 0x0000_0000_0000_0001L) != 0L)
        {
            newStream = newInitialStream(begin, throttle);
        }
        else
        {
            newStream = newReplyStream(begin, throttle);
        }

        return newStream;
    }

    private MessageConsumer newInitialStream(
        final BeginFW begin,
        final MessageConsumer acceptReply)
    {
        final long affinity = begin.affinity();
        final OctetsFW extension = begin.extension();
        final HttpBeginExFW httpBeginEx = extension.get(httpBeginExRO::tryWrap);

        MessageConsumer newStream = null;

        if (isCorsPreflightRequest(httpBeginEx))
        {
            final long acceptRouteId = begin.routeId();
            final long acceptInitialId = begin.streamId();
            final long acceptReplyId = supplyReplyId.applyAsLong(acceptInitialId);
            final long newTraceId = supplyTraceId.getAsLong();

            doWindow(acceptReply, acceptRouteId, acceptInitialId, 0L, 0L, 0, newTraceId, 0L, 0L, 0, 0);
            doHttpBegin(acceptReply, acceptRouteId, acceptReplyId, 0L, 0L, 0, newTraceId, 0L, affinity,
                    SseServerFactory::setCorsPreflightResponse);
            doHttpEnd(acceptReply, acceptRouteId, acceptReplyId, 0L, 0L, 0, newTraceId, 0L);

            newStream = (t, b, i, l) -> {};
        }
        else if (!isSseRequestMethod(httpBeginEx))
        {
            doHttpResponse(begin, acceptReply, HEADER_VALUE_STATUS_405);
            newStream = (t, b, i, l) -> {};
        }
        else
        {
            newStream = newInitialSseStream(begin, acceptReply, httpBeginEx);
        }

        return newStream;
    }

    public MessageConsumer newInitialSseStream(
        final BeginFW begin,
        final MessageConsumer acceptReply,
        final HttpBeginExFW httpBeginEx)
    {
        final long acceptRouteId = begin.routeId();
        final long acceptInitialId = begin.streamId();
        final long sequence = begin.sequence();
        final long acknowledge = begin.acknowledge();
        final int maximum = begin.maximum();
        final long traceId = begin.traceId();
        final long authorization = begin.authorization();
        final long affinity = begin.affinity();

        Array32FW<HttpHeaderFW> headers = httpBeginEx.headers();
        httpHelper.reset();
        headers.forEach(httpHelper::onHttpHeader);

        String16FW pathInfo = httpHelper.path; // TODO: ":pathinfo" ?
        String16FW lastEventId = httpHelper.lastEventId;

        // extract lastEventId query parameter from pathInfo
        // use query parameter value as default for missing Last-Event-ID header
        if (pathInfo != null)
        {
            Matcher matcher = QUERY_PARAMS_PATTERN.matcher(pathInfo.asString());
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
                pathInfo = new String16FW(builder.toString());
            }
        }

        MessageConsumer newStream = null;

        if (lastEventId == null || lastEventId.length() <= MAXIMUM_LAST_EVENT_ID_SIZE)
        {
            final MessagePredicate filter = (t, b, o, l) ->
            {
                final RouteFW route = routeRO.wrap(b, o, o + l);
                final SseRouteExFW routeEx = route.extension().get(sseRouteExRO::tryWrap);
                final String routePathInfo = routeEx != null ? routeEx.pathInfo().asString() : null;

                // TODO: process pathInfo matching
                //       && pathInfo.startsWith(routePathInfo);
                return true;
            };

            final RouteFW route = router.resolve(acceptRouteId, authorization, filter, wrapRoute);
            if (route != null)
            {
                final long connectRouteId = route.correlationId();
                final long connectInitialId = supplyInitialId.applyAsLong(connectRouteId);
                final long connectReplyId = supplyReplyId.applyAsLong(connectInitialId);
                final MessageConsumer connectInitial = router.supplyReceiver(connectInitialId);

                final long acceptReplyId = supplyReplyId.applyAsLong(acceptInitialId);

                final boolean timestampRequested = httpBeginEx.headers().anyMatch(header ->
                    HEADER_NAME_ACCEPT.equals(header.name()) && header.value().asString().contains("ext=timestamp"));

                final String8FW lastEventId8 = httpHelper.asLastEventId(lastEventId);

                final SseServerInitial initialStream = new SseServerInitial(
                    acceptReply,
                    acceptRouteId,
                    acceptInitialId,
                    acceptReplyId,
                    connectInitial,
                    connectRouteId,
                    connectInitialId);

                final SseServerReply replyStream = new SseServerReply(
                    connectInitial,
                    connectRouteId,
                    connectReplyId,
                    acceptReply,
                    acceptRouteId,
                    acceptReplyId,
                    timestampRequested);

                correlations.put(connectReplyId, replyStream);

                router.setThrottle(connectInitialId, initialStream::handleThrottle);
                router.setThrottle(acceptReplyId, replyStream::handleThrottle);

                doSseBegin(connectInitial, connectRouteId, connectInitialId, sequence, acknowledge, maximum,
                        traceId, authorization, affinity, pathInfo, lastEventId8);

                newStream = initialStream::handleStream;
            }
        }
        else
        {
            doHttpResponse(begin, acceptReply, HEADER_VALUE_STATUS_400);

            newStream = (t, b, i, l) -> {};
        }

        return newStream;
    }



    private MessageConsumer newReplyStream(
        final BeginFW begin,
        final MessageConsumer applicationReplyThrottle)
    {
        final long connectReplyId = begin.streamId();

        final SseServerReply replyStream = correlations.remove(connectReplyId);

        MessageConsumer newStream = null;

        if (replyStream != null)
        {
            newStream = replyStream::handleStream;
        }

        return newStream;
    }

    private RouteFW wrapRoute(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        return routeRO.wrap(buffer, index, index + length);
    }

    private final class SseServerInitial
    {
        private final MessageConsumer acceptReply;
        private final long acceptRouteId;
        private final long acceptInitialId;
        private final long acceptReplyId;
        private final MessageConsumer connectInitial;
        private final long connectRouteId;
        private final long connectInitialId;

        private long initialSeq;
        private long initialAck;
        private int initialMax;

        private SseServerInitial(
            MessageConsumer acceptReply,
            long acceptRouteId,
            long acceptInitialId,
            long acceptReplyId,
            MessageConsumer connectInitial,
            long connectRouteId,
            long connectInitialId)
        {
            this.acceptReply = acceptReply;
            this.acceptRouteId = acceptRouteId;
            this.acceptInitialId = acceptInitialId;
            this.acceptReplyId = acceptReplyId;
            this.connectInitial = connectInitial;
            this.connectRouteId = connectRouteId;
            this.connectInitialId = connectInitialId;
        }

        private void handleStream(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                handleBegin(begin);
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
                final FrameFW frame = frameRO.wrap(buffer, index, index + length);
                final long sequence = frame.sequence();
                final long acknowledge = frame.acknowledge();
                final int maximum = frame.maximum();
                doReset(acceptReply, acceptRouteId, acceptInitialId, sequence, acknowledge, maximum);
                break;
            }
        }

        private void handleBegin(
            BeginFW begin)
        {
            final long sequence = begin.sequence();
            final long acknowledge = begin.acknowledge();

            assert acknowledge <= sequence;
            assert sequence >= initialSeq;
            assert acknowledge >= initialAck;

            initialSeq = sequence;
            initialAck = acknowledge;

            assert initialAck <= initialSeq;
        }

        private void handleEnd(
            EndFW end)
        {
            final long sequence = end.sequence();
            final long acknowledge = end.acknowledge();
            final long traceId = end.traceId();
            final long authorization = end.authorization();

            assert acknowledge <= sequence;
            assert sequence >= initialSeq;

            initialSeq = sequence;

            assert initialAck <= initialSeq;

            doSseEnd(connectInitial, connectRouteId, connectInitialId, initialSeq, initialAck, initialMax,
                    traceId, authorization);
        }

        private void handleAbort(
            AbortFW abort)
        {
            final long sequence = abort.sequence();
            final long acknowledge = abort.acknowledge();
            final long traceId = abort.traceId();
            final long authorization = abort.authorization();

            assert acknowledge <= sequence;
            assert sequence >= initialSeq;

            initialSeq = sequence;

            assert initialAck <= initialSeq;

            // TODO: SseAbortEx
            doSseAbort(connectInitial, connectRouteId, connectInitialId, initialSeq, initialAck, initialMax,
                    traceId, authorization);
            cleanupCorrelationIfNecessary();
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
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                handleWindow(window);
                break;
            default:
                // ignore
                break;
            }
        }

        private void handleReset(
            ResetFW reset)
        {
            final long sequence = reset.sequence();
            final long acknowledge = reset.acknowledge();
            final long traceId = reset.traceId();

            assert acknowledge <= sequence;
            assert acknowledge >= initialAck;

            initialAck = acknowledge;

            assert initialAck <= initialSeq;

            doReset(acceptReply, acceptRouteId, acceptInitialId, initialSeq, initialAck, initialMax, traceId);
        }

        private void handleWindow(
            WindowFW window)
        {
            final long sequence = window.sequence();
            final long acknowledge = window.acknowledge();
            final int maximum = window.maximum();
            final long authorization = window.authorization();
            final long traceId = window.traceId();
            final long budgetId = window.budgetId();
            final int padding = window.padding();
            final int capabilities = window.capabilities() | CHALLENGE_CAPABILITIES_MASK;

            assert acknowledge <= sequence;
            assert acknowledge >= initialAck;
            assert maximum >= initialMax;

            initialAck = acknowledge;
            initialMax = maximum;

            assert initialAck <= initialSeq;

            doWindow(acceptReply, acceptRouteId, acceptInitialId, initialSeq, initialAck, initialMax,
                    traceId, authorization, budgetId, padding, capabilities);
        }

        private boolean cleanupCorrelationIfNecessary()
        {
            final SseServerReply correlated = correlations.remove(acceptReplyId);
            if (correlated != null)
            {
                router.clearThrottle(acceptReplyId);
            }

            return correlated != null;
        }
    }

    final class SseServerReply
    {
        private final MessageConsumer applicationReplyThrottle;
        private final long applicationRouteId;
        private final long applicationReplyId;

        private final MessageConsumer networkReply;
        private final long networkRouteId;
        private final long networkReplyId;

        private final boolean timestampRequested;
        private final MessageConsumer afterBeginOrData;

        private int networkSlot = NO_SLOT;
        int networkSlotOffset;
        int deferredClaim;
        boolean deferredEnd;

        private MessageConsumer streamState;

        private long networkReplyBudgetId;
        private int httpReplyPad;
        private long networkReplyAuthorization;
        private BudgetDebitor networkReplyDebitor;
        private long networkReplyDebitorIndex = NO_DEBITOR_INDEX;

        private boolean initialCommentPending;

        private long sseReplySeq;
        private long sseReplyAck;
        private int sseReplyMax;

        private long httpReplySeq;
        private long httpReplyAck;
        private int httpReplyMax;

        private SseServerReply(
            MessageConsumer applicationReplyThrottle,
            long applicationRouteId,
            long applicationReplyId,
            MessageConsumer networkReply,
            long networkRouteId,
            long networkReplyId,
            boolean timestampRequested)
        {
            this.applicationReplyThrottle = applicationReplyThrottle;
            this.applicationRouteId = applicationRouteId;
            this.applicationReplyId = applicationReplyId;
            this.networkReply = networkReply;
            this.networkRouteId = networkRouteId;
            this.networkReplyId = networkReplyId;
            this.timestampRequested = timestampRequested;
            this.initialCommentPending = initialComment != null;
            this.streamState = this::beforeBegin;
            this.afterBeginOrData = this::afterBeginOrData;
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
                final FrameFW frame = frameRO.wrap(buffer, index, index + length);
                final long sequence = frame.sequence();
                final long acknowledge = frame.acknowledge();
                final int maximum = frame.maximum();
                doReset(applicationReplyThrottle, applicationRouteId, applicationReplyId, sequence, acknowledge, maximum);
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
            case FlushFW.TYPE_ID:
                final FlushFW flush = flushRO.wrap(buffer, index, index + length);
                handleFlush(flush);
                break;
            default:
                final FrameFW frame = frameRO.wrap(buffer, index, index + length);
                final long sequence = frame.sequence();
                final long acknowledge = frame.acknowledge();
                final int maximum = frame.maximum();
                doReset(applicationReplyThrottle, applicationRouteId, applicationReplyId, sequence, acknowledge, maximum);
                break;
            }
        }

        private void handleBegin(
            BeginFW begin)
        {
            final long sequence = begin.sequence();
            final long acknowledge = begin.acknowledge();
            final int maximum = begin.maximum();
            final long applicationReplyTraceId = begin.traceId();
            final long applicationReplyAuthorization = begin.authorization();
            final long applicationReplyAffinity = begin.affinity();

            assert acknowledge <= sequence;
            assert sequence >= sseReplySeq;
            assert acknowledge >= sseReplyAck;

            sseReplySeq = sequence;
            sseReplyAck = acknowledge;
            sseReplyMax = maximum;

            assert sseReplyAck <= sseReplySeq;

            if (timestampRequested)
            {
                doHttpBegin(
                    networkReply,
                    networkRouteId,
                    networkReplyId,
                    sseReplySeq,
                    sseReplyAck,
                    sseReplyMax,
                    applicationReplyTraceId,
                    applicationReplyAuthorization,
                    applicationReplyAffinity,
                    setHttpResponseHeadersWithTimestampExt);
            }
            else
            {
                doHttpBegin(
                    networkReply,
                    networkRouteId,
                    networkReplyId,
                    sseReplySeq,
                    sseReplyAck,
                    sseReplyMax,
                    applicationReplyTraceId,
                    applicationReplyAuthorization,
                    applicationReplyAffinity,
                    setHttpResponseHeaders);
            }

            this.streamState = afterBeginOrData;

            doFlush(applicationReplyTraceId);
        }

        private void handleData(
            DataFW data)
        {
            final long sequence = data.sequence();
            final long acknowledge = data.acknowledge();
            final long traceId = data.traceId();
            final long authorization = data.authorization();
            final long budgetId = data.budgetId();
            final int reserved = data.reserved();

            assert acknowledge <= sequence;
            assert sequence >= sseReplySeq;

            sseReplySeq = sequence + reserved;

            assert sseReplyAck <= sseReplySeq;

            if (sseReplySeq > sseReplyAck + sseReplyMax)
            {
                doReset(applicationReplyThrottle, applicationRouteId, applicationReplyId, sseReplySeq, sseReplyAck, sseReplyMax);
                doHttpAbort(networkReply, networkRouteId, networkReplyId, httpReplySeq, httpReplyAck, httpReplyMax,
                        supplyTraceId.getAsLong(), authorization);
            }
            else
            {
                final int flags = data.flags();
                final OctetsFW payload = data.payload();
                final OctetsFW extension = data.extension();

                DirectBuffer id = null;
                DirectBuffer type = null;
                long timestamp = 0L;
                if (flags != 0x00 && extension.sizeof() > 0)
                {
                    final SseDataExFW sseDataEx = extension.get(sseDataExRO::wrap);
                    id = sseDataEx.id().value();
                    type = sseDataEx.type().value();

                    if (timestampRequested)
                    {
                        timestamp = sseDataEx.timestamp();
                    }
                }

                final SseEventFW sseEvent = sseEventRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                        .flags(flags)
                        .dataFinOnly(payload)
                        .timestamp(timestamp)
                        .id(id)
                        .type(type)
                        .dataInit(payload)
                        .dataContOnly(payload)
                        .build();

                doHttpData(networkReply, networkRouteId, networkReplyId, httpReplySeq, httpReplyAck, httpReplyMax,
                        traceId, authorization, budgetId, flags, reserved, sseEvent);

                httpReplySeq += reserved;

                assert httpReplySeq <= httpReplyAck + httpReplyMax;

                initialCommentPending = false;
            }
        }

        private void handleEnd(
            EndFW end)
        {
            final long sequence = end.sequence();
            final long acknowledge = end.acknowledge();
            final long traceId = end.traceId();
            final long authorization = end.authorization();
            final OctetsFW extension = end.extension();

            assert acknowledge <= sequence;
            assert sequence >= sseReplySeq;

            sseReplySeq = sequence;

            assert sseReplyAck <= sseReplySeq;

            if (extension.sizeof() > 0)
            {
                final SseEndExFW sseEndEx = extension.get(sseEndExRO::wrap);
                final DirectBuffer id = sseEndEx.id().value();

                int flags = FIN | INIT;

                final SseEventFW sseEvent = sseEventRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                        .flags(flags)
                        .id(id)
                        .build();

                final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                    .routeId(networkRouteId)
                    .streamId(networkReplyId)
                    .sequence(httpReplySeq)
                    .acknowledge(httpReplyAck)
                    .maximum(httpReplyMax)
                    .traceId(traceId)
                    .authorization(authorization)
                    .flags(flags)
                    .budgetId(networkReplyBudgetId)
                    .reserved(sseEvent.sizeof() + httpReplyPad)
                    .payload(sseEvent.buffer(), sseEvent.offset(), sseEvent.sizeof())
                    .build();

                if (networkSlot == NO_SLOT)
                {
                    networkSlot = bufferPool.acquire(networkReplyId);
                }

                if (networkSlot != NO_SLOT)
                {
                    MutableDirectBuffer buffer = bufferPool.buffer(networkSlot);
                    buffer.putBytes(networkSlotOffset, data.buffer(), data.offset(), data.sizeof());
                    networkSlotOffset += data.sizeof();

                    if (networkReplyDebitorIndex != NO_DEBITOR_INDEX)
                    {
                        deferredClaim += data.reserved();
                    }

                    deferredEnd = true;
                    doFlushIfNecessary(traceId);
                }
                else
                {
                    cleanupDebitorIfNecessary();
                    doHttpAbort(networkReply, networkRouteId, networkReplyId, httpReplySeq, httpReplyAck, httpReplyMax,
                            traceId, authorization);
                }
            }
            else
            {
                doHttpEnd(networkReply, networkRouteId, networkReplyId, httpReplySeq, httpReplyAck, httpReplyMax,
                        traceId, authorization);
                cleanupDebitorIfNecessary();
            }
        }

        private void handleFlush(
            FlushFW flush)
        {
            final long sequence = flush.sequence();
            final long acknowledge = flush.acknowledge();
            final long traceId = flush.traceId();
            final long authorization = flush.authorization();
            final long budgetId = flush.budgetId();
            final int reserved = flush.reserved();

            assert acknowledge <= sequence;
            assert sequence >= sseReplySeq;

            sseReplySeq = sequence;

            assert sseReplyAck <= sseReplySeq;

            doFlushIfNecessary(traceId);
            doHttpFlush(networkReply, networkRouteId, networkReplyId, httpReplySeq, httpReplyAck, httpReplyMax,
                    traceId, authorization, budgetId, reserved);
        }

        private void handleAbort(
            AbortFW abort)
        {
            final long sequence = abort.sequence();
            final long acknowledge = abort.acknowledge();
            final long traceId = abort.traceId();
            final long authorization = abort.authorization();

            assert acknowledge <= sequence;
            assert sequence >= sseReplySeq;

            sseReplySeq = sequence;

            assert sseReplyAck <= sseReplySeq;

            doHttpAbort(networkReply, networkRouteId, networkReplyId, httpReplySeq, httpReplyAck, httpReplyMax,
                    traceId, authorization);
            cleanupDebitorIfNecessary();
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
            case ChallengeFW.TYPE_ID:
                final ChallengeFW challenge = challengeRO.wrap(buffer, index, index + length);
                handleChallenge(challenge);
                break;
            default:
                // ignore
                break;
            }
        }

        private void handleWindow(
            WindowFW window)
        {
            final long sequence = window.sequence();
            final long acknowledge = window.acknowledge();
            final int maximum = window.maximum();
            final long traceId = window.traceId();
            final long authorization = window.authorization();
            final long budgetId = window.budgetId();
            final int padding = window.padding();

            assert acknowledge <= sequence;
            assert sequence <= httpReplySeq;
            assert acknowledge >= httpReplyAck;
            assert maximum >= httpReplyMax;

            httpReplyAck = acknowledge;
            httpReplyMax = maximum;
            httpReplyPad = padding;
            networkReplyBudgetId = budgetId;
            networkReplyAuthorization = authorization;

            assert httpReplyAck <= httpReplySeq;

            if (networkReplyBudgetId != 0L && networkReplyDebitorIndex == NO_DEBITOR_INDEX)
            {
                networkReplyDebitor = supplyDebitor.apply(budgetId);
                networkReplyDebitorIndex = networkReplyDebitor.acquire(budgetId, networkReplyId, this::doFlushIfNecessary);
            }

            if (networkReplyBudgetId != 0L && networkReplyDebitorIndex == NO_DEBITOR_INDEX)
            {
                doHttpAbort(networkReply, networkRouteId, networkReplyId, httpReplySeq, httpReplyAck, httpReplyMax,
                        traceId, authorization);
                doReset(applicationReplyThrottle, applicationRouteId, applicationReplyId, sseReplySeq, sseReplyAck, httpReplyMax);
            }
            else
            {
                doFlushIfNecessary(traceId);
            }
        }

        private void handleReset(
            ResetFW reset)
        {
            final long sequence = reset.sequence();
            final long acknowledge = reset.acknowledge();
            final int maximum = reset.maximum();
            final long traceId = reset.traceId();

            assert acknowledge <= sequence;
            assert sequence <= httpReplySeq;
            assert acknowledge >= httpReplyAck;
            assert maximum >= httpReplyMax;

            httpReplyAck = acknowledge;
            httpReplyMax = maximum;

            assert httpReplyAck <= httpReplySeq;

            doSseResponseReset(traceId);
            cleanupDebitorIfNecessary();
        }

        private void doSseResponseReset(
            long traceId)
        {
            correlations.remove(applicationReplyId);
            doReset(applicationReplyThrottle, applicationRouteId, applicationReplyId, sseReplySeq, sseReplyAck, sseReplyMax,
                    traceId);
        }

        private void handleChallenge(
            ChallengeFW challenge)
        {
            final long sequence = challenge.sequence();
            final long acknowledge = challenge.acknowledge();
            final int maximum = challenge.maximum();

            assert acknowledge <= sequence;
            assert sequence <= httpReplySeq;
            assert acknowledge >= httpReplyAck;
            assert maximum >= httpReplyMax;

            httpReplyAck = acknowledge;
            httpReplyMax = maximum;

            assert httpReplyAck <= httpReplySeq;

            final HttpChallengeExFW httpChallengeEx = challenge.extension().get(httpChallengeExRO::tryWrap);
            if (httpChallengeEx != null)
            {
                final JsonObject challengeObject = new JsonObject();
                final JsonObject challengeHeaders = new JsonObject();
                final Array32FW<HttpHeaderFW> httpHeaders = httpChallengeEx.headers();

                httpHeaders.forEach(header ->
                {
                    final String8FW name = header.name();
                    final String16FW value = header.value();
                    if (name != null)
                    {
                        if (name.sizeof() > SIZE_OF_BYTE &&
                            name.buffer().getByte(name.offset() + SIZE_OF_BYTE) != ASCII_COLON)
                        {
                            final String propertyName = name.asString();
                            final String propertyValue = value.asString();
                            challengeHeaders.addProperty(propertyName, propertyValue);
                        }
                        else if (name.equals(HEADER_NAME_METHOD))
                        {
                            final String propertyValue = value.asString();
                            challengeObject.addProperty(METHOD_PROPERTY, propertyValue);
                        }
                    }
                });
                challengeObject.add(HEADERS_PROPERTY, challengeHeaders);

                final String challengeJson = gson.toJson(challengeObject);
                final int challengeBytes = challengeBuffer.putStringWithoutLengthUtf8(0, challengeJson);

                final SseEventFW sseEvent = sseEventRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                        .flags(Flags.INIT | Flags.FIN)
                        .type(challengeEventType.value())
                        .data(challengeBuffer, 0, challengeBytes)
                        .build();

                final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                        .routeId(networkRouteId)
                        .streamId(networkReplyId)
                        .sequence(httpReplySeq)
                        .acknowledge(httpReplyAck)
                        .maximum(httpReplyMax)
                        .traceId(challenge.traceId())
                        .authorization(0)
                        .budgetId(networkReplyBudgetId)
                        .reserved(sseEvent.sizeof() + httpReplyPad)
                        .payload(sseEvent.buffer(), sseEvent.offset(), sseEvent.sizeof())
                        .build();

                if (networkSlot == NO_SLOT)
                {
                    networkSlot = bufferPool.acquire(networkReplyId);
                }

                if (networkSlot != NO_SLOT)
                {
                    MutableDirectBuffer buffer = bufferPool.buffer(networkSlot);
                    buffer.putBytes(networkSlotOffset, data.buffer(), data.offset(), data.sizeof());
                    networkSlotOffset += data.sizeof();

                    if (networkReplyDebitorIndex != NO_DEBITOR_INDEX)
                    {
                        deferredClaim += data.reserved();
                    }
                }

                doFlushIfNecessary(challenge.traceId());
            }
        }

        private void doFlushIfNecessary(
            long traceId)
        {
            if (streamState == afterBeginOrData)
            {
                doFlush(traceId);
            }

            doWindowIfNecessary(traceId);
        }

        private void doFlush(
            long traceId)
        {
            comment:
            if (initialCommentPending)
            {
                assert initialComment != null;

                final int flags = FIN | INIT;
                final SseEventFW sseEvent =
                        sseEventRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                                  .flags(flags)
                                  .comment(initialComment)
                                  .build();

                final int reserved = sseEvent.sizeof() + httpReplyPad;

                if (reserved > (int)(httpReplySeq - httpReplyAck + httpReplyMax))
                {
                    break comment;
                }

                int claimed = reserved;
                if (networkReplyDebitorIndex != NO_DEBITOR_INDEX)
                {
                    claimed = networkReplyDebitor.claim(traceId, networkReplyDebitorIndex, networkReplyId,
                        reserved, reserved, 0);
                }

                if (claimed == reserved)
                {
                    doHttpData(networkReply, networkRouteId, networkReplyId, httpReplySeq, httpReplyAck, httpReplyMax,
                            traceId, networkReplyAuthorization, networkReplyBudgetId, flags, reserved, sseEvent);

                    httpReplySeq += reserved;

                    assert httpReplySeq <= httpReplyAck + httpReplyMax;

                    initialCommentPending = false;
                }
            }

            if (deferredClaim > 0)
            {
                assert networkReplyDebitorIndex != NO_DEBITOR_INDEX;

                int claimed = networkReplyDebitor.claim(traceId, networkReplyDebitorIndex, networkReplyId,
                    deferredClaim, deferredClaim, 0);

                if (claimed == deferredClaim)
                {
                    deferredClaim = 0;
                }
            }

            if (deferredClaim == 0)
            {
                if (networkSlot != NO_SLOT)
                {
                    final MutableDirectBuffer buffer = bufferPool.buffer(networkSlot);
                    final DataFW data = dataRO.wrap(buffer,  0,  networkSlotOffset);
                    final int networkSlotReserved = data.reserved();

                    if (httpReplySeq + networkSlotReserved <= httpReplyAck + httpReplyMax)
                    {
                        networkReply.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
                        httpReplySeq += networkSlotReserved;
                        networkSlotOffset -= data.sizeof();
                        assert networkSlotOffset == 0;
                        bufferPool.release(networkSlot);
                        networkSlot = NO_SLOT;

                        if (deferredEnd)
                        {
                            doHttpEnd(networkReply, networkRouteId, networkReplyId, httpReplySeq, httpReplyAck, httpReplyMax,
                                    data.traceId(), data.authorization());
                            cleanupDebitorIfNecessary();
                            deferredEnd = false;
                        }
                    }
                }
            }
        }

        private void doWindowIfNecessary(
            long traceId)
        {
            int httpReplyPendingAck = (int)(httpReplySeq - httpReplyAck) + networkSlotOffset;
            if (initialCommentPending)
            {
                assert initialComment != null;
                httpReplyPendingAck += initialComment.capacity() + 3 + httpReplyPad;
            }

            int sseReplyPad = httpReplyPad + MAXIMUM_HEADER_SIZE;
            int sseReplyAckMax = (int)(sseReplySeq - httpReplyPendingAck);
            if (sseReplyAckMax > sseReplyAck || httpReplyMax > sseReplyMax)
            {
                sseReplyAck = sseReplyAckMax;
                assert sseReplyAck <= sseReplySeq;

                sseReplyMax = httpReplyMax;

                doWindow(applicationReplyThrottle, applicationRouteId, applicationReplyId, sseReplySeq, sseReplyAck, sseReplyMax,
                        traceId, networkReplyAuthorization, networkReplyBudgetId, sseReplyPad, 0);
            }
        }

        private void cleanupDebitorIfNecessary()
        {
            if (networkReplyDebitorIndex != NO_DEBITOR_INDEX)
            {
                networkReplyDebitor.release(networkReplyDebitorIndex, networkReplyId);
                networkReplyDebitor = null;
                networkReplyDebitorIndex = NO_DEBITOR_INDEX;
            }
        }
    }

    private final class HttpDecodeHelper
    {
        private final String8FW.Builder lastEventIdRW = new String8FW.Builder().wrap(new UnsafeBuffer(new byte[256]), 0, 256);

        private final String16FW pathRO = new String16FW();
        private final String16FW lastEventIdRO = new String16FW();

        private String16FW path;
        private String16FW lastEventId;

        private void onHttpHeader(
            HttpHeaderFW header)
        {
            final String8FW name = header.name();
            final String16FW value = header.value();

            if (HEADER_NAME_PATH.equals(name))
            {
                path = pathRO.wrap(value.buffer(), value.offset(), value.limit());
            }
            else if (HEADER_NAME_LAST_EVENT_ID.equals(name))
            {
                lastEventId = lastEventIdRO.wrap(value.buffer(), value.offset(), value.limit());
            }
        }

        private String8FW asLastEventId(
            String16FW lastEventId)
        {
            lastEventIdRW.rewrap();
            return lastEventId != null ? lastEventIdRW.set(lastEventId).build() : LAST_EVENT_ID_NULL;
        }

        private void reset()
        {
            path = null;
            lastEventId = null;
        }
    }

    private void setHttpResponseHeaders(
        Array32FW.Builder<HttpHeaderFW.Builder, HttpHeaderFW> headers)
    {
        headers.item(h -> h.name(":status").value("200"));
        headers.item(h -> h.name("content-type").value("text/event-stream"));
    }

    private void setHttpResponseHeadersWithTimestampExt(
        Array32FW.Builder<HttpHeaderFW.Builder, HttpHeaderFW> headers)
    {
        headers.item(h -> h.name(":status").value("200"));
        headers.item(h -> h.name("content-type").value("text/event-stream;ext=timestamp"));
    }

    private void doHttpBegin(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        long authorization,
        long affinity,
        Consumer<Array32FW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> mutator)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .sequence(sequence)
                .acknowledge(acknowledge)
                .maximum(maximum)
                .traceId(traceId)
                .authorization(authorization)
                .affinity(affinity)
                .extension(e -> e.set(visitHttpBeginEx(mutator)))
                .build();

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    private Flyweight.Builder.Visitor visitHttpBeginEx(
        Consumer<Array32FW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> headers)
    {
        return (buffer, offset, limit) ->
            httpBeginExRW.wrap(buffer, offset, limit)
                         .typeId(httpTypeId)
                         .headers(headers)
                         .build()
                         .sizeof();
    }

    private void doHttpData(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        long authorization,
        long budgetId,
        int flags,
        int reserved,
        Flyweight payload)
    {
        final DataFW frame = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .sequence(sequence)
                .acknowledge(acknowledge)
                .maximum(maximum)
                .traceId(traceId)
                .authorization(authorization)
                .flags(flags)
                .budgetId(budgetId)
                .reserved(reserved)
                .payload(payload.buffer(), payload.offset(), payload.sizeof())
                .build();

        receiver.accept(frame.typeId(), frame.buffer(), frame.offset(), frame.sizeof());
    }

    private void doHttpEnd(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        long authorization)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .sequence(sequence)
                .acknowledge(acknowledge)
                .maximum(maximum)
                .traceId(traceId)
                .authorization(authorization)
                .build();

        receiver.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    private void doHttpAbort(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        long authorization)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .sequence(sequence)
                .acknowledge(acknowledge)
                .maximum(maximum)
                .traceId(traceId)
                .authorization(authorization)
                .build();

        receiver.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    private void doHttpFlush(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        long authorization,
        long budgetId,
        int reserved)
    {
        final FlushFW flush = flushRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .sequence(sequence)
                .acknowledge(acknowledge)
                .maximum(maximum)
                .traceId(traceId)
                .authorization(authorization)
                .budgetId(budgetId)
                .reserved(reserved)
                .build();

        receiver.accept(flush.typeId(), flush.buffer(), flush.offset(), flush.sizeof());
    }

    private void doHttpResponse(
        BeginFW begin,
        MessageConsumer acceptReply,
        String16FW status)
    {
        final long sequence = begin.sequence();
        final long acknowledge = begin.acknowledge();
        final long acceptRouteId = begin.routeId();
        final long acceptInitialId = begin.streamId();
        final long acceptReplyId = supplyReplyId.applyAsLong(acceptInitialId);
        final long affinity = begin.affinity();
        final long traceId = begin.traceId();

        doWindow(acceptReply, acceptRouteId, acceptInitialId, sequence, acknowledge, 0, traceId, 0L, 0, 0, 0);
        doHttpBegin(acceptReply, acceptRouteId, acceptReplyId, 0L, 0L, 0, traceId, 0L, affinity,
            hs -> hs.item(h -> h.name(HEADER_NAME_STATUS).value(status)));
        doHttpEnd(acceptReply, acceptRouteId, acceptReplyId, 0L, 0L, 0, traceId, 0L);
    }

    private void doSseBegin(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        long authorization,
        long affinity,
        String16FW pathInfo,
        String8FW lastEventId)
    {
        final SseBeginExFW sseBegin = sseBeginExRW.wrap(writeBuffer, BeginFW.FIELD_OFFSET_EXTENSION, writeBuffer.capacity())
                .typeId(sseTypeId)
                .pathInfo(pathInfo)
                .lastEventId(lastEventId)
                .build();

        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .sequence(sequence)
                .acknowledge(acknowledge)
                .maximum(maximum)
                .traceId(traceId)
                .authorization(authorization)
                .affinity(affinity)
                .extension(sseBegin.buffer(), sseBegin.offset(), sseBegin.sizeof())
                .build();

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    private void doSseAbort(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        long authorization)
    {
        // TODO: SseAbortEx
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .sequence(sequence)
                .acknowledge(acknowledge)
                .maximum(maximum)
                .traceId(traceId)
                .authorization(authorization)
                .build();

        receiver.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    private void doSseEnd(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        long authorization)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .sequence(sequence)
                .acknowledge(acknowledge)
                .maximum(maximum)
                .traceId(traceId)
                .authorization(authorization)
                .build();

        receiver.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    private void doWindow(
        MessageConsumer sender,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        long authorization,
        long budgetId,
        int padding,
        int capabilities)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .sequence(sequence)
                .acknowledge(acknowledge)
                .maximum(maximum)
                .traceId(traceId)
                .authorization(authorization)
                .budgetId(budgetId)
                .padding(padding)
                .capabilities(capabilities)
                .build();

        sender.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    private void doReset(
        MessageConsumer sender,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
               .routeId(routeId)
               .streamId(streamId)
               .sequence(sequence)
               .acknowledge(acknowledge)
               .maximum(maximum)
               .traceId(traceId)
               .build();

        sender.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

    private void doReset(
        MessageConsumer sender,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum)
    {
        doReset(sender, routeId, streamId, sequence, acknowledge, maximum, supplyTraceId.getAsLong());
    }

    private static String16FW decodeLastEventId(
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

        return lastEventId != null ? new String16FW(lastEventId) : null;
    }

    private static boolean isCorsPreflightRequest(
        HttpBeginExFW httpBeginEx)
    {
        return httpBeginEx != null &&
               httpBeginEx.headers().anyMatch(h -> HEADER_NAME_METHOD.equals(h.name()) &&
                                                   CORS_PREFLIGHT_METHOD.equals(h.value())) &&
               httpBeginEx.headers().anyMatch(h -> HEADER_NAME_ACCESS_CONTROL_REQUEST_METHOD.equals(h.name()) ||
                                                   HEADER_NAME_ACCESS_CONTROL_REQUEST_HEADERS.equals(h.name()));
    }

    private static void setCorsPreflightResponse(
        Array32FW.Builder<HttpHeaderFW.Builder, HttpHeaderFW> headers)
    {
        headers.item(h -> h.name(HEADER_NAME_STATUS).value(HEADER_VALUE_STATUS_204))
               .item(h -> h.name(HEADER_NAME_ACCESS_CONTROL_ALLOW_METHODS).value(CORS_ALLOWED_METHODS));
    }

    private static boolean isSseRequestMethod(
        HttpBeginExFW httpBeginEx)
    {
        return httpBeginEx != null &&
               httpBeginEx.headers().anyMatch(h -> HEADER_NAME_METHOD.equals(h.name()) &&
                                                   HEADER_VALUE_METHOD_GET.equals(h.value()));
    }
}
