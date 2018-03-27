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
package org.reaktivity.nukleus.sse.internal.bench;

import static java.nio.ByteBuffer.allocateDirect;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;
import static org.reaktivity.nukleus.sse.internal.types.stream.FrameFW.FIELD_OFFSET_TIMESTAMP;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Control;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.sse.internal.stream.ServerStreamFactoryBuilder;
import org.reaktivity.nukleus.sse.internal.types.control.Role;
import org.reaktivity.nukleus.sse.internal.types.control.RouteFW;
import org.reaktivity.nukleus.sse.internal.types.control.SseRouteExFW;
import org.reaktivity.nukleus.sse.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.sse.internal.types.stream.DataFW;
import org.reaktivity.nukleus.sse.internal.types.stream.FrameFW;
import org.reaktivity.nukleus.sse.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.sse.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.reaktor.internal.buffer.DefaultBufferPool;
import org.reaktivity.reaktor.internal.router.ReferenceKind;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@Fork(3)
@Warmup(iterations = 10, time = 1, timeUnit = SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = SECONDS)
@OutputTimeUnit(SECONDS)
public class ServerStreamBM
{
    private static final class Router implements RouteManager
    {
        private MutableDirectBuffer writeBuffer = new UnsafeBuffer(new byte[1024]);
        private AtomicLong routeRefs = new AtomicLong(0L);
        private SseRouteExFW.Builder sseRouteExRW = new SseRouteExFW.Builder();
        private Map<String, MessageConsumer> throttles = new HashMap<>();
        private Map<String, MessageConsumer> targets = new HashMap<>();

        @Override
        public <R> R resolve(
            long authorization,
            MessagePredicate filter,
            MessageFunction<R> mapper)
        {
            RouteFW route = new RouteFW.Builder().wrap(writeBuffer, 0, writeBuffer.capacity())
                    .correlationId(1L)
                    .role(b -> b.set(Role.SERVER))
                    .source("source")
                    .sourceRef(ReferenceKind.SERVER.nextRef(routeRefs))
                    .target("target")
                    .targetRef(ReferenceKind.SERVER.nextRef(routeRefs))
                    .extension(e -> e.set((b, o, l)
                                 -> sseRouteExRW.wrap(b, o, l)
                                                .pathInfo("/")
                                                .limit()))
                    .build();

            return mapper.apply(route.typeId(), route.buffer(), route.offset(), route.sizeof());
        }

        @Override
        public MessageConsumer supplyTarget(
            String targetName)
        {
            return targets.get(targetName);
        }

        @Override
        public void setThrottle(
            String targetName,
            long streamId,
            MessageConsumer throttle)
        {
            throttles.put(targetName, throttle);
        }

        public void setTarget(
            String targetName,
            MessageConsumer target)
        {
            targets.put(targetName, target);
        }

        public MessageConsumer getSource(
            String sourceName)
        {
            return throttles.get(sourceName);
        }
    }

    private DataFW dataRO;
    private Long2ObjectHashMap<MessageConsumer> streams;
    private MessageConsumer stream;

    @Setup(Level.Trial)
    public void init()
    {
        this.streams = new Long2ObjectHashMap<>();
        this.source = new OneToOneRingBuffer(new UnsafeBuffer(allocateDirect(1024 * 1024 * 64 + TRAILER_LENGTH)));
        this.nukleus = new OneToOneRingBuffer(new UnsafeBuffer(allocateDirect(1024 * 1024 * 64 + TRAILER_LENGTH)));
        this.target = new OneToOneRingBuffer(new UnsafeBuffer(allocateDirect(1024 * 1024 * 64 + TRAILER_LENGTH)));
        Configuration config = new Configuration();
        BufferPool bufferPool = new DefaultBufferPool(0, 0);
        MutableInteger correlationId = new MutableInteger();
        MutableInteger groupId = new MutableInteger();
        MutableInteger streamId = new MutableInteger();
        MutableInteger traceId = new MutableInteger();
        MutableDirectBuffer writeBuffer = new UnsafeBuffer(new byte[1024]);
        Router router = new Router();
        StreamFactory streamFactory = new ServerStreamFactoryBuilder(config)
                .setAccumulatorSupplier(s -> l -> {})
                .setCounterSupplier(s -> () -> 0)
                .setBufferPoolSupplier(() -> bufferPool)
                .setCorrelationIdSupplier(() -> ++correlationId.value)
                .setGroupBudgetClaimer(g -> c -> c)
                .setGroupBudgetReleaser(g -> c -> c)
                .setGroupIdSupplier(() -> ++groupId.value)
                .setRouteManager(router)
                .setStreamIdSupplier(() -> ++streamId.value)
                .setTraceSupplier(() -> ++traceId.value)
                .setWriteBuffer(writeBuffer)
                .build();
        BeginFW beginRO = new BeginFW();
        DataFW dataRO = new DataFW();
        WindowFW.Builder windowRW = new WindowFW.Builder();
        HttpBeginExFW.Builder httpBeginExRW = new HttpBeginExFW.Builder();
        MutableDirectBuffer sourceBuffer = new UnsafeBuffer(new byte[1024]);
        MessageConsumer[] throttleRef = new MessageConsumer[1];
        router.setTarget("source", (t, b, o, l) ->
        {
            ((MutableDirectBuffer) b).putLong(o + FIELD_OFFSET_TIMESTAMP, System.nanoTime());
            source.write(t, b, o, l);
            if (t == BeginFW.TYPE_ID)
            {
                MessageConsumer throttle = router.getSource("source");
                Objects.requireNonNull(throttle);
                BeginFW begin = beginRO.wrap(b, o, l);
                WindowFW window = windowRW.wrap(sourceBuffer, 0, sourceBuffer.capacity())
                                          .streamId(begin.streamId())
                                          .credit(Integer.MAX_VALUE)
                                          .padding(0)
                                          .groupId(0L)
                                          .build();
                throttle.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
                throttleRef[0] = throttle;
            }
            else if (t == DataFW.TYPE_ID)
            {
                DataFW data = dataRO.wrap(b, o, l);
                WindowFW window = windowRW.wrap(sourceBuffer, 0, sourceBuffer.capacity())
                        .streamId(data.streamId())
                        .credit(data.length())
                        .padding(data.padding())
                        .groupId(data.groupId())
                        .build();
                throttleRef[0].accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
            }
        });
        MutableDirectBuffer targetBuffer = new UnsafeBuffer(new byte[1024]);
        router.setTarget("target", (t, b, o, l) ->
        {
            ((MutableDirectBuffer) b).putLong(o + FIELD_OFFSET_TIMESTAMP, System.nanoTime());
            target.write(t, b, o, l);
            if (t == BeginFW.TYPE_ID)
            {
                MessageConsumer throttle = router.getSource("target");
                Objects.requireNonNull(throttle);
                BeginFW begin = beginRO.wrap(b, o, l);
                WindowFW window = windowRW.wrap(targetBuffer, 0, targetBuffer.capacity())
                                          .streamId(begin.streamId())
                                          .credit(Integer.MAX_VALUE)
                                          .padding(0)
                                          .groupId(0)
                                          .build();
                throttle.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());

                BeginFW reply = new BeginFW.Builder().wrap(targetBuffer, 0, targetBuffer.capacity())
                                                     .streamId(1L)
                                                     .source("target")
                                                     .sourceRef(0L)
                                                     .correlationId(begin.correlationId())
                                                     .build();

                stream = streamFactory.newStream(reply.typeId(), reply.buffer(),
                                                 reply.offset(), reply.sizeof(),
                                                 (t2, b2, o2, l2) -> {});
                stream.accept(reply.typeId(), reply.buffer(), reply.offset(), reply.sizeof());
                streams.put(1L, stream);
            }
        });
        MutableDirectBuffer buffer = new UnsafeBuffer(new byte[256]);
        BeginFW begin = new BeginFW.Builder().wrap(buffer, 0, buffer.capacity())
                                             .streamId(1L)
                                             .source("source")
                                             .sourceRef(1)
                                             .correlationId(2L)
                                             .extension(e -> e.set((b, o, l)
                                                          -> httpBeginExRW.wrap(b, o, l)
                                                                          .headersItem(hs -> hs.name("accept")
                                                                                               .value("text/event-stream"))
                                                                          .headersItem(hs -> hs.name(":path")
                                                                                               .value("/"))
                                                                          .limit()))
                                             .build();
        MessageConsumer throttle = (t, b, o, l) -> {};
        streamFactory.newStream(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof(), throttle)
                     .accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
        // TOOD: end request stream
        DataFW data = new DataFW.Builder().wrap(buffer, 0, buffer.capacity())
                                          .streamId(1L)
                                          .groupId(0L)
                                          .padding(0)
                                          .payload(b -> b.set(new byte[128]))
                                          .build();
        this.dataRO = data;
    }

    @Setup(Level.Iteration)
    public void reset()
    {
        AtomicBuffer buffer = nukleus.buffer();
        buffer.setMemory(buffer.capacity() - TRAILER_LENGTH, TRAILER_LENGTH, (byte)0);
        buffer.putLongOrdered(0, 0L);
    }

    private OneToOneRingBuffer source;
    private OneToOneRingBuffer nukleus;
    private OneToOneRingBuffer target;

    @Benchmark
    @Group("data")
    public void write(
        Control control)
    {
        while (!control.stopMeasurement &&
                !nukleus.write(dataRO.typeId(), dataRO.buffer(), dataRO.offset(), dataRO.sizeof()))
        {
            Thread.yield();
        }
    }

    @Benchmark
    @Group("data")
    public void read()
    {
        nukleus.read(readHandler);
    }

    private final MessageHandler readHandler = this::handleRead;
    private final FrameFW frameRO = new FrameFW();

    private void handleRead(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        final FrameFW frame = frameRO.wrap(buffer, index, index + length);
        streams.get(frame.streamId()).accept(msgTypeId, buffer, index, length);
    }

    public static void main(
        String[] args) throws RunnerException
    {
        Options opt = new OptionsBuilder()
                .include(ServerStreamBM.class.getSimpleName())
                .forks(0)
                .build();

        new Runner(opt).run();
    }
}
