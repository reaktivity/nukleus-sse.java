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
package org.reaktivity.nukleus.sse.internal.test.counters;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.sse.internal.test.SseCountersRule;
import org.reaktivity.reaktor.test.ReaktorRule;

public class FrameAndBytesCounterIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/sse/control/route")
            .addScriptRoot("client", "org/reaktivity/specification/sse/data")
            .addScriptRoot("server", "org/reaktivity/specification/nukleus/sse/streams/data");

    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .directory("target/nukleus-itests")
        .controller("sse"::equals)
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024)
        .nukleus("sse"::equals)
        .clean();

    private final SseCountersRule counters = new SseCountersRule(reaktor);

    @Rule
    public final TestRule chain = outerRule(reaktor).around(counters).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/non.empty/request",
        "${server}/non.empty/response" })
    public void shouldReceiveNonEmptyMessage() throws Exception
    {
        k3po.finish();
        final long routeId = 0;
        Assert.assertEquals(12, counters.bytesRead(routeId));
        Assert.assertEquals(0, counters.bytesWritten(routeId));
        Assert.assertEquals(1, counters.framesRead(routeId));
        Assert.assertEquals(0, counters.framesWritten(routeId));
    }
}
