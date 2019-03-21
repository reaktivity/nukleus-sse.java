/**
 * Copyright 2016-2019 The Reaktivity Project
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
package org.reaktivity.nukleus.sse.internal.streams.server;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;
import static org.reaktivity.nukleus.sse.internal.SseConfigurationTest.SSE_INITIAL_COMMENT_ENABLED_NAME;
import static org.reaktivity.reaktor.test.ReaktorRule.EXTERNAL_AFFINITY_MASK;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.ReaktorRule;
import org.reaktivity.reaktor.test.annotation.Configure;

public class HandshakeIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/sse/control/route")
            .addScriptRoot("client", "org/reaktivity/specification/sse/handshake")
            .addScriptRoot("server", "org/reaktivity/specification/nukleus/sse/streams/handshake");

    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(4096)
        .nukleus("sse"::equals)
        .affinityMask("target#0", EXTERNAL_AFFINITY_MASK)
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Test
    @Configure(name = SSE_INITIAL_COMMENT_ENABLED_NAME, value = "true")
    @Specification({
            "${route}/server/controller",
            "${client}/initial.comment/request",
            "${server}/last.event.id/response" })
    public void shouldSendInitialComment() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/request.header.last.event.id/request",
        "${server}/last.event.id/response" })
    public void shouldHandshakeWithRequestHeaderLastEventId() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/request.parameter.last.event.id/request",
        "${server}/last.event.id/response" })
    public void shouldHandshakeWithRequestParameterLastEventId() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/request.parameter.last.event.id.url.encoded/request",
        "${server}/last.event.id/response" })
    public void shouldHandshakeWithURLEncodedRequestParameterLastEventId() throws Exception
    {
        k3po.finish();
    }
}
