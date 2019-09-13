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
package org.reaktivity.nukleus.sse.internal;

import static org.junit.Assert.assertEquals;
import static org.reaktivity.nukleus.sse.internal.SseConfiguration.EVENT_TYPE;
import static org.reaktivity.nukleus.sse.internal.SseConfiguration.EVENT_TYPE_NAME;
import static org.reaktivity.nukleus.sse.internal.SseConfiguration.SSE_INITIAL_COMMENT_ENABLED;

import org.junit.Test;

public class SseConfigurationTest
{
    // needed by test annotations
    public static final String SSE_INITIAL_COMMENT_ENABLED_NAME = "nukleus.sse.initial.comment.enabled";

    @Test
    public void shouldVerifyConstants() throws Exception
    {
        assertEquals(SSE_INITIAL_COMMENT_ENABLED.name(), SSE_INITIAL_COMMENT_ENABLED_NAME);
    }

    @Test
    public void shouldMatchEventTypeConfigName()
    {
        assertEquals(EVENT_TYPE_NAME, EVENT_TYPE.name());
    }
}
