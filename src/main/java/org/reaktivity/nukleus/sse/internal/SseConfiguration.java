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

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.sse.internal.types.StringFW;

public class SseConfiguration extends Configuration
{
    public static final String EVENT_TYPE_NAME = "nukleus.sse.event.type";

    public static final BooleanPropertyDef SSE_INITIAL_COMMENT_ENABLED;

    private static final DirectBuffer INITIAL_COMMENT_DEFAULT = new UnsafeBuffer(new byte[0]);

    private static final ConfigurationDef SSE_CONFIG;

    static final PropertyDef<String> EVENT_TYPE;

    static
    {
        final ConfigurationDef config = new ConfigurationDef("nukleus.sse");
        SSE_INITIAL_COMMENT_ENABLED = config.property("initial.comment.enabled", false);
        EVENT_TYPE = config.property("event.type", "challenge");
        SSE_CONFIG = config;
    }

    public SseConfiguration(
        Configuration config)
    {
        super(SSE_CONFIG, config);
    }

    public DirectBuffer initialComment()
    {
        return SSE_INITIAL_COMMENT_ENABLED.getAsBoolean(this) ? INITIAL_COMMENT_DEFAULT : null;
    }

    public StringFW getEventType() {
        return new StringFW(EVENT_TYPE.get(this));
    }

}
