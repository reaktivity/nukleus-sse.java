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
package org.reaktivity.nukleus.sse.internal;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.Configuration;

public class SseConfiguration extends Configuration
{
    public static final String INITIAL_COMMENT_ENABLED = "nukleus.sse.initial.comment.enabled";

    private static final boolean INITIAL_COMMENT_ENABLED_DEFAULT = false;
    private static final DirectBuffer INITIAL_COMMENT_DEFAULT = new UnsafeBuffer(new byte[0]);

    public SseConfiguration(
        Configuration config)
    {
        super(config);
    }

    public DirectBuffer initialComment()
    {
        boolean enabled = getBoolean(INITIAL_COMMENT_ENABLED, INITIAL_COMMENT_ENABLED_DEFAULT);
        return enabled ? INITIAL_COMMENT_DEFAULT : null;
    }

}
