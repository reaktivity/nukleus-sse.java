/**
 * Copyright 2016-2021 The Reaktivity Project
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
package org.reaktivity.nukleus.sse.internal.config;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class SseMatcher
{
    private final Matcher path;

    public SseMatcher(
        SseCondition condition)
    {
        this.path = condition.path != null ? asMatcher(condition.path) : null;
    }

    public boolean matches(
        String path)
    {
        return matchPath(path);
    }

    private boolean matchPath(
        String path)
    {
        return this.path == null || this.path.reset(path).matches();
    }

    private static Matcher asMatcher(
        String wildcard)
    {
        return Pattern.compile(wildcard.replace(".", "\\.").replace("*", ".*")).matcher("");
    }
}
