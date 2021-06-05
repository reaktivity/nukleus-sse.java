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

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.bind.adapter.JsonbAdapter;

import org.reaktivity.nukleus.sse.internal.SseNukleus;
import org.reaktivity.reaktor.config.Options;
import org.reaktivity.reaktor.config.OptionsAdapterSpi;

public final class SseOptionsAdapter implements OptionsAdapterSpi, JsonbAdapter<Options, JsonObject>
{
    private static final String RETRY_NAME = "retry";

    @Override
    public String type()
    {
        return SseNukleus.NAME;
    }

    @Override
    public JsonObject adaptToJson(
        Options options)
    {
        SseOptions sseOptions = (SseOptions) options;

        JsonObjectBuilder object = Json.createObjectBuilder();

        if (sseOptions.retry != SseOptions.RETRY_DEFAULT)
        {
            object.add(RETRY_NAME, sseOptions.retry);
        }

        return object.build();
    }

    @Override
    public Options adaptFromJson(
        JsonObject object)
    {
        int retry = object.containsKey(RETRY_NAME)
                ? object.getInt(RETRY_NAME)
                : SseOptions.RETRY_DEFAULT;

        return new SseOptions(retry);
    }
}
