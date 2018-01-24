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
package org.reaktivity.nukleus.sse.internal.types.codec;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.sse.internal.types.Flyweight;

public final class SseEventFW extends Flyweight
{
    private static final byte[] DATA_FIELD_HEADER = "data:".getBytes(UTF_8);
    private static final byte[] ID_FIELD_HEADER = "id:".getBytes(UTF_8);
    private static final byte[] TYPE_FIELD_HEADER = "event:".getBytes(UTF_8);

    private static final byte FIELD_TRAILER = 0x0a;
    private static final int FIELD_TRAILER_LENGTH = 1;

    private static final byte EVENT_TRAILER = 0x0a;
    private static final int EVENT_TRAILER_LENGTH = 1;

    @Override
    public int limit()
    {
        // TODO
        return maxLimit();
    }

    @Override
    public SseEventFW wrap(DirectBuffer buffer, int offset, int maxLimit)
    {
        super.wrap(buffer, offset, maxLimit);

        checkLimit(limit(), maxLimit);

        return this;
    }

    @Override
    public String toString()
    {
        return String.format("[TODO]");
    }

    public static final class Builder extends Flyweight.Builder<SseEventFW>
    {
        public Builder()
        {
            super(new SseEventFW());
        }

        @Override
        public Builder wrap(MutableDirectBuffer buffer, int offset, int maxLimit)
        {
            super.wrap(buffer, offset, maxLimit);

            checkLimit(limit() +
                       EVENT_TRAILER_LENGTH,
                       maxLimit);

            buffer().putByte(limit(), EVENT_TRAILER);
            limit(limit() + EVENT_TRAILER_LENGTH);

            return this;
        }

        public Builder data(
            String data)
        {
            if (data != null)
            {
                final byte[] dataBytes = data.getBytes(UTF_8);

                checkLimit(limit() +
                           DATA_FIELD_HEADER.length +
                           dataBytes.length +
                           FIELD_TRAILER_LENGTH,
                           maxLimit());

                limit(limit() - EVENT_TRAILER_LENGTH);

                buffer().putBytes(limit(), DATA_FIELD_HEADER);
                limit(limit() + DATA_FIELD_HEADER.length);

                buffer().putBytes(limit(), dataBytes);
                limit(limit() + dataBytes.length);

                buffer().putByte(limit(), FIELD_TRAILER);
                limit(limit() + FIELD_TRAILER_LENGTH);

                buffer().putByte(limit(), EVENT_TRAILER);
                limit(limit() + EVENT_TRAILER_LENGTH);
            }

            return this;
        }

        public Builder id(
            String id)
        {
            if (id != null)
            {
                final byte[] idBytes = id.getBytes(UTF_8);

                checkLimit(limit() +
                           ID_FIELD_HEADER.length +
                           idBytes.length +
                           FIELD_TRAILER_LENGTH,
                           maxLimit());

                limit(limit() - EVENT_TRAILER_LENGTH);

                buffer().putBytes(limit(), ID_FIELD_HEADER);
                limit(limit() + ID_FIELD_HEADER.length);

                buffer().putBytes(limit(), idBytes);
                limit(limit() + idBytes.length);

                buffer().putByte(limit(), FIELD_TRAILER);
                limit(limit() + FIELD_TRAILER_LENGTH);

                buffer().putByte(limit(), EVENT_TRAILER);
                limit(limit() + EVENT_TRAILER_LENGTH);
            }

            return this;
        }

        public Builder type(
            String type)
        {
            if (type != null)
            {
                final byte[] typeBytes = type.getBytes(UTF_8);

                checkLimit(limit() +
                           TYPE_FIELD_HEADER.length +
                           typeBytes.length +
                           FIELD_TRAILER_LENGTH,
                           maxLimit());

                limit(limit() - EVENT_TRAILER_LENGTH);

                buffer().putBytes(limit(), TYPE_FIELD_HEADER);
                limit(limit() + TYPE_FIELD_HEADER.length);

                buffer().putBytes(limit(), typeBytes);
                limit(limit() + typeBytes.length);

                buffer().putByte(limit(), FIELD_TRAILER);
                limit(limit() + FIELD_TRAILER_LENGTH);

                buffer().putByte(limit(), EVENT_TRAILER);
                limit(limit() + EVENT_TRAILER_LENGTH);
            }

            return this;
        }
    }
}
