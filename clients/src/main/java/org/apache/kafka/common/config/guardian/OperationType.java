/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.config.guardian;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum OperationType {
    ALL((byte) -1),

    /**
     * Set the value of the configuration entry.
     */
    SET((byte) 0),

    /**
     * Revert the configuration entry to the default value (possibly null).
     */
    DELETE((byte) 1),
    /**
     * (For list-type configuration entries only.) Add the specified values to the
     * current value of the configuration entry. If the configuration value has not been set,
     * adds to the default value.
     */
    APPEND((byte) 2),
    /**
     * (For list-type configuration entries only.) Removes the specified values from the current
     * value of the configuration entry. It is legal to remove values that are not currently in the
     * configuration entry. Removing all entries from the current configuration value leaves an empty
     * list and does NOT revert to the default value of the entry.
     */
    SUBTRACT((byte) 3);

    private static final Map<Byte, OperationType> OP_TYPES = Collections.unmodifiableMap(
            Arrays.stream(values()).collect(Collectors.toMap(OperationType::getId, Function.identity()))
    );

    private final byte id;

    OperationType(final byte id) {
        this.id = id;
    }

    public byte getId() {
        return id;
    }

    public static OperationType forId(final byte id) {
        return OP_TYPES.get(id);
    }
}
