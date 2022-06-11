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
package org.apache.kafka.streams.state;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class HostInfoTest {
    
    @Test
    public void shouldCreateHostInfo() {
        final String endPoint = "host:9090";
        final HostInfo hostInfo = HostInfo.buildFromEndpoint(endPoint);

        assertThat(hostInfo.host(), is("host"));
        assertThat(hostInfo.port(), is(9090));
    }

    @Test
    public void shouldReturnNullHostInfoForNullEndPoint() {
        Assertions.assertNull(HostInfo.buildFromEndpoint(null));
    }

    @Test
    public void shouldReturnNullHostInfoForEmptyEndPoint() {
        Assertions.assertNull(HostInfo.buildFromEndpoint("  "));
    }

    @Test
    public void shouldThrowConfigExceptionForNonsenseEndPoint() {
        Assertions.assertThrows(ConfigException.class, () -> HostInfo.buildFromEndpoint("nonsense"));
    }
}
