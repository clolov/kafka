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
package org.apache.kafka.streams.kstream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static java.time.Duration.ofMillis;
import static org.apache.kafka.streams.EqualityCheck.verifyEquality;
import static org.apache.kafka.streams.EqualityCheck.verifyInEquality;
import static org.apache.kafka.streams.kstream.Windows.DEPRECATED_DEFAULT_24_HR_GRACE_PERIOD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

public class SessionWindowsTest {

    private static final long ANY_SIZE = 123L;
    private static final long ANY_OTHER_SIZE = 456L; // should be larger than anySize
    private static final long ANY_GRACE = 1024L;

    @Test
    public void shouldSetWindowGap() {
        final long anyGap = 42L;

        Assertions.assertEquals(anyGap, SessionWindows.ofInactivityGapWithNoGrace(ofMillis(anyGap)).inactivityGap());
        Assertions.assertEquals(anyGap, SessionWindows.ofInactivityGapAndGrace(ofMillis(anyGap), ofMillis(ANY_GRACE)).inactivityGap());
    }

    @Test
    public void gracePeriodShouldEnforceBoundaries() {
        SessionWindows.ofInactivityGapAndGrace(ofMillis(3L), ofMillis(0));

        try {
            SessionWindows.ofInactivityGapAndGrace(ofMillis(3L), ofMillis(-1L));
            Assertions.fail("should not accept negatives");
        } catch (final IllegalArgumentException e) {
            //expected
        }
    }

    @Test
    public void noGraceAPIShouldNotSetGracePeriod() {
        Assertions.assertEquals(0L, SessionWindows.ofInactivityGapWithNoGrace(ofMillis(3L)).gracePeriodMs());
        Assertions.assertEquals(0L, SessionWindows.ofInactivityGapWithNoGrace(ofMillis(ANY_SIZE)).gracePeriodMs());
        Assertions.assertEquals(0L, SessionWindows.ofInactivityGapWithNoGrace(ofMillis(ANY_OTHER_SIZE)).gracePeriodMs());
    }

    @Test
    public void withGraceAPIShouldSetGracePeriod() {
        Assertions.assertEquals(ANY_GRACE, SessionWindows.ofInactivityGapAndGrace(ofMillis(3L), ofMillis(ANY_GRACE)).gracePeriodMs());
        Assertions.assertEquals(ANY_GRACE, SessionWindows.ofInactivityGapAndGrace(ofMillis(ANY_SIZE), ofMillis(ANY_GRACE)).gracePeriodMs());
        Assertions.assertEquals(ANY_GRACE, SessionWindows.ofInactivityGapAndGrace(ofMillis(ANY_OTHER_SIZE), ofMillis(ANY_GRACE)).gracePeriodMs());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void oldAPIShouldSetDefaultGracePeriod() {
        Assertions.assertEquals(Duration.ofDays(1).toMillis(), DEPRECATED_DEFAULT_24_HR_GRACE_PERIOD);
        Assertions.assertEquals(DEPRECATED_DEFAULT_24_HR_GRACE_PERIOD - 3L, SessionWindows.with(ofMillis(3L)).gracePeriodMs());
        Assertions.assertEquals(0L, SessionWindows.with(ofMillis(DEPRECATED_DEFAULT_24_HR_GRACE_PERIOD)).gracePeriodMs());
        Assertions.assertEquals(0L, SessionWindows.with(ofMillis(DEPRECATED_DEFAULT_24_HR_GRACE_PERIOD + 1L)).gracePeriodMs());
    }

    @Test
    public void windowSizeMustNotBeNegative() {
        assertThrows(IllegalArgumentException.class, () -> SessionWindows.ofInactivityGapWithNoGrace(ofMillis(-1)));
    }

    @Test
    public void windowSizeMustNotBeZero() {
        assertThrows(IllegalArgumentException.class, () -> SessionWindows.ofInactivityGapWithNoGrace(ofMillis(0)));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void graceShouldNotCalledAfterGraceSet() {
        assertThrows(IllegalStateException.class, () -> SessionWindows.ofInactivityGapAndGrace(ofMillis(10), ofMillis(10)).grace(ofMillis(10)));
        assertThrows(IllegalStateException.class, () -> SessionWindows.ofInactivityGapWithNoGrace(ofMillis(10)).grace(ofMillis(10)));
    }

    @Test
    public void equalsAndHashcodeShouldBeValidForPositiveCases() {
        verifyEquality(
            SessionWindows.ofInactivityGapWithNoGrace(ofMillis(1)),
            SessionWindows.ofInactivityGapWithNoGrace(ofMillis(1))
        );

        verifyEquality(
            SessionWindows.ofInactivityGapAndGrace(ofMillis(1), ofMillis(11)),
            SessionWindows.ofInactivityGapAndGrace(ofMillis(1), ofMillis(11))
        );
    }

    @Test
    public void equalsAndHashcodeShouldBeValidForNegativeCases() {
        verifyInEquality(
            SessionWindows.ofInactivityGapWithNoGrace(ofMillis(9)),
            SessionWindows.ofInactivityGapWithNoGrace(ofMillis(1))
        );

        verifyInEquality(
            SessionWindows.ofInactivityGapAndGrace(ofMillis(9), ofMillis(9)),
            SessionWindows.ofInactivityGapAndGrace(ofMillis(1), ofMillis(9))
        );

        verifyInEquality(
            SessionWindows.ofInactivityGapAndGrace(ofMillis(1), ofMillis(9)),
            SessionWindows.ofInactivityGapAndGrace(ofMillis(1), ofMillis(6))
        );
    }
}
