/*
 *   HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 *   (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 *   This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 *   Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 *   to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 *   properly licensed third party, you do not have any rights to this code.
 *
 *   If this code is provided to you under the terms of the AGPLv3:
 *   (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 *   (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *     LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 *   (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *     FROM OR RELATED TO THE CODE; AND
 *   (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *     DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *     DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *     OR LOSS OR CORRUPTION OF DATA.
 */
package com.hortonworks.smm.kafka.services.metric;

import com.google.common.base.Preconditions;

import java.time.Duration;
import java.time.Instant;

public class TimeSpan {
    public static final String ALLOWED_VALUES = "LAST_THIRTY_MINUTES, LAST_ONE_HOUR, LAST_SIX_HOURS, LAST_ONE_DAY, LAST_TWO_DAYS, LAST_ONE_WEEK, LAST_THIRTY_DAYS";
    public static final TimeSpan EMPTY = new TimeSpan(-1L, -1L);

    private Long startTimeMs;
    private Long endTimeMs;
    private TimePeriod timePeriod;

    public enum TimePeriod {
        LAST_THIRTY_MINUTES(Duration.ofMinutes(30)),
        LAST_ONE_HOUR(Duration.ofHours(1)),
        LAST_SIX_HOURS(Duration.ofHours(6)),
        LAST_ONE_DAY(Duration.ofDays(1)),
        LAST_TWO_DAYS(Duration.ofDays(2)),
        LAST_ONE_WEEK(Duration.ofDays(7)),
        LAST_THIRTY_DAYS(Duration.ofDays(30));

        private Duration amount;

        TimePeriod(Duration amount) {
            this.amount = amount;
        }

        public long startTimeMillis() {
            return Instant.now().minus(amount).toEpochMilli();
        }

        public long endTimeMillis() {
            return System.currentTimeMillis();
        }

        public long millis() {
            return amount.toMillis();
        }
    }

    public TimeSpan(Long startTimeMs, Long endTimeMs) {
        this.startTimeMs = startTimeMs;
        this.endTimeMs = endTimeMs;
    }

    public TimeSpan(TimePeriod timePeriod) {
        this.timePeriod = timePeriod;
        this.startTimeMs = timePeriod.startTimeMillis();
        this.endTimeMs = timePeriod.endTimeMillis();
    }

    public Long startTimeMs() {
        return startTimeMs;
    }

    public Long endTimeMs() {
        return endTimeMs;
    }

    public TimePeriod timePeriod() {
        return timePeriod;
    }

    public boolean readFromCache() {
        return timePeriod != null;
    }

    @Override
    public String toString() {
        return "TimeSpan{" +
                "startTimeMs=" + startTimeMs +
                ", endTimeMs=" + endTimeMs +
                ", timePeriod=" + timePeriod +
                '}';
    }

    public static TimeSpan from(String duration, Long fromInMs, Long toInMs) {
        if (duration == null || duration.isEmpty()) {
            Preconditions.checkArgument(fromInMs != null && fromInMs >= -1L, "From time period should be >= -1 when duration is empty");
            Preconditions.checkArgument(toInMs != null && toInMs >= -1L, "To time period should be >= -1 when duration is empty");
            Preconditions.checkArgument(toInMs >= fromInMs, "From time period shouldn't be greater than To");
        }

        if (duration != null && !duration.isEmpty()) {
            TimePeriod timePeriod = TimePeriod.valueOf(duration);
            return new TimeSpan(timePeriod);
        } else {
            return new TimeSpan(fromInMs, toInMs);
        }
    }

}
