/*
 * Copyright 2013 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ngdata.hbaseindexer.model.api;

import com.google.common.base.Objects;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Information about the last run batch index build, stored as part of an {@link IndexerDefinition}.
 *
 * <p>This object is immutable, construct it using {@link BatchBuildInfoBuilder}.</p>
 */
public class BatchBuildInfo {
    private String jobId;
    private long submitTime;
    private boolean success;
    private String jobState;
    private String trackingUrl;
    private byte[] batchIndexConfiguration;
    private Map<String, Long> counters = new HashMap<String, Long>();

    BatchBuildInfo(String jobId, long submitTime, boolean success, String jobState,
            String trackingUrl, byte[] batchIndexConfiguration, Map<String, Long> counters) {
        this.jobId = jobId;
        this.submitTime = submitTime;
        this.success = success;
        this.jobState = jobState;
        this.trackingUrl = trackingUrl;
        this.batchIndexConfiguration = batchIndexConfiguration;
        this.counters = counters;
    }

    public String getJobId() {
        return jobId;
    }

    public long getSubmitTime() {
        return submitTime;
    }

    public boolean getSuccess() {
        return success;
    }

    public String getJobState() {
        return jobState;
    }

    public String getTrackingUrl() {
        return trackingUrl;
    }

    public Map<String, Long> getCounters() {
        return Collections.unmodifiableMap(counters);
    }

    public byte[] getBatchIndexConfiguration() {
        return batchIndexConfiguration;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        BatchBuildInfo other = (BatchBuildInfo)obj;

        if (!Objects.equal(jobId, other.jobId))
            return false;

        if (submitTime != other.submitTime)
            return false;

        if (success != other.success)
            return false;

        if (!Objects.equal(jobState, other.jobState))
            return false;

        if (!Objects.equal(trackingUrl, other.trackingUrl))
            return false;

        if (!Objects.equal(counters, other.counters))
            return false;

        if (!Arrays.equals(this.batchIndexConfiguration, batchIndexConfiguration))
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = jobId != null ? jobId.hashCode() : 0;
        result = 31 * result + (int) (submitTime ^ (submitTime >>> 32));
        result = 31 * result + (success ? 1 : 0);
        result = 31 * result + (jobState != null ? jobState.hashCode() : 0);
        result = 31 * result + (trackingUrl != null ? trackingUrl.hashCode() : 0);
        result = 31 * result + (counters != null ? counters.hashCode() : 0);
        result = 31 * result + (batchIndexConfiguration != null ? Arrays.hashCode(batchIndexConfiguration) : 0);
        return result;
    }
}
