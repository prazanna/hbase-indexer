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

/**
 * Information about a currently running batch indexing build, stored as part of the {@link IndexerDefinition}.
 *
 * <p>This object is immutable, construct it using {@link ActiveBatchBuildInfoBuilder}.</p>
 */
public class ActiveBatchBuildInfo {
    private String jobId;
    private long submitTime;
    private String trackingUrl;
    private byte[] batchIndexConfiguration;

    ActiveBatchBuildInfo(String jobId, long submitTime, String trackingUrl, byte[] batchIndexConfiguration) {
        this.jobId = jobId;
        this.submitTime = submitTime;
        this.trackingUrl = trackingUrl;
        this.batchIndexConfiguration = batchIndexConfiguration;
    }

    public String getJobId() {
        return jobId;
    }

    public long getSubmitTime() {
        return submitTime;
    }

    public String getTrackingUrl() {
        return trackingUrl;
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
        ActiveBatchBuildInfo other = (ActiveBatchBuildInfo)obj;

        if (!Objects.equal(jobId, other.jobId))
            return false;

        if (submitTime != other.submitTime)
            return false;

        if (!Objects.equal(trackingUrl, other.trackingUrl))
            return false;

        if (!Arrays.equals(batchIndexConfiguration, other.batchIndexConfiguration))
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = jobId != null ? jobId.hashCode() : 0;
        result = 31 * result + (int) (submitTime ^ (submitTime >>> 32));
        result = 31 * result + (trackingUrl != null ? trackingUrl.hashCode() : 0);
        result = 31 * result + (batchIndexConfiguration != null ? Arrays.hashCode(batchIndexConfiguration) : 0);
        return result;
    }
}
