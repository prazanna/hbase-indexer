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

import java.util.HashMap;
import java.util.Map;

public class BatchBuildInfoBuilder {
    private String jobId;
    private long submitTime;
    private boolean success;
    private String jobState;
    private String trackingUrl;
    private byte[] batchIndexConfiguration;
    private Map<String, Long> counters = new HashMap<String, Long>();

    public BatchBuildInfoBuilder jobId(String jobId) {
        this.jobId = jobId;
        return this;
    }

    public BatchBuildInfoBuilder submitTime(long submitTime) {
        this.submitTime = submitTime;
        return this;
    }

    public BatchBuildInfoBuilder success(boolean success) {
        this.success = success;
        return this;
    }

    public BatchBuildInfoBuilder jobState(String jobState) {
        this.jobState = jobState;
        return this;
    }

    public BatchBuildInfoBuilder trackingUrl(String trackingUrl) {
        this.trackingUrl = trackingUrl;
        return this;
    }

    public BatchBuildInfoBuilder counter(String key, long value) {
        counters.put(key, value);
        return this;
    }

    public BatchBuildInfoBuilder batchIndexConfiguration(byte[] batchIndexConfiguration) {
        this.batchIndexConfiguration = batchIndexConfiguration;
        return this;
    }

    public BatchBuildInfo build() {
        return new BatchBuildInfo(jobId, submitTime, success, jobState, trackingUrl, batchIndexConfiguration,
                new HashMap<String, Long>(counters));
    }
}
