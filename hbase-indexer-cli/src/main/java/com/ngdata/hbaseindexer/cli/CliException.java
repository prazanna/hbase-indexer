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
package com.ngdata.hbaseindexer.cli;

/**
 * CliException's can be used to stop the CLI tool with a certain exit code and
 * printing a message.
 *
 * <p>It shouldn't be used in cases where stack traces and nested exceptions
 * are important. They are just for flow control.</p>
 */
public class CliException extends RuntimeException {
    private int exitCode;

    public CliException(String message, int exitCode) {
        super(message);
        this.exitCode = exitCode;
    }

    public CliException(String message) {
        this(message, 1);
    }

    public int getExitCode() {
        return exitCode;
    }
}
