/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.coomia.flink.tugraph.it;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

/**
 * Testcontainers wrapper that boots a TuGraph-DB server exposing the Bolt port (7687).
 *
 * <p>Image and credentials are overridable via environment variables so the suite can target the
 * image available in a given CI / dev environment:
 * <ul>
 *   <li>{@code TUGRAPH_IMAGE} — default {@value #DEFAULT_IMAGE}</li>
 *   <li>{@code TUGRAPH_USERNAME} / {@code TUGRAPH_PASSWORD} — default {@code admin} / {@code 73@TuGraph}</li>
 * </ul>
 *
 * <p>The whole integration suite is gated on {@code TUGRAPH_IT=1}; without it the tests are skipped,
 * so unit-only CI runs do not require Docker.
 */
public class TuGraphTestContainer extends GenericContainer<TuGraphTestContainer> {

    public static final String DEFAULT_IMAGE = "tugraph/tugraph-runtime-centos7:4.5.0";
    public static final int BOLT_PORT = 7687;

    public TuGraphTestContainer() {
        super(DockerImageName.parse(envOrDefault("TUGRAPH_IMAGE", DEFAULT_IMAGE)));
        withExposedPorts(BOLT_PORT);
        // Most TuGraph runtime images ship a bash entrypoint; start the server with Bolt enabled.
        withCommand("bash", "-c",
                "lgraph_server -c /usr/local/etc/lgraph.json --host 0.0.0.0 --port 7071 "
                        + "--enable_bolt true --bolt_port " + BOLT_PORT + " -d run "
                        + "&& tail -f /dev/null");
        waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(3)));
    }

    public String boltUri() {
        return "bolt://" + getHost() + ":" + getMappedPort(BOLT_PORT);
    }

    public static String username() {
        return envOrDefault("TUGRAPH_USERNAME", "admin");
    }

    public static String password() {
        return envOrDefault("TUGRAPH_PASSWORD", "73@TuGraph");
    }

    private static String envOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isEmpty() ? defaultValue : value;
    }
}
