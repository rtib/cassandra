/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.management.api;

import java.io.IOException;
import javax.inject.Inject;

import com.google.common.base.Throwables;

import org.apache.cassandra.management.ManagementContext;
import org.apache.cassandra.tools.INodeProbeFactory;
import org.apache.cassandra.tools.Output;
import picocli.CommandLine;

import static java.lang.Integer.parseInt;
import static org.apache.commons.lang3.StringUtils.EMPTY;

/**
 * Command options for NodeTool commands that are executed via JMX.
 */
@CommandLine.Command(name = "connect", description = "Connect NodeTool to a Cassandra node using JMX")
public class JmxConnect implements Runnable
{
    public static final String MIXIN_KEY = "jmx";

    @CommandLine.Option(names = { "-h", "--host"}, description = "Node hostname or ip address")
    private String host = "127.0.0.1";

    @CommandLine.Option(names = {"-p", "--port"}, description = "Remote jmx agent port number")
    private String port = "7199";

    @CommandLine.Option(names = {"-u", "--username"}, description = "Remote jmx agent username")
    private String username = EMPTY;

    @CommandLine.Option(names = {"-pw", "--password"}, description = "Remote jmx agent password")
    private String password = EMPTY;

    @CommandLine.Option(names = {"-pwf", "--password-file"}, description = "Path to the JMX password file")
    private String passwordFilePath = EMPTY;

    @CommandLine.Option(names = { "-pp", "--print-port"}, description = "Operate in 4.0 mode with hosts disambiguated by port number")
    private boolean printPort = false;

    @Inject
    private INodeProbeFactory nodeProbeFactory;
    @Inject
    private Output output;
    public ManagementContext nodeClient;

    public ManagementContext init()
    {
        try
        {
            if (username.isEmpty())
                nodeClient = nodeProbeFactory.create(host, parseInt(port));
            else
                nodeClient = nodeProbeFactory.create(host, parseInt(port), username, password);

            return nodeClient;
        }
        catch (IOException | SecurityException e)
        {
            Throwable rootCause = Throwables.getRootCause(e);
            output.err.printf("nodetool: Failed to connect to '%s:%s' - %s: '%s'.%n", host, port,
                              rootCause.getClass().getSimpleName(), rootCause.getMessage());
        }

        return null;
    }

    @Override
    public void run()
    {
        // no-op
    }
}
