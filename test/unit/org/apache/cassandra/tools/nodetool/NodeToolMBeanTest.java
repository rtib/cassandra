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

package org.apache.cassandra.tools.nodetool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.QueryExp;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.utils.MBeanWrapper;
import org.mockito.Mockito;

import static org.apache.cassandra.config.CassandraRelevantProperties.MBEAN_REGISTRATION_CLASS;
import static org.mockito.Mockito.when;

public class NodeToolMBeanTest extends CQLToolRunnerTester
{
    private static MBeanMockInstance mbeanMockInstance;

    @BeforeClass
    public static void setUpClass()
    {
        CassandraRelevantProperties.DTEST_IS_IN_JVM_DTEST.setBoolean(true);
        MBEAN_REGISTRATION_CLASS.setString(MBeanMockInstance.class.getName());
        mbeanMockInstance = (MBeanMockInstance) ((MBeanWrapper.DelegatingMbeanWrapper) MBeanWrapper.instance).getDelegate();
        CQLToolRunnerTester.setUpClass();
    }

    @AfterClass
    public static void resetMBeanWrapper()
    {
        CassandraRelevantProperties.DTEST_IS_IN_JVM_DTEST.setBoolean(false);
    }

    @Test
    public void keyPresent() throws Throwable
    {
        long token = 42;
        long key = Murmur3Partitioner.LongToken.keyForToken(token).getLong();
        String table = "table";
        StorageServiceMBean mock = getMock(mbeanMockInstance, StorageServiceMBean.class);
        when(mock.getKeyspaces()).thenReturn(List.of(keyspace()));
        when(mock.getNonSystemKeyspaces()).thenReturn(List.of(keyspace()));

        invokeNodetool("compact", "--partition", Long.toString(key), keyspace(), table).assertOnCleanExit();
        Mockito.verify(mock).forceKeyspaceCompactionForPartitionKey(keyspace(), Long.toString(key), table);
    }

    private static <T> T getMock(MBeanMockInstance mbeanMockInstance, Class<T> clz)
    {
        return clz.cast(mbeanMockInstance.mocks.get(clz));
    }

    public static class MBeanMockInstance implements MBeanWrapper
    {
        private final Map<ObjectName, Object> mbeans = new HashMap<>();
        private final Map<Class<?>, Object> mocks = new HashMap<>();
        private final PlatformMBeanWrapper wrapper = new PlatformMBeanWrapper();

        public MBeanMockInstance()
        {
        }

        @Override
        public void registerMBean(Object obj, ObjectName mbeanName, OnException onException)
        {
            Class<?> clz = obj.getClass();
            Object mock = obj;
            if (mbeanName.toString().equals("org.apache.cassandra.db:type=StorageService"))
            {
                mbeans.put(mbeanName, mock = Mockito.mock(clz));
                mocks.put(StorageServiceMBean.class, mock);
            }

            wrapper.registerMBean(mock, mbeanName, onException);
        }

        @Override
        public boolean isRegistered(ObjectName mbeanName, OnException onException)
        {
            if (mbeans.containsKey(mbeanName))
                return true;
            return wrapper.isRegistered(mbeanName, onException);
        }

        @Override
        public void unregisterMBean(ObjectName mbeanName, OnException onException)
        {
            if (mbeans.containsKey(mbeanName))
                mbeans.remove(mbeanName);
            else
                wrapper.unregisterMBean(mbeanName, onException);
        }

        @Override
        public Set<ObjectName> queryNames(ObjectName name, QueryExp query)
        {
            return wrapper.queryNames(name, query);
        }

        @Override
        public MBeanServer getMBeanServer()
        {
            return wrapper.getMBeanServer();
        }
    }
}
