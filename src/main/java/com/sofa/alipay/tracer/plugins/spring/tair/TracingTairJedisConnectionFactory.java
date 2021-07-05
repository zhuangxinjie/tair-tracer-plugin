/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sofa.alipay.tracer.plugins.spring.tair;

import com.aliyun.tair.springdata.TairJedisConnectionFactory;
import com.aliyun.tair.springdata.extend.commands.TairConnection;
import com.aliyun.tair.springdata.extend.connection.TairJedisClusterConnection;
import com.sofa.alipay.tracer.plugins.spring.tair.common.TairActionWrapperHelper;
import com.sofa.alipay.tracer.plugins.spring.tair.connections.TracingRedisSentinelConnection;
import com.sofa.alipay.tracer.plugins.spring.tair.connections.TracingTairConnection;
import com.sofa.alipay.tracer.plugins.spring.tair.connections.TracingTairJedisClusterConnection;
import org.springframework.data.redis.connection.*;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import redis.clients.jedis.JedisCluster;

import java.lang.reflect.Field;

/**
 * @ClassName: TracingTairJedisConnectionFactory
 * @Description:
 * @Author: zhuangxinjie
 * @Date: 2021/6/19 10:19 上午
 */
public class TracingTairJedisConnectionFactory extends TairJedisConnectionFactory {
    private final TairActionWrapperHelper    actionWrapper;
    private final TairJedisConnectionFactory delegate;

    public TracingTairJedisConnectionFactory(TairActionWrapperHelper actionWrapper,
                                             TairJedisConnectionFactory delegate) {
        this.actionWrapper = actionWrapper;
        this.delegate = delegate;
    }

    @Override
    public RedisConnection getConnection() {

        RedisConnection connection = this.delegate.getConnection();
        if (connection instanceof RedisClusterConnection) {
            return this.getClusterConnection();
        }
        return new TracingTairConnection(actionWrapper, (TairConnection) connection);
    }

    @Override
    public RedisClusterConnection getClusterConnection() {
        JedisCluster jedisCluster = this.getJedisClusterwithReflect();
        ClusterCommandExecutor clusterCommandExecutor = this.getClusterCommandExecutorwithReflect();
        ClusterTopologyProvider clusterTopologyProvider = this
            .getClusterTopologyProviderwithReflect();
        if (null != jedisCluster && null == clusterCommandExecutor
            && null == clusterTopologyProvider) {
            return new TracingTairJedisClusterConnection(jedisCluster, actionWrapper,
                (TairJedisClusterConnection) delegate.getClusterConnection());
        } else if (null != jedisCluster && null != clusterCommandExecutor
                   && null == clusterTopologyProvider) {
            return new TracingTairJedisClusterConnection(jedisCluster, clusterCommandExecutor,
                actionWrapper, (TairJedisClusterConnection) delegate.getClusterConnection());
        } else if (null != jedisCluster && null != clusterCommandExecutor
                   && null != clusterTopologyProvider) {
            return new TracingTairJedisClusterConnection(jedisCluster, clusterCommandExecutor,
                clusterTopologyProvider, actionWrapper,
                (TairJedisClusterConnection) delegate.getClusterConnection());
        } else {
            return null;
        }

    }

    @Override
    public boolean getConvertPipelineAndTxResults() {
        return delegate.getConvertPipelineAndTxResults();
    }

    @Override
    public RedisSentinelConnection getSentinelConnection() {
        return new TracingRedisSentinelConnection(delegate.getSentinelConnection(), actionWrapper);
    }

    private JedisCluster getJedisClusterwithReflect() {
        try {
            Field privateStringField = JedisConnectionFactory.class.getDeclaredField("cluster");
            privateStringField.setAccessible(true);
            return (JedisCluster) privateStringField.get(delegate);
        } catch (Exception var2) {
            throw new RuntimeException(var2);
        }
    }

    private ClusterCommandExecutor getClusterCommandExecutorwithReflect() {
        try {
            Field privateStringField = JedisConnectionFactory.class
                .getDeclaredField("clusterCommandExecutor");
            privateStringField.setAccessible(true);
            return (ClusterCommandExecutor) privateStringField.get(delegate);
        } catch (Exception var2) {
            throw new RuntimeException(var2);
        }
    }

    private ClusterTopologyProvider getClusterTopologyProviderwithReflect() {
        try {
            Field privateStringField = JedisConnectionFactory.class
                .getDeclaredField("topologyProvider");
            privateStringField.setAccessible(true);
            return (ClusterTopologyProvider) privateStringField.get(delegate);
        } catch (Exception var2) {
            throw new RuntimeException(var2);
        }
    }
}
