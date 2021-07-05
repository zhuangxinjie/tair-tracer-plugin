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

import com.aliyun.tair.springdata.extend.connection.TairJedisClusterConnection;
import com.sofa.alipay.tracer.plugins.spring.tair.common.TairActionWrapperHelper;
import com.sofa.alipay.tracer.plugins.spring.tair.connections.TracingJedisClusterConnection;
import com.sofa.alipay.tracer.plugins.spring.tair.connections.TracingJedisConnection;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.data.redis.connection.*;
import org.springframework.data.redis.connection.jedis.JedisClientConfiguration;
import org.springframework.data.redis.connection.jedis.JedisClusterConnection;
import org.springframework.data.redis.connection.jedis.JedisConnection;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.util.Assert;
import redis.clients.jedis.*;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class TracingJedisConnectionFactory extends JedisConnectionFactory {

    private final TairActionWrapperHelper actionWrapper;
    private final JedisConnectionFactory  delegate;

    public TracingJedisConnectionFactory(TairActionWrapperHelper actionWrapper,
                                         JedisConnectionFactory delegate) {
        this.actionWrapper = actionWrapper;
        this.delegate = delegate;
    }

    @Override
    public RedisConnection getConnection() {
        RedisConnection connection = this.delegate.getConnection();
        if (this.isRedisClusterAware()) {
            return this.getClusterConnection();
        }
        return new TracingJedisConnection(this.createJedis(), (JedisConnection) connection,
            actionWrapper);
    }

    @Override
    public RedisClusterConnection getClusterConnection() {
        JedisCluster cluster = this.createCluster();
        ClusterCommandExecutor executor = this.getClusterCommandExecutorwithReflect();
        ClusterTopologyProvider topologyProvider = this.getClusterTopologyProviderwithReflect();
        if (null != cluster && null == executor && null == topologyProvider) {
            return new TracingJedisClusterConnection(cluster, actionWrapper,
                (TairJedisClusterConnection) delegate.getClusterConnection());
        } else if (null != cluster && null != executor && null == topologyProvider) {
            return new TracingJedisClusterConnection(cluster, executor, actionWrapper,
                (TairJedisClusterConnection) delegate.getClusterConnection());
        } else if (null != cluster && null != executor && null != topologyProvider) {
            return new TracingJedisClusterConnection(cluster, executor, topologyProvider,
                actionWrapper, (TairJedisClusterConnection) delegate.getClusterConnection());
        } else {
            return null;
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

    private JedisCluster createCluster() {
        return this.createCluster(this.getRedisClusterConfiguration(), this.getPoolConfig());
    }

    protected ClusterTopologyProvider createTopologyProvider(JedisCluster cluster) {
        return new JedisClusterConnection.JedisClusterTopologyProvider(cluster);
    }

    protected JedisCluster createCluster(RedisClusterConfiguration clusterConfig,
                                         GenericObjectPoolConfig poolConfig) {
        Assert.notNull(clusterConfig, "Cluster configuration must not be null!");
        Set<HostAndPort> hostAndPort = new HashSet();
        Iterator var4 = clusterConfig.getClusterNodes().iterator();

        while (var4.hasNext()) {
            RedisNode node = (RedisNode) var4.next();
            hostAndPort.add(new HostAndPort(node.getHost(), node.getPort()));
        }

        int redirects = clusterConfig.getMaxRedirects() != null ? clusterConfig.getMaxRedirects()
            : 5;
        return new JedisCluster(hostAndPort, this.getConnectTimeout(), this.getReadTimeout(),
            redirects, this.getPassword(), this.getClientName(), poolConfig, this.isUseSsl(),
            (SSLSocketFactory) this.getJedisClientConfiguration().getSslSocketFactory()
                .orElse(null), (SSLParameters) this.getJedisClientConfiguration()
                .getSslParameters().orElse(null), (HostnameVerifier) this
                .getJedisClientConfiguration().getHostnameVerifier().orElse(null),
            (JedisClusterHostAndPortMap) null);
    }

    private RedisClusterConfiguration getRedisClusterConfiguration() {
        return delegate.getClusterConfiguration();
    }

    public JedisClientConfiguration getJedisClientConfiguration() {
        return delegate.getClientConfiguration();
    }

    private int getConnectTimeout() {
        return Math.toIntExact(this.getJedisClientConfiguration().getConnectTimeout().toMillis());
    }

    private int getReadTimeout() {
        return Math.toIntExact(this.getJedisClientConfiguration().getReadTimeout().toMillis());
    }

    private Jedis createJedis() {
        if (this.getProvidedShardInfo()) {
            return new Jedis(this.getJedisShardInfo());
        } else {
            Jedis jedis = new Jedis(this.getHostName(), this.getPort(), this.getConnectTimeout(), this.getReadTimeout(), this.isUseSsl(), (SSLSocketFactory)this.getJedisClientConfiguration().getSslSocketFactory().orElse(null), (SSLParameters)this.getJedisClientConfiguration().getSslParameters().orElse(null), (HostnameVerifier)this.getJedisClientConfiguration().getHostnameVerifier().orElse(null));
            Client client = jedis.getClient();
            this.getRedisPassword().map(String::new).ifPresent(client::setPassword);
            client.setDb(this.getDatabase());
            return jedis;
        }

    }

    private Boolean getProvidedShardInfo() {
        try {
            Field privateStringField = JedisConnectionFactory.class
                .getDeclaredField("providedShardInfo");
            privateStringField.setAccessible(true);
            return (Boolean) privateStringField.get(delegate);
        } catch (Exception var2) {
            throw new RuntimeException(var2);
        }
    }

    private JedisShardInfo getJedisShardInfo() {
        try {
            Field privateStringField = JedisConnectionFactory.class.getDeclaredField("shardInfo");
            privateStringField.setAccessible(true);
            return (JedisShardInfo) privateStringField.get(delegate);
        } catch (Exception var2) {
            throw new RuntimeException(var2);
        }
    }

    private RedisStandaloneConfiguration getRedisStandaloneConfiguration() {
        try {
            Field privateStringField = JedisConnectionFactory.class
                .getDeclaredField("standaloneConfig");
            privateStringField.setAccessible(true);
            return (RedisStandaloneConfiguration) privateStringField.get(delegate);
        } catch (Exception var2) {
            throw new RuntimeException(var2);
        }
    }

    private RedisConfiguration getRedisConfiguration() {
        try {
            Field privateStringField = JedisConnectionFactory.class
                .getDeclaredField("configuration");
            privateStringField.setAccessible(true);
            return (RedisConfiguration) privateStringField.get(delegate);
        } catch (Exception var2) {
            throw new RuntimeException(var2);
        }
    }

    private RedisPassword getRedisPassword() {
        RedisConfiguration var10000 = this.getRedisConfiguration();
        RedisStandaloneConfiguration var10001 = this.getRedisStandaloneConfiguration();
        var10001.getClass();
        return RedisConfiguration.getPasswordOrElse(var10000, var10001::getPassword);
    }
}
