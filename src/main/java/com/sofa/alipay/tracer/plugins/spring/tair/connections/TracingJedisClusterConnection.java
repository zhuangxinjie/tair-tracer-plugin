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
package com.sofa.alipay.tracer.plugins.spring.tair.connections;

import com.sofa.alipay.tracer.plugins.spring.tair.common.TairActionWrapperHelper;
import com.sofa.alipay.tracer.plugins.spring.tair.common.TairAndRedisCommand;
import org.springframework.dao.DataAccessException;
import org.springframework.data.geo.*;
import org.springframework.data.redis.connection.*;
import org.springframework.data.redis.connection.jedis.JedisClusterConnection;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.core.types.RedisClientInfo;
import redis.clients.jedis.JedisCluster;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName: TracingJedisClusterConnection
 * @Description:
 * @Author: zhuangxinjie
 * @Date: 2021/6/25 5:47 下午
 */
public class TracingJedisClusterConnection extends JedisClusterConnection {
    private final TairActionWrapperHelper actionWrapper;
    private final JedisClusterConnection  jedisClusterConnection;

    public TracingJedisClusterConnection(JedisCluster cluster,
                                         TairActionWrapperHelper actionWrapper,
                                         JedisClusterConnection jedisClusterConnection) {
        super(cluster);
        this.actionWrapper = actionWrapper;
        this.jedisClusterConnection = jedisClusterConnection;
    }

    public TracingJedisClusterConnection(JedisCluster cluster, ClusterCommandExecutor executor,
                                         TairActionWrapperHelper actionWrapper,
                                         JedisClusterConnection jedisClusterConnection) {
        super(cluster, executor);
        this.actionWrapper = actionWrapper;
        this.jedisClusterConnection = jedisClusterConnection;
    }

    public TracingJedisClusterConnection(JedisCluster cluster, ClusterCommandExecutor executor,
                                         ClusterTopologyProvider topologyProvider,
                                         TairActionWrapperHelper actionWrapper,
                                         JedisClusterConnection jedisClusterConnection) {
        super(cluster, executor, topologyProvider);
        this.actionWrapper = actionWrapper;
        this.jedisClusterConnection = jedisClusterConnection;
    }

    @Override
    public RedisGeoCommands geoCommands() {
        return jedisClusterConnection.geoCommands();
    }

    @Override
    public RedisHashCommands hashCommands() {
        return jedisClusterConnection.hashCommands();
    }

    @Override
    public RedisHyperLogLogCommands hyperLogLogCommands() {
        return jedisClusterConnection.hyperLogLogCommands();
    }

    @Override
    public RedisKeyCommands keyCommands() {
        return jedisClusterConnection.keyCommands();
    }

    @Override
    public RedisListCommands listCommands() {
        return jedisClusterConnection.listCommands();
    }

    @Override
    public RedisSetCommands setCommands() {
        return jedisClusterConnection.setCommands();
    }

    @Override
    public RedisScriptingCommands scriptingCommands() {
        return jedisClusterConnection.scriptingCommands();
    }

    @Override
    public RedisClusterServerCommands serverCommands() {
        return jedisClusterConnection.serverCommands();
    }

    @Override
    public RedisStringCommands stringCommands() {
        return jedisClusterConnection.stringCommands();
    }

    @Override
    public RedisZSetCommands zSetCommands() {
        return jedisClusterConnection.zSetCommands();
    }

    @Override
    public void close() throws DataAccessException {
        jedisClusterConnection.close();
    }

    @Override
    public boolean isClosed() {
        return jedisClusterConnection.isClosed();
    }

    @Override
    public JedisCluster getNativeConnection() {
        return jedisClusterConnection.getNativeConnection();
    }

    @Override
    public boolean isQueueing() {
        return jedisClusterConnection.isQueueing();
    }

    @Override
    public boolean isPipelined() {
        return jedisClusterConnection.isPipelined();
    }

    @Override
    public void openPipeline() {
        jedisClusterConnection.openPipeline();
    }

    @Override
    public List<Object> closePipeline() throws RedisPipelineException {
        return jedisClusterConnection.closePipeline();
    }

    @Override
    public RedisSentinelConnection getSentinelConnection() {
        return jedisClusterConnection.getSentinelConnection();
    }

    @Override
    public Object execute(String command, byte[]... bytes) {
        return actionWrapper.doInScope(command, () -> jedisClusterConnection.execute(command, bytes));
    }

    @Override
    public void select(int dbIndex) {
        actionWrapper.doInScope(TairAndRedisCommand.SELECT, () -> jedisClusterConnection.select(dbIndex));
    }

    @Override
    public byte[] echo(byte[] message) {
        return actionWrapper.doInScope(TairAndRedisCommand.ECHO, () -> jedisClusterConnection.echo(message));
    }

    @Override
    public String ping() {
        return actionWrapper.doInScope(TairAndRedisCommand.PING, () -> jedisClusterConnection.ping());
    }

    @Override
    public Long geoAdd(byte[] bytes, Point point, byte[] bytes1) {
        return actionWrapper.doInScope(TairAndRedisCommand.GEOADD, bytes, () -> jedisClusterConnection.geoAdd(bytes, point, bytes1));
    }

    @Override
    public Long geoAdd(byte[] key, GeoLocation<byte[]> location) {
        return actionWrapper.doInScope(TairAndRedisCommand.GEOADD, key, () -> jedisClusterConnection.geoAdd(key, location));
    }

    @Override
    public Long geoAdd(byte[] bytes, Map<byte[], Point> map) {
        return actionWrapper.doInScope(TairAndRedisCommand.GEOADD, bytes, () -> jedisClusterConnection.geoAdd(bytes, map));
    }

    @Override
    public Long geoAdd(byte[] bytes, Iterable<GeoLocation<byte[]>> iterable) {
        return actionWrapper.doInScope(TairAndRedisCommand.GEOADD, bytes, () -> jedisClusterConnection.geoAdd(bytes, iterable));

    }

    @Override
    public Distance geoDist(byte[] bytes, byte[] bytes1, byte[] bytes2) {
        return actionWrapper.doInScope(TairAndRedisCommand.GEODIST, bytes, () -> jedisClusterConnection.geoDist(bytes, bytes1, bytes2));
    }

    @Override
    public Distance geoDist(byte[] bytes, byte[] bytes1, byte[] bytes2, Metric metric) {
        return actionWrapper.doInScope(TairAndRedisCommand.GEODIST, bytes, () -> jedisClusterConnection.geoDist(bytes, bytes1, bytes2, metric));
    }

    @Override
    public List<String> geoHash(byte[] bytes, byte[]... bytes1) {
        return actionWrapper.doInScope(TairAndRedisCommand.GEOHASH, bytes, () -> jedisClusterConnection.geoHash(bytes, bytes1));
    }

    @Override
    public List<Point> geoPos(byte[] bytes, byte[]... bytes1) {
        return actionWrapper.doInScope(TairAndRedisCommand.GEOPOS, bytes, () -> jedisClusterConnection.geoPos(bytes, bytes1));
    }

    @Override
    public GeoResults<GeoLocation<byte[]>> geoRadius(byte[] bytes, Circle circle) {
        return actionWrapper.doInScope(TairAndRedisCommand.GEORADIUS, bytes, () -> jedisClusterConnection.geoRadius(bytes, circle));
    }

    @Override
    public GeoResults<GeoLocation<byte[]>> geoRadius(byte[] bytes, Circle circle,
                                                     GeoRadiusCommandArgs geoRadiusCommandArgs) {
        return actionWrapper.doInScope(TairAndRedisCommand.GEORADIUS, bytes, () -> jedisClusterConnection.geoRadius(bytes, circle, geoRadiusCommandArgs));
    }

    @Override
    public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member,
                                                             double radius) {
        return actionWrapper.doInScope(TairAndRedisCommand.GEORADIUSBYMEMBER, key, () -> jedisClusterConnection.geoRadiusByMember(key, member, radius));
    }

    @Override
    public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] bytes, byte[] bytes1,
                                                             Distance distance) {
        return actionWrapper.doInScope(TairAndRedisCommand.GEORADIUSBYMEMBER, bytes, () -> jedisClusterConnection.geoRadiusByMember(bytes, bytes1, distance));
    }

    @Override
    public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] bytes,
                                                             byte[] bytes1,
                                                             Distance distance,
                                                             GeoRadiusCommandArgs geoRadiusCommandArgs) {
        return actionWrapper.doInScope(TairAndRedisCommand.GEORADIUSBYMEMBER, bytes, () -> jedisClusterConnection.geoRadiusByMember(bytes, bytes1, distance, geoRadiusCommandArgs));
    }

    @Override
    public Long geoRemove(byte[] bytes, byte[]... bytes1) {
        return actionWrapper.doInScope(TairAndRedisCommand.GEOREMOVE, bytes, () -> jedisClusterConnection.geoRemove(bytes, bytes1));

    }

    @Override
    public Boolean hSet(byte[] bytes, byte[] bytes1, byte[] bytes2) {
        return actionWrapper.doInScope(TairAndRedisCommand.HSET, bytes, () -> jedisClusterConnection.hSet(bytes, bytes1, bytes2));
    }

    @Override
    public Boolean hSetNX(byte[] bytes, byte[] bytes1, byte[] bytes2) {
        return actionWrapper.doInScope(TairAndRedisCommand.HSETNX, bytes, () -> jedisClusterConnection.hSetNX(bytes, bytes1, bytes2));

    }

    @Override
    public byte[] hGet(byte[] bytes, byte[] bytes1) {
        return actionWrapper.doInScope(TairAndRedisCommand.HGET, bytes, () -> jedisClusterConnection.hGet(bytes, bytes1));
    }

    @Override
    public List<byte[]> hMGet(byte[] bytes, byte[]... bytes1) {
        return actionWrapper.doInScope(TairAndRedisCommand.HMGET, bytes, () -> jedisClusterConnection.hMGet(bytes, bytes1));

    }

    @Override
    public void hMSet(byte[] bytes, Map<byte[], byte[]> map) {
        actionWrapper.doInScope(TairAndRedisCommand.HMGET, bytes, () -> jedisClusterConnection.hMSet(bytes, map));
    }

    @Override
    public Long hIncrBy(byte[] bytes, byte[] bytes1, long l) {
        return actionWrapper.doInScope(TairAndRedisCommand.HINCRBY, bytes, () -> jedisClusterConnection.hIncrBy(bytes, bytes1, l));

    }

    @Override
    public Double hIncrBy(byte[] bytes, byte[] bytes1, double v) {
        return actionWrapper.doInScope(TairAndRedisCommand.HINCRBY, bytes, () -> jedisClusterConnection.hIncrBy(bytes, bytes1, v));

    }

    @Override
    public Boolean hExists(byte[] key, byte[] field) {
        return actionWrapper.doInScope(TairAndRedisCommand.HEXISTS, key, () -> jedisClusterConnection.hExists(key, field));
    }

    @Override
    public Long hDel(byte[] key, byte[]... fields) {
        return actionWrapper.doInScope(TairAndRedisCommand.HDEL, key, () -> jedisClusterConnection.hDel(key, fields));
    }

    @Override
    public Long hLen(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.HLEN, key, () -> jedisClusterConnection.hLen(key));
    }

    @Override
    public Set<byte[]> hKeys(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.HKEYS, key, () -> jedisClusterConnection.hKeys(key));
    }

    @Override
    public List<byte[]> hVals(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.HVALS, key, () -> jedisClusterConnection.hVals(key));
    }

    @Override
    public Map<byte[], byte[]> hGetAll(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.HGETALL, key, () -> jedisClusterConnection.hGetAll(key));
    }

    @Override
    public Cursor<Map.Entry<byte[], byte[]>> hScan(byte[] key, ScanOptions options) {
        return actionWrapper.doInScope(TairAndRedisCommand.HSCAN, key, () -> jedisClusterConnection.hScan(key, options));
    }

    @Override
    public Long hStrLen(byte[] key, byte[] field) {
        return actionWrapper.doInScope(TairAndRedisCommand.HSTRLEN, key, () -> jedisClusterConnection.hStrLen(key, field));
    }

    @Override
    public Long pfAdd(byte[] key, byte[]... values) {
        return actionWrapper.doInScope(TairAndRedisCommand.PFADD, key, () -> jedisClusterConnection.pfAdd(key, values));
    }

    @Override
    public Long pfCount(byte[]... keys) {
        return actionWrapper.doInScope(TairAndRedisCommand.PFCOUNT, keys, () -> jedisClusterConnection.pfCount(keys));
    }

    @Override
    public void pfMerge(byte[] destinationKey, byte[]... sourceKeys) {
        actionWrapper.doInScope(TairAndRedisCommand.PFMERGE,
                () -> jedisClusterConnection.pfMerge(destinationKey, sourceKeys));
    }

    @Override
    public Long exists(byte[]... keys) {
        return actionWrapper.doInScope(TairAndRedisCommand.EXISTS, keys, jedisClusterConnection::exists);
    }

    @Override
    public Long del(byte[]... keys) {
        return actionWrapper.doInScope(TairAndRedisCommand.DEL, keys, () -> jedisClusterConnection.del(keys));
    }

    @Override
    public Long unlink(byte[]... keys) {
        return actionWrapper.doInScope(TairAndRedisCommand.UNLINK, keys, jedisClusterConnection::unlink);
    }

    @Override
    public DataType type(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.TYPE, key, () -> jedisClusterConnection.type(key));
    }

    @Override
    public Long touch(byte[]... keys) {
        return actionWrapper.doInScope(TairAndRedisCommand.TOUCH, keys, jedisClusterConnection::touch);
    }

    @Override
    public Set<byte[]> keys(byte[] pattern) {
        return actionWrapper.doInScope(TairAndRedisCommand.KEYS, () -> jedisClusterConnection.keys(pattern));
    }

    @Override
    public Cursor<byte[]> scan(ScanOptions options) {
        return actionWrapper.doInScope(TairAndRedisCommand.SCAN, () -> jedisClusterConnection.scan(options));
    }

    @Override
    public byte[] randomKey() {
        return actionWrapper.doInScope(TairAndRedisCommand.RANDOMKEY, () -> jedisClusterConnection.randomKey());
    }

    @Override
    public void rename(byte[] sourceKey, byte[] targetKey) {
        actionWrapper.doInScope(TairAndRedisCommand.RENAME, sourceKey, () -> jedisClusterConnection.rename(sourceKey, targetKey));
    }

    @Override
    public Boolean renameNX(byte[] sourceKey, byte[] targetKey) {
        return actionWrapper.doInScope(TairAndRedisCommand.RENAMENX, sourceKey,
                () -> jedisClusterConnection.renameNX(sourceKey, targetKey));
    }

    @Override
    public Boolean expire(byte[] key, long seconds) {
        return actionWrapper.doInScope(TairAndRedisCommand.EXPIRE, key, () -> jedisClusterConnection.expire(key, seconds));
    }

    @Override
    public Boolean pExpire(byte[] key, long millis) {
        return actionWrapper.doInScope(TairAndRedisCommand.PEXPIRE, key, () -> jedisClusterConnection.pExpire(key, millis));
    }

    @Override
    public Boolean expireAt(byte[] key, long unixTime) {
        return actionWrapper.doInScope(TairAndRedisCommand.EXPIREAT, key, () -> jedisClusterConnection.expireAt(key, unixTime));
    }

    @Override
    public Boolean pExpireAt(byte[] key, long unixTimeInMillis) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.PEXPIREAT, key, () -> jedisClusterConnection.pExpireAt(key, unixTimeInMillis));
    }

    @Override
    public Boolean persist(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.PERSIST, key, () -> jedisClusterConnection.persist(key));
    }

    @Override
    public Boolean move(byte[] key, int dbIndex) {
        return actionWrapper.doInScope(TairAndRedisCommand.MOVE, key, () -> jedisClusterConnection.move(key, dbIndex));
    }

    @Override
    public Long ttl(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.TTL, key, () -> jedisClusterConnection.ttl(key));
    }

    @Override
    public Long ttl(byte[] key, TimeUnit timeUnit) {
        return actionWrapper.doInScope(TairAndRedisCommand.TTL, key, () -> jedisClusterConnection.ttl(key, timeUnit));
    }

    @Override
    public Long pTtl(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.PTTL, key, () -> jedisClusterConnection.pTtl(key));
    }

    @Override
    public Long pTtl(byte[] key, TimeUnit timeUnit) {
        return actionWrapper.doInScope(TairAndRedisCommand.PTTL, key, () -> jedisClusterConnection.pTtl(key, timeUnit));
    }

    @Override
    public List<byte[]> sort(byte[] key, SortParameters params) {
        return actionWrapper.doInScope(TairAndRedisCommand.SORT, key, () -> jedisClusterConnection.sort(key, params));
    }

    @Override
    public Long sort(byte[] key, SortParameters params, byte[] storeKey) {
        return actionWrapper.doInScope(TairAndRedisCommand.SORT, key, () -> jedisClusterConnection.sort(key, params, storeKey));
    }

    @Override
    public byte[] dump(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.DUMP, key, () -> jedisClusterConnection.dump(key));
    }

    @Override
    public void restore(byte[] key, long ttlInMillis, byte[] serializedValue) {
        actionWrapper.doInScope(TairAndRedisCommand.RESTORE, key,
                () -> jedisClusterConnection.restore(key, ttlInMillis, serializedValue));
    }

    @Override
    public void restore(byte[] key, long ttlInMillis, byte[] serializedValue, boolean replace) {
        actionWrapper.doInScope(TairAndRedisCommand.RESTORE, key,
                () -> jedisClusterConnection.restore(key, ttlInMillis, serializedValue, replace));
    }

    @Override
    public ValueEncoding encodingOf(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.ENCODING, key, () -> jedisClusterConnection.encodingOf(key));
    }

    @Override
    public Duration idletime(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.IDLETIME, key, () -> jedisClusterConnection.idletime(key));
    }

    @Override
    public Long refcount(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.REFCOUNT, key, () -> jedisClusterConnection.refcount(key));
    }

    @Override
    public Long rPush(byte[] key, byte[]... values) {
        return actionWrapper.doInScope(TairAndRedisCommand.RPUSH, key, () -> jedisClusterConnection.rPush(key, values));
    }

    @Override
    public Long lPush(byte[] key, byte[]... values) {
        return actionWrapper.doInScope(TairAndRedisCommand.LPUSH, key, () -> jedisClusterConnection.lPush(key, values));
    }

    @Override
    public Long rPushX(byte[] key, byte[] value) {
        return actionWrapper.doInScope(TairAndRedisCommand.RPUSHX, key, () -> jedisClusterConnection.rPushX(key, value));
    }

    @Override
    public Long lPushX(byte[] key, byte[] value) {
        return actionWrapper.doInScope(TairAndRedisCommand.LPUSHX, key, () -> jedisClusterConnection.lPushX(key, value));
    }

    @Override
    public Long lLen(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.LLEN, key, () -> jedisClusterConnection.lLen(key));
    }

    @Override
    public List<byte[]> lRange(byte[] key, long start, long end) {
        return actionWrapper.doInScope(TairAndRedisCommand.LRANGE, key, () -> jedisClusterConnection.lRange(key, start, end));
    }

    @Override
    public void lTrim(byte[] key, long start, long end) {
        actionWrapper.doInScope(TairAndRedisCommand.LTRIM, key, () -> jedisClusterConnection.lTrim(key, start, end));
    }

    @Override
    public byte[] lIndex(byte[] key, long index) {
        return actionWrapper.doInScope(TairAndRedisCommand.LINDEX, key, () -> jedisClusterConnection.lIndex(key, index));
    }

    @Override
    public Long lInsert(byte[] key, Position where, byte[] pivot, byte[] value) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.LINSERT, key, () -> jedisClusterConnection.lInsert(key, where, pivot, value));
    }

    @Override
    public void lSet(byte[] key, long index, byte[] value) {
        actionWrapper.doInScope(TairAndRedisCommand.LSET, key, () -> jedisClusterConnection.lSet(key, index, value));
    }

    @Override
    public Long lRem(byte[] key, long count, byte[] value) {
        return actionWrapper.doInScope(TairAndRedisCommand.LREM, key, () -> jedisClusterConnection.lRem(key, count, value));
    }

    @Override
    public byte[] lPop(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.LPOP, key, () -> jedisClusterConnection.lPop(key));
    }

    @Override
    public byte[] rPop(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.RPOP, key, () -> jedisClusterConnection.rPop(key));
    }

    @Override
    public List<byte[]> bLPop(int timeout, byte[]... keys) {
        return actionWrapper.doInScope(TairAndRedisCommand.BLPOP, keys, () -> jedisClusterConnection.bLPop(timeout, keys));
    }

    @Override
    public List<byte[]> bRPop(int timeout, byte[]... keys) {
        return actionWrapper.doInScope(TairAndRedisCommand.BRPOP, keys, () -> jedisClusterConnection.bRPop(timeout, keys));
    }

    @Override
    public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.RPOPLPUSH, srcKey, () -> jedisClusterConnection.rPopLPush(srcKey, dstKey));
    }

    @Override
    public byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey) {
        return actionWrapper.doInScope(TairAndRedisCommand.BRPOPLPUSH, srcKey,
                () -> jedisClusterConnection.bRPopLPush(timeout, srcKey, dstKey));
    }

    @Override
    public boolean isSubscribed() {
        return jedisClusterConnection.isSubscribed();
    }

    @Override
    public Subscription getSubscription() {
        return jedisClusterConnection.getSubscription();
    }

    @Override
    public Long publish(byte[] channel, byte[] message) {
        return actionWrapper.doInScope(TairAndRedisCommand.PUBLISH, () -> jedisClusterConnection.publish(channel, message));
    }

    @Override
    public void subscribe(MessageListener listener, byte[]... channels) {
        actionWrapper.doInScope(TairAndRedisCommand.SUBSCRIBE, () -> jedisClusterConnection.subscribe(listener, channels));
    }

    @Override
    public void pSubscribe(MessageListener listener, byte[]... patterns) {
        actionWrapper.doInScope(TairAndRedisCommand.PSUBSCRIBE, () -> jedisClusterConnection.pSubscribe(listener, patterns));
    }

    @Override
    public void scriptFlush() {
        actionWrapper.doInScope(TairAndRedisCommand.SCRIPT_FLUSH, () -> jedisClusterConnection.scriptFlush());
    }

    @Override
    public void scriptKill() {
        actionWrapper.doInScope(TairAndRedisCommand.SCRIPT_KILL, () -> jedisClusterConnection.scriptKill());
    }

    @Override
    public String scriptLoad(byte[] script) {
        return actionWrapper.doInScope(TairAndRedisCommand.SCRIPT_LOAD, () -> jedisClusterConnection.scriptLoad(script));
    }

    @Override
    public List<Boolean> scriptExists(String... scriptShas) {
        return actionWrapper.doInScope(TairAndRedisCommand.SCRIPT_EXISTS, () -> jedisClusterConnection.scriptExists(scriptShas));
    }

    @Override
    public <T> T eval(byte[] script, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
        return actionWrapper.doInScope(TairAndRedisCommand.EVAL,
                () -> jedisClusterConnection.eval(script, returnType, numKeys, keysAndArgs));
    }

    @Override
    public <T> T evalSha(String scriptSha, ReturnType returnType, int numKeys,
                         byte[]... keysAndArgs) {
        return actionWrapper.doInScope(TairAndRedisCommand.EVALSHA,
                () -> jedisClusterConnection.evalSha(scriptSha, returnType, numKeys, keysAndArgs));
    }

    @Override
    public <T> T evalSha(byte[] scriptSha, ReturnType returnType, int numKeys,
                         byte[]... keysAndArgs) {
        return actionWrapper.doInScope(TairAndRedisCommand.EVALSHA,
                () -> jedisClusterConnection.evalSha(scriptSha, returnType, numKeys, keysAndArgs));
    }

    @Override
    public void bgWriteAof() {
        actionWrapper.doInScope(TairAndRedisCommand.BGWRITEAOF, () -> jedisClusterConnection.bgWriteAof());
    }

    @Override
    public void bgReWriteAof() {
        actionWrapper.doInScope(TairAndRedisCommand.BGREWRITEAOF, () -> jedisClusterConnection.bgReWriteAof());
    }

    @Override
    public void bgSave() {
        actionWrapper.doInScope(TairAndRedisCommand.BGSAVE, () -> jedisClusterConnection.bgSave());
    }

    @Override
    public Long lastSave() {
        return actionWrapper.doInScope(TairAndRedisCommand.LASTSAVE, () -> jedisClusterConnection.lastSave());
    }

    @Override
    public void save() {
        actionWrapper.doInScope(TairAndRedisCommand.SAVE, () -> jedisClusterConnection.save());
    }

    @Override
    public Long dbSize() {
        return actionWrapper.doInScope(TairAndRedisCommand.DBSIZE, () -> jedisClusterConnection.dbSize());
    }

    @Override
    public void flushDb() {
        actionWrapper.doInScope(TairAndRedisCommand.FLUSHDB, () -> jedisClusterConnection.flushDb());
    }

    @Override
    public void flushAll() {
        actionWrapper.doInScope(TairAndRedisCommand.FLUSHALL, () -> jedisClusterConnection.flushAll());
    }

    @Override
    public Properties info() {
        return actionWrapper.doInScope(TairAndRedisCommand.INFO, () -> jedisClusterConnection.info());
    }

    @Override
    public Properties info(String section) {
        return actionWrapper.doInScope(TairAndRedisCommand.INFO, () -> jedisClusterConnection.info(section));
    }

    @Override
    public void shutdown() {
        actionWrapper.doInScope(TairAndRedisCommand.SHUTDOWN, () -> jedisClusterConnection.shutdown());
    }

    @Override
    public void shutdown(ShutdownOption option) {
        actionWrapper.doInScope(TairAndRedisCommand.SHUTDOWN, () -> jedisClusterConnection.shutdown(option));
    }

    @Override
    public Properties getConfig(String pattern) {
        return actionWrapper.doInScope(TairAndRedisCommand.CONFIG_GET, () -> jedisClusterConnection.getConfig(pattern));
    }

    @Override
    public void setConfig(String param, String value) {
        actionWrapper.doInScope(TairAndRedisCommand.CONFIG_SET, () -> jedisClusterConnection.setConfig(param, value));
    }

    @Override
    public void resetConfigStats() {
        actionWrapper.doInScope(TairAndRedisCommand.CONFIG_RESETSTAT, () -> jedisClusterConnection.resetConfigStats());
    }

    @Override
    public Long time() {
        return actionWrapper.doInScope(TairAndRedisCommand.TIME, () -> jedisClusterConnection.time());
    }

    @Override
    public void killClient(String host, int port) {
        actionWrapper.doInScope(TairAndRedisCommand.CLIENT_KILL, () -> jedisClusterConnection.killClient(host, port));
    }

    @Override
    public void setClientName(byte[] name) {
        actionWrapper.doInScope(TairAndRedisCommand.CLIENT_SETNAME, () -> jedisClusterConnection.setClientName(name));
    }

    @Override
    public String getClientName() {
        return actionWrapper.doInScope(TairAndRedisCommand.CLIENT_GETNAME, () -> jedisClusterConnection.getClientName());
    }

    @Override
    public List<RedisClientInfo> getClientList() {
        return actionWrapper.doInScope(TairAndRedisCommand.CLIENT_LIST, () -> jedisClusterConnection.getClientList());
    }

    @Override
    public void slaveOf(String host, int port) {
        actionWrapper.doInScope(TairAndRedisCommand.SLAVEOF, () -> jedisClusterConnection.slaveOf(host, port));
    }

    @Override
    public void slaveOfNoOne() {
        actionWrapper.doInScope(TairAndRedisCommand.SLAVEOFNOONE, () -> jedisClusterConnection.slaveOfNoOne());
    }

    @Override
    public void migrate(byte[] key, RedisNode target, int dbIndex, MigrateOption option) {
        actionWrapper.doInScope(TairAndRedisCommand.MIGRATE, key,
                () -> jedisClusterConnection.migrate(key, target, dbIndex, option));
    }

    @Override
    public void migrate(byte[] key, RedisNode target, int dbIndex, MigrateOption option,
                        long timeout) {
        actionWrapper.doInScope(TairAndRedisCommand.MIGRATE,
                key, () -> jedisClusterConnection.migrate(key, target, dbIndex, option, timeout));
    }

    @Override
    public Long sAdd(byte[] key, byte[]... values) {
        return actionWrapper.doInScope(TairAndRedisCommand.SADD, key, () -> jedisClusterConnection.sAdd(key, values));
    }

    @Override
    public Long sRem(byte[] key, byte[]... values) {
        return actionWrapper.doInScope(TairAndRedisCommand.SREM, key, () -> jedisClusterConnection.sRem(key, values));
    }

    @Override
    public byte[] sPop(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.SPOP, key, () -> jedisClusterConnection.sPop(key));
    }

    @Override
    public List<byte[]> sPop(byte[] key, long count) {
        return actionWrapper.doInScope(TairAndRedisCommand.SPOP, key, () -> jedisClusterConnection.sPop(key, count));
    }

    @Override
    public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.SMOVE, srcKey, () -> jedisClusterConnection.sMove(srcKey, destKey, value));
    }

    @Override
    public Long sCard(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.SCARD, key, () -> jedisClusterConnection.sCard(key));
    }

    @Override
    public Boolean sIsMember(byte[] key, byte[] value) {
        return actionWrapper.doInScope(TairAndRedisCommand.SISMEMBER, key, () -> jedisClusterConnection.sIsMember(key, value));
    }

    @Override
    public Set<byte[]> sInter(byte[]... keys) {
        return actionWrapper.doInScope(TairAndRedisCommand.SINTER, keys, () -> jedisClusterConnection.sInter(keys));
    }

    @Override
    public Long sInterStore(byte[] destKey, byte[]... keys) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.SINTERSTORE, keys, () -> jedisClusterConnection.sInterStore(destKey, keys));
    }

    @Override
    public Set<byte[]> sUnion(byte[]... keys) {
        return actionWrapper.doInScope(TairAndRedisCommand.SUNION, keys, () -> jedisClusterConnection.sUnion(keys));
    }

    @Override
    public Long sUnionStore(byte[] destKey, byte[]... keys) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.SUNIONSTORE, keys, () -> jedisClusterConnection.sUnionStore(destKey, keys));
    }

    @Override
    public Set<byte[]> sDiff(byte[]... keys) {
        return actionWrapper.doInScope(TairAndRedisCommand.SDIFF, keys, () -> jedisClusterConnection.sDiff(keys));
    }

    @Override
    public Long sDiffStore(byte[] destKey, byte[]... keys) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.SDIFFSTORE, keys, () -> jedisClusterConnection.sDiffStore(destKey, keys));
    }

    @Override
    public Set<byte[]> sMembers(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.SMEMBERS, key, () -> jedisClusterConnection.sMembers(key));
    }

    @Override
    public byte[] sRandMember(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.SRANDMEMBER, key, () -> jedisClusterConnection.sRandMember(key));
    }

    @Override
    public List<byte[]> sRandMember(byte[] key, long count) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.SRANDMEMBER, key, () -> jedisClusterConnection.sRandMember(key, count));
    }

    @Override
    public Cursor<byte[]> sScan(byte[] key, ScanOptions options) {
        return actionWrapper.doInScope(TairAndRedisCommand.SSCAN, key, () -> jedisClusterConnection.sScan(key, options));
    }

    @Override
    public byte[] get(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.GET, key, () -> jedisClusterConnection.get(key));
    }

    @Override
    public byte[] getSet(byte[] key, byte[] value) {
        return actionWrapper.doInScope(TairAndRedisCommand.GETSET, key, () -> jedisClusterConnection.getSet(key, value));
    }

    @Override
    public List<byte[]> mGet(byte[]... keys) {
        return actionWrapper.doInScope(TairAndRedisCommand.MGET, keys, () -> jedisClusterConnection.mGet(keys));
    }

    @Override
    public Boolean set(byte[] key, byte[] value) {
        return actionWrapper.doInScope(TairAndRedisCommand.SET, key, () -> jedisClusterConnection.set(key, value));
    }

    @Override
    public Boolean set(byte[] key, byte[] value, Expiration expiration, SetOption option) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.SET, key, () -> jedisClusterConnection.set(key, value, expiration, option));
    }

    @Override
    public Boolean setNX(byte[] key, byte[] value) {
        return actionWrapper.doInScope(TairAndRedisCommand.SETNX, key, () -> jedisClusterConnection.setNX(key, value));
    }

    @Override
    public Boolean setEx(byte[] key, long seconds, byte[] value) {
        return actionWrapper.doInScope(TairAndRedisCommand.SETEX, key, () -> jedisClusterConnection.setEx(key, seconds, value));
    }

    @Override
    public Boolean pSetEx(byte[] key, long milliseconds, byte[] value) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.PSETEX, key, () -> jedisClusterConnection.pSetEx(key, milliseconds, value));
    }

    @Override
    public Boolean mSet(Map<byte[], byte[]> tuple) {
        return actionWrapper.doInScope(TairAndRedisCommand.MSET, () -> jedisClusterConnection.mSet(tuple));
    }

    @Override
    public Boolean mSetNX(Map<byte[], byte[]> tuple) {
        return actionWrapper.doInScope(TairAndRedisCommand.MSETNX, () -> jedisClusterConnection.mSetNX(tuple));
    }

    @Override
    public Long incr(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.INCR, key, () -> jedisClusterConnection.incr(key));
    }

    @Override
    public Long incrBy(byte[] key, long value) {
        return actionWrapper.doInScope(TairAndRedisCommand.INCRBY, key, () -> jedisClusterConnection.incrBy(key, value));
    }

    @Override
    public Double incrBy(byte[] key, double value) {
        return actionWrapper.doInScope(TairAndRedisCommand.INCRBY, key, () -> jedisClusterConnection.incrBy(key, value));
    }

    @Override
    public Long decr(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.DECR, key, () -> jedisClusterConnection.decr(key));
    }

    @Override
    public Long decrBy(byte[] key, long value) {
        return actionWrapper.doInScope(TairAndRedisCommand.DECRBY, key, () -> jedisClusterConnection.decrBy(key, value));
    }

    @Override
    public Long append(byte[] key, byte[] value) {
        return actionWrapper.doInScope(TairAndRedisCommand.APPEND, key, () -> jedisClusterConnection.append(key, value));
    }

    @Override
    public byte[] getRange(byte[] key, long begin, long end) {
        return actionWrapper.doInScope(TairAndRedisCommand.GETRANGE, key, () -> jedisClusterConnection.getRange(key, begin, end));
    }

    @Override
    public void setRange(byte[] key, byte[] value, long offset) {
        actionWrapper.doInScope(TairAndRedisCommand.SETRANGE, key, () -> jedisClusterConnection.setRange(key, value, offset));
    }

    @Override
    public Boolean getBit(byte[] key, long offset) {
        return actionWrapper.doInScope(TairAndRedisCommand.GETBIT, key, () -> jedisClusterConnection.getBit(key, offset));
    }

    @Override
    public Boolean setBit(byte[] key, long offset, boolean value) {
        return actionWrapper.doInScope(TairAndRedisCommand.SETBIT, key, () -> jedisClusterConnection.setBit(key, offset, value));
    }

    @Override
    public Long bitCount(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.BITCOUNT, key, () -> jedisClusterConnection.bitCount(key));
    }

    @Override
    public Long bitCount(byte[] key, long begin, long end) {
        return actionWrapper.doInScope(TairAndRedisCommand.BITCOUNT, key, () -> jedisClusterConnection.bitCount(key, begin, end));
    }

    @Override
    public List<Long> bitField(byte[] key, BitFieldSubCommands subCommands) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.BITFIELD, key, () -> jedisClusterConnection.bitField(key, subCommands));
    }

    @Override
    public Long bitOp(BitOperation op, byte[] destination, byte[]... keys) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.BITOP, keys, () -> jedisClusterConnection.bitOp(op, destination, keys));
    }

    @Override
    public Long bitPos(byte[] key, boolean bit, org.springframework.data.domain.Range<Long> range) {
        return actionWrapper.doInScope(TairAndRedisCommand.BITPOS, key, () -> jedisClusterConnection.bitPos(key, bit, range));
    }

    @Override
    public Long strLen(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.STRLEN, key, () -> jedisClusterConnection.strLen(key));
    }

    @Override
    public void multi() {
        actionWrapper.doInScope(TairAndRedisCommand.MULTI, () -> jedisClusterConnection.multi());
    }

    @Override
    public List<Object> exec() {
        return actionWrapper.doInScope(TairAndRedisCommand.EXEC, () -> jedisClusterConnection.exec());
    }

    @Override
    public void discard() {
        actionWrapper.doInScope(TairAndRedisCommand.DISCARD, () -> jedisClusterConnection.discard());
    }

    @Override
    public void watch(byte[]... keys) {
        actionWrapper.doInScope(TairAndRedisCommand.WATCH, () -> jedisClusterConnection.watch(keys));
    }

    @Override
    public void unwatch() {
        actionWrapper.doInScope(TairAndRedisCommand.UNWATCH, () -> jedisClusterConnection.unwatch());
    }

    @Override
    public Boolean zAdd(byte[] key, double score, byte[] value) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZADD, key, () -> jedisClusterConnection.zAdd(key, score, value));
    }

    @Override
    public Long zAdd(byte[] key, Set<Tuple> tuples) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZADD, key, () -> jedisClusterConnection.zAdd(key, tuples));
    }

    @Override
    public Long zRem(byte[] key, byte[]... values) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZREM, key, () -> jedisClusterConnection.zRem(key, values));
    }

    @Override
    public Double zIncrBy(byte[] key, double increment, byte[] value) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.ZINCRBY, key, () -> jedisClusterConnection.zIncrBy(key, increment, value));
    }

    @Override
    public Long zRank(byte[] key, byte[] value) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZRANK, key, () -> jedisClusterConnection.zRank(key, value));
    }

    @Override
    public Long zRevRank(byte[] key, byte[] value) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZREVRANK, key, () -> jedisClusterConnection.zRevRank(key, value));
    }

    @Override
    public Set<byte[]> zRange(byte[] key, long start, long end) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZRANGE, key, () -> jedisClusterConnection.zRange(key, start, end));
    }

    @Override
    public Set<byte[]> zRevRange(byte[] key, long start, long end) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.ZREVRANGE, key, () -> jedisClusterConnection.zRevRange(key, start, end));
    }

    @Override
    public Set<Tuple> zRevRangeWithScores(byte[] key, long start, long end) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZREVRANGE_WITHSCORES,
                key, () -> jedisClusterConnection.zRevRangeWithScores(key, start, end));
    }

    @Override
    public Set<Tuple> zRangeWithScores(byte[] key, long start, long end) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZRANGE_WITHSCORES,
                key, () -> jedisClusterConnection.zRangeWithScores(key, start, end));
    }

    @Override
    public Set<byte[]> zRangeByScore(byte[] key, double min, double max) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.ZRANGEBYSCORE, key, () -> jedisClusterConnection.zRangeByScore(key, min, max));
    }

    @Override
    public Set<byte[]> zRangeByScore(byte[] key, double min, double max, long offset, long count) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZRANGEBYSCORE,
                key, () -> jedisClusterConnection.zRangeByScore(key, min, max, offset, count));
    }

    @Override
    public Set<byte[]> zRangeByScore(byte[] key, String min, String max) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.ZRANGEBYSCORE, key, () -> jedisClusterConnection.zRangeByScore(key, min, max));
    }

    @Override
    public Set<byte[]> zRangeByScore(byte[] key, Range range) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.ZRANGEBYSCORE, key, () -> jedisClusterConnection.zRangeByScore(key, range));
    }

    @Override
    public Set<byte[]> zRangeByScore(byte[] key, String min, String max, long offset, long count) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZRANGEBYSCORE,
                key, () -> jedisClusterConnection.zRangeByScore(key, min, max, offset, count));
    }

    @Override
    public Set<byte[]> zRangeByScore(byte[] key, Range range, Limit limit) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZRANGEBYSCORE, key,
                () -> jedisClusterConnection.zRangeByScore(key, range, limit));
    }

    @Override
    public Set<Tuple> zRangeByScoreWithScores(byte[] key, Range range) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZRANGEBYSCORE_WITHSCORES,
                key, () -> jedisClusterConnection.zRangeByScoreWithScores(key, range));
    }

    @Override
    public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZRANGEBYSCORE_WITHSCORES,
                key, () -> jedisClusterConnection.zRangeByScoreWithScores(key, min, max));
    }

    @Override
    public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max, long offset,
                                              long count) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZRANGEBYSCORE_WITHSCORES,
                key, () -> jedisClusterConnection.zRangeByScoreWithScores(key, min, max, offset, count));
    }

    @Override
    public Set<Tuple> zRangeByScoreWithScores(byte[] key, Range range, Limit limit) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZRANGEBYSCORE_WITHSCORES,
                key, () -> jedisClusterConnection.zRangeByScoreWithScores(key, range, limit));
    }

    @Override
    public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZREVRANGEBYSCORE,
                key, () -> jedisClusterConnection.zRevRangeByScore(key, min, max));
    }

    @Override
    public Set<byte[]> zRevRangeByScore(byte[] key, Range range) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZREVRANGEBYSCORE, key,
                () -> jedisClusterConnection.zRevRangeByScore(key, range));
    }

    @Override
    public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max, long offset, long count) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZREVRANGEBYSCORE,
                key, () -> jedisClusterConnection.zRevRangeByScore(key, min, max, offset, count));
    }

    @Override
    public Set<byte[]> zRevRangeByScore(byte[] key, Range range, Limit limit) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZREVRANGEBYSCORE,
                key, () -> jedisClusterConnection.zRevRangeByScore(key, range, limit));
    }

    @Override
    public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZREVRANGEBYSCORE_WITHSCORES,
                key, () -> jedisClusterConnection.zRevRangeByScoreWithScores(key, min, max));
    }

    @Override
    public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max, long offset,
                                                 long count) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZREVRANGEBYSCORE_WITHSCORES,
                key, () -> jedisClusterConnection.zRevRangeByScoreWithScores(key, min, max, offset, count));
    }

    @Override
    public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, Range range) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZREVRANGEBYSCORE_WITHSCORES,
                key, () -> jedisClusterConnection.zRevRangeByScoreWithScores(key, range));
    }

    @Override
    public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, Range range, Limit limit) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZREVRANGEBYSCORE_WITHSCORES,
                key, () -> jedisClusterConnection.zRevRangeByScoreWithScores(key, range, limit));
    }

    @Override
    public Long zCount(byte[] key, double min, double max) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZCOUNT, key, () -> jedisClusterConnection.zCount(key, min, max));
    }

    @Override
    public Long zCount(byte[] key, Range range) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZCOUNT, key, () -> jedisClusterConnection.zCount(key, range));
    }

    @Override
    public Long zCard(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZCARD, key, () -> jedisClusterConnection.zCard(key));
    }

    @Override
    public Double zScore(byte[] key, byte[] value) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZSCORE, key, () -> jedisClusterConnection.zScore(key, value));
    }

    @Override
    public Long zRemRange(byte[] key, long start, long end) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.ZREMRANGE, key, () -> jedisClusterConnection.zRemRange(key, start, end));
    }

    @Override
    public Long zRemRangeByScore(byte[] key, double min, double max) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZREMRANGEBYSCORE,
                key, () -> jedisClusterConnection.zRemRangeByScore(key, min, max));
    }

    @Override
    public Long zRemRangeByScore(byte[] key, Range range) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZREMRANGEBYSCORE, key,
                () -> jedisClusterConnection.zRemRangeByScore(key, range));
    }

    @Override
    public Long zUnionStore(byte[] destKey, byte[]... sets) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZUNIONSTORE, () -> jedisClusterConnection.zUnionStore(destKey, sets));
    }

    @Override
    public Long zUnionStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZUNIONSTORE,
                destKey, () -> jedisClusterConnection.zUnionStore(destKey, aggregate, weights, sets));
    }

    @Override
    public Long zUnionStore(byte[] destKey, Aggregate aggregate, Weights weights, byte[]... sets) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZUNIONSTORE,
                destKey, () -> jedisClusterConnection.zUnionStore(destKey, aggregate, weights, sets));
    }

    @Override
    public Long zInterStore(byte[] destKey, byte[]... sets) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.ZINTERSTORE, destKey, () -> jedisClusterConnection.zInterStore(destKey, sets));
    }

    @Override
    public Long zInterStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZINTERSTORE,
                destKey, () -> jedisClusterConnection.zInterStore(destKey, aggregate, weights, sets));
    }

    @Override
    public Long zInterStore(byte[] destKey, Aggregate aggregate, Weights weights, byte[]... sets) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZINTERSTORE,
                destKey, () -> jedisClusterConnection.zInterStore(destKey, aggregate, weights, sets));
    }

    @Override
    public Cursor<Tuple> zScan(byte[] key, ScanOptions options) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZSCAN, key, () -> jedisClusterConnection.zScan(key, options));
    }

    @Override
    public Set<byte[]> zRangeByLex(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZRANGEBYLEX, key, () -> jedisClusterConnection.zRangeByLex(key));
    }

    @Override
    public Set<byte[]> zRangeByLex(byte[] key, Range range) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.ZRANGEBYLEX, key, () -> jedisClusterConnection.zRangeByLex(key, range));
    }

    @Override
    public Set<byte[]> zRangeByLex(byte[] key, Range range, Limit limit) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.ZRANGEBYLEX, key, () -> jedisClusterConnection.zRangeByLex(key, range, limit));
    }
}
