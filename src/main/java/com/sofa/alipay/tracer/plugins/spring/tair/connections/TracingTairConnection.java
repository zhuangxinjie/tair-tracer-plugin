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

import com.aliyun.tair.springdata.extend.commands.TairConnection;
import com.aliyun.tair.springdata.extend.tairstring.TairStringCommands;
import com.aliyun.tair.tairstring.params.CasParams;
import com.aliyun.tair.tairstring.params.ExincrbyFloatParams;
import com.aliyun.tair.tairstring.params.ExincrbyParams;
import com.aliyun.tair.tairstring.params.ExsetParams;
import com.aliyun.tair.tairstring.results.ExcasResult;
import com.aliyun.tair.tairstring.results.ExgetResult;
import com.sofa.alipay.tracer.plugins.spring.tair.common.TairActionWrapperHelper;
import com.sofa.alipay.tracer.plugins.spring.tair.common.TairAndRedisCommand;
import org.springframework.dao.DataAccessException;
import org.springframework.data.geo.*;
import org.springframework.data.redis.connection.*;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.core.types.RedisClientInfo;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName: TracingTairConnection
 * @Description: tair的集群模式包含了此方法，所以该类可能取消
 * @Author: zhuangxinjie
 * @Date: 2021/6/11 2:41 下午
 */
public class TracingTairConnection implements TairConnection {
    private final TairActionWrapperHelper actionWrapper;
    private final TairConnection          tairConnection;

    public TracingTairConnection(TairActionWrapperHelper actionWrapper,
                                 TairConnection tairConnection) {
        this.actionWrapper = actionWrapper;
        this.tairConnection = tairConnection;
    }

    @Override
    public RedisGeoCommands geoCommands() {
        return tairConnection.geoCommands();
    }

    @Override
    public RedisHashCommands hashCommands() {
        return tairConnection.hashCommands();
    }

    @Override
    public RedisHyperLogLogCommands hyperLogLogCommands() {
        return tairConnection.hyperLogLogCommands();
    }

    @Override
    public RedisKeyCommands keyCommands() {
        return tairConnection.keyCommands();
    }

    @Override
    public RedisListCommands listCommands() {
        return tairConnection.listCommands();
    }

    @Override
    public RedisSetCommands setCommands() {
        return tairConnection.setCommands();
    }

    @Override
    public RedisScriptingCommands scriptingCommands() {
        return tairConnection.scriptingCommands();
    }

    @Override
    public RedisServerCommands serverCommands() {
        return tairConnection.serverCommands();
    }

    @Override
    public RedisStringCommands stringCommands() {
        return tairConnection.stringCommands();
    }

    @Override
    public RedisZSetCommands zSetCommands() {
        return tairConnection.zSetCommands();
    }

    @Override
    public void close() throws DataAccessException {
        tairConnection.close();
    }

    @Override
    public boolean isClosed() {
        return tairConnection.isClosed();
    }

    @Override
    public Object getNativeConnection() {
        return tairConnection.getNativeConnection();
    }

    @Override
    public boolean isQueueing() {
        return tairConnection.isQueueing();
    }

    @Override
    public boolean isPipelined() {
        return tairConnection.isPipelined();
    }

    @Override
    public void openPipeline() {
        tairConnection.openPipeline();
    }

    @Override
    public List<Object> closePipeline() throws RedisPipelineException {
        return tairConnection.closePipeline();
    }

    @Override
    public RedisSentinelConnection getSentinelConnection() {
        return tairConnection.getSentinelConnection();
    }

    @Override
    public Object execute(String command, byte[]... bytes) {
        return actionWrapper.doInScope(command, () -> tairConnection.execute(command, bytes));
    }

    @Override
    public void select(int dbIndex) {
        actionWrapper.doInScope(TairAndRedisCommand.SELECT, () -> tairConnection.select(dbIndex));
    }

    @Override
    public byte[] echo(byte[] message) {
        return actionWrapper.doInScope(TairAndRedisCommand.ECHO, () -> tairConnection.echo(message));
    }

    @Override
    public String ping() {
        return actionWrapper.doInScope(TairAndRedisCommand.PING, () -> tairConnection.ping());
    }

    @Override
    public Long geoAdd(byte[] bytes, Point point, byte[] bytes1) {
        return actionWrapper.doInScope(TairAndRedisCommand.GEOADD, bytes, () -> tairConnection.geoAdd(bytes, point, bytes1));
    }

    @Override
    public Long geoAdd(byte[] key, GeoLocation<byte[]> location) {
        return actionWrapper.doInScope(TairAndRedisCommand.GEOADD, key, () -> tairConnection.geoAdd(key, location));
    }

    @Override
    public Long geoAdd(byte[] bytes, Map<byte[], Point> map) {
        return actionWrapper.doInScope(TairAndRedisCommand.GEOADD, bytes, () -> tairConnection.geoAdd(bytes, map));
    }

    @Override
    public Long geoAdd(byte[] bytes, Iterable<GeoLocation<byte[]>> iterable) {
        return actionWrapper.doInScope(TairAndRedisCommand.GEOADD, bytes, () -> tairConnection.geoAdd(bytes, iterable));

    }

    @Override
    public Distance geoDist(byte[] bytes, byte[] bytes1, byte[] bytes2) {
        return actionWrapper.doInScope(TairAndRedisCommand.GEODIST, bytes, () -> tairConnection.geoDist(bytes, bytes1, bytes2));
    }

    @Override
    public Distance geoDist(byte[] bytes, byte[] bytes1, byte[] bytes2, Metric metric) {
        return actionWrapper.doInScope(TairAndRedisCommand.GEODIST, bytes, () -> tairConnection.geoDist(bytes, bytes1, bytes2, metric));
    }

    @Override
    public List<String> geoHash(byte[] bytes, byte[]... bytes1) {
        return actionWrapper.doInScope(TairAndRedisCommand.GEOHASH, bytes, () -> tairConnection.geoHash(bytes, bytes1));
    }

    @Override
    public List<Point> geoPos(byte[] bytes, byte[]... bytes1) {
        return actionWrapper.doInScope(TairAndRedisCommand.GEOPOS, bytes, () -> tairConnection.geoPos(bytes, bytes1));
    }

    @Override
    public GeoResults<GeoLocation<byte[]>> geoRadius(byte[] bytes, Circle circle) {
        return actionWrapper.doInScope(TairAndRedisCommand.GEORADIUS, bytes, () -> tairConnection.geoRadius(bytes, circle));
    }

    @Override
    public GeoResults<GeoLocation<byte[]>> geoRadius(byte[] bytes, Circle circle,
                                                     GeoRadiusCommandArgs geoRadiusCommandArgs) {
        return actionWrapper.doInScope(TairAndRedisCommand.GEORADIUS, bytes, () -> tairConnection.geoRadius(bytes, circle, geoRadiusCommandArgs));
    }

    @Override
    public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member,
                                                             double radius) {
        return actionWrapper.doInScope(TairAndRedisCommand.GEORADIUSBYMEMBER, key, () -> tairConnection.geoRadiusByMember(key, member, radius));
    }

    @Override
    public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] bytes, byte[] bytes1,
                                                             Distance distance) {
        return actionWrapper.doInScope(TairAndRedisCommand.GEORADIUSBYMEMBER, bytes, () -> tairConnection.geoRadiusByMember(bytes, bytes1, distance));
    }

    @Override
    public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] bytes,
                                                             byte[] bytes1,
                                                             Distance distance,
                                                             GeoRadiusCommandArgs geoRadiusCommandArgs) {
        return actionWrapper.doInScope(TairAndRedisCommand.GEORADIUSBYMEMBER, bytes, () -> tairConnection.geoRadiusByMember(bytes, bytes1, distance, geoRadiusCommandArgs));
    }

    @Override
    public Long geoRemove(byte[] bytes, byte[]... bytes1) {
        return actionWrapper.doInScope(TairAndRedisCommand.GEOREMOVE, bytes, () -> tairConnection.geoRemove(bytes, bytes1));

    }

    @Override
    public Boolean hSet(byte[] bytes, byte[] bytes1, byte[] bytes2) {
        return actionWrapper.doInScope(TairAndRedisCommand.HSET, bytes, () -> tairConnection.hSet(bytes, bytes1, bytes2));
    }

    @Override
    public Boolean hSetNX(byte[] bytes, byte[] bytes1, byte[] bytes2) {
        return actionWrapper.doInScope(TairAndRedisCommand.HSETNX, bytes, () -> tairConnection.hSetNX(bytes, bytes1, bytes2));

    }

    @Override
    public byte[] hGet(byte[] bytes, byte[] bytes1) {
        return actionWrapper.doInScope(TairAndRedisCommand.HGET, bytes, () -> tairConnection.hGet(bytes, bytes1));
    }

    @Override
    public List<byte[]> hMGet(byte[] bytes, byte[]... bytes1) {
        return actionWrapper.doInScope(TairAndRedisCommand.HMGET, bytes, () -> tairConnection.hMGet(bytes, bytes1));

    }

    @Override
    public void hMSet(byte[] bytes, Map<byte[], byte[]> map) {
        actionWrapper.doInScope(TairAndRedisCommand.HMGET, bytes, () -> tairConnection.hMSet(bytes, map));
    }

    @Override
    public Long hIncrBy(byte[] bytes, byte[] bytes1, long l) {
        return actionWrapper.doInScope(TairAndRedisCommand.HINCRBY, bytes, () -> tairConnection.hIncrBy(bytes, bytes1, l));

    }

    @Override
    public Double hIncrBy(byte[] bytes, byte[] bytes1, double v) {
        return actionWrapper.doInScope(TairAndRedisCommand.HINCRBY, bytes, () -> tairConnection.hIncrBy(bytes, bytes1, v));

    }

    @Override
    public Boolean hExists(byte[] key, byte[] field) {
        return actionWrapper.doInScope(TairAndRedisCommand.HEXISTS, key, () -> tairConnection.hExists(key, field));
    }

    @Override
    public Long hDel(byte[] key, byte[]... fields) {
        return actionWrapper.doInScope(TairAndRedisCommand.HDEL, key, () -> tairConnection.hDel(key, fields));
    }

    @Override
    public Long hLen(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.HLEN, key, () -> tairConnection.hLen(key));
    }

    @Override
    public Set<byte[]> hKeys(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.HKEYS, key, () -> tairConnection.hKeys(key));
    }

    @Override
    public List<byte[]> hVals(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.HVALS, key, () -> tairConnection.hVals(key));
    }

    @Override
    public Map<byte[], byte[]> hGetAll(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.HGETALL, key, () -> tairConnection.hGetAll(key));
    }

    @Override
    public Cursor<Map.Entry<byte[], byte[]>> hScan(byte[] key, ScanOptions options) {
        return actionWrapper.doInScope(TairAndRedisCommand.HSCAN, key, () -> tairConnection.hScan(key, options));
    }

    @Override
    public Long hStrLen(byte[] key, byte[] field) {
        return actionWrapper.doInScope(TairAndRedisCommand.HSTRLEN, key, () -> tairConnection.hStrLen(key, field));
    }

    @Override
    public Long pfAdd(byte[] key, byte[]... values) {
        return actionWrapper.doInScope(TairAndRedisCommand.PFADD, key, () -> tairConnection.pfAdd(key, values));
    }

    @Override
    public Long pfCount(byte[]... keys) {
        return actionWrapper.doInScope(TairAndRedisCommand.PFCOUNT, keys, () -> tairConnection.pfCount(keys));
    }

    @Override
    public void pfMerge(byte[] destinationKey, byte[]... sourceKeys) {
        actionWrapper.doInScope(TairAndRedisCommand.PFMERGE,
                () -> tairConnection.pfMerge(destinationKey, sourceKeys));
    }

    @Override
    public Long exists(byte[]... keys) {
        return actionWrapper.doInScope(TairAndRedisCommand.EXISTS, keys, tairConnection::exists);
    }

    @Override
    public Long del(byte[]... keys) {
        return actionWrapper.doInScope(TairAndRedisCommand.DEL, keys, () -> tairConnection.del(keys));
    }

    @Override
    public Long unlink(byte[]... keys) {
        return actionWrapper.doInScope(TairAndRedisCommand.UNLINK, keys, tairConnection::unlink);
    }

    @Override
    public DataType type(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.TYPE, key, () -> tairConnection.type(key));
    }

    @Override
    public Long touch(byte[]... keys) {
        return actionWrapper.doInScope(TairAndRedisCommand.TOUCH, keys, tairConnection::touch);
    }

    @Override
    public Set<byte[]> keys(byte[] pattern) {
        return actionWrapper.doInScope(TairAndRedisCommand.KEYS, () -> tairConnection.keys(pattern));
    }

    @Override
    public Cursor<byte[]> scan(ScanOptions options) {
        return actionWrapper.doInScope(TairAndRedisCommand.SCAN, () -> tairConnection.scan(options));
    }

    @Override
    public byte[] randomKey() {
        return actionWrapper.doInScope(TairAndRedisCommand.RANDOMKEY, () -> tairConnection.randomKey());
    }

    @Override
    public void rename(byte[] sourceKey, byte[] targetKey) {
        actionWrapper.doInScope(TairAndRedisCommand.RENAME, sourceKey, () -> tairConnection.rename(sourceKey, targetKey));
    }

    @Override
    public Boolean renameNX(byte[] sourceKey, byte[] targetKey) {
        return actionWrapper.doInScope(TairAndRedisCommand.RENAMENX, sourceKey,
                () -> tairConnection.renameNX(sourceKey, targetKey));
    }

    @Override
    public Boolean expire(byte[] key, long seconds) {
        return actionWrapper.doInScope(TairAndRedisCommand.EXPIRE, key, () -> tairConnection.expire(key, seconds));
    }

    @Override
    public Boolean pExpire(byte[] key, long millis) {
        return actionWrapper.doInScope(TairAndRedisCommand.PEXPIRE, key, () -> tairConnection.pExpire(key, millis));
    }

    @Override
    public Boolean expireAt(byte[] key, long unixTime) {
        return actionWrapper.doInScope(TairAndRedisCommand.EXPIREAT, key, () -> tairConnection.expireAt(key, unixTime));
    }

    @Override
    public Boolean pExpireAt(byte[] key, long unixTimeInMillis) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.PEXPIREAT, key, () -> tairConnection.pExpireAt(key, unixTimeInMillis));
    }

    @Override
    public Boolean persist(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.PERSIST, key, () -> tairConnection.persist(key));
    }

    @Override
    public Boolean move(byte[] key, int dbIndex) {
        return actionWrapper.doInScope(TairAndRedisCommand.MOVE, key, () -> tairConnection.move(key, dbIndex));
    }

    @Override
    public Long ttl(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.TTL, key, () -> tairConnection.ttl(key));
    }

    @Override
    public Long ttl(byte[] key, TimeUnit timeUnit) {
        return actionWrapper.doInScope(TairAndRedisCommand.TTL, key, () -> tairConnection.ttl(key, timeUnit));
    }

    @Override
    public Long pTtl(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.PTTL, key, () -> tairConnection.pTtl(key));
    }

    @Override
    public Long pTtl(byte[] key, TimeUnit timeUnit) {
        return actionWrapper.doInScope(TairAndRedisCommand.PTTL, key, () -> tairConnection.pTtl(key, timeUnit));
    }

    @Override
    public List<byte[]> sort(byte[] key, SortParameters params) {
        return actionWrapper.doInScope(TairAndRedisCommand.SORT, key, () -> tairConnection.sort(key, params));
    }

    @Override
    public Long sort(byte[] key, SortParameters params, byte[] storeKey) {
        return actionWrapper.doInScope(TairAndRedisCommand.SORT, key, () -> tairConnection.sort(key, params, storeKey));
    }

    @Override
    public byte[] dump(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.DUMP, key, () -> tairConnection.dump(key));
    }

    @Override
    public void restore(byte[] key, long ttlInMillis, byte[] serializedValue) {
        actionWrapper.doInScope(TairAndRedisCommand.RESTORE, key,
                () -> tairConnection.restore(key, ttlInMillis, serializedValue));
    }

    @Override
    public void restore(byte[] key, long ttlInMillis, byte[] serializedValue, boolean replace) {
        actionWrapper.doInScope(TairAndRedisCommand.RESTORE, key,
                () -> tairConnection.restore(key, ttlInMillis, serializedValue, replace));
    }

    @Override
    public ValueEncoding encodingOf(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.ENCODING, key, () -> tairConnection.encodingOf(key));
    }

    @Override
    public Duration idletime(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.IDLETIME, key, () -> tairConnection.idletime(key));
    }

    @Override
    public Long refcount(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.REFCOUNT, key, () -> tairConnection.refcount(key));
    }

    @Override
    public Long rPush(byte[] key, byte[]... values) {
        return actionWrapper.doInScope(TairAndRedisCommand.RPUSH, key, () -> tairConnection.rPush(key, values));
    }

    @Override
    public Long lPush(byte[] key, byte[]... values) {
        return actionWrapper.doInScope(TairAndRedisCommand.LPUSH, key, () -> tairConnection.lPush(key, values));
    }

    @Override
    public Long rPushX(byte[] key, byte[] value) {
        return actionWrapper.doInScope(TairAndRedisCommand.RPUSHX, key, () -> tairConnection.rPushX(key, value));
    }

    @Override
    public Long lPushX(byte[] key, byte[] value) {
        return actionWrapper.doInScope(TairAndRedisCommand.LPUSHX, key, () -> tairConnection.lPushX(key, value));
    }

    @Override
    public Long lLen(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.LLEN, key, () -> tairConnection.lLen(key));
    }

    @Override
    public List<byte[]> lRange(byte[] key, long start, long end) {
        return actionWrapper.doInScope(TairAndRedisCommand.LRANGE, key, () -> tairConnection.lRange(key, start, end));
    }

    @Override
    public void lTrim(byte[] key, long start, long end) {
        actionWrapper.doInScope(TairAndRedisCommand.LTRIM, key, () -> tairConnection.lTrim(key, start, end));
    }

    @Override
    public byte[] lIndex(byte[] key, long index) {
        return actionWrapper.doInScope(TairAndRedisCommand.LINDEX, key, () -> tairConnection.lIndex(key, index));
    }

    @Override
    public Long lInsert(byte[] key, Position where, byte[] pivot, byte[] value) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.LINSERT, key, () -> tairConnection.lInsert(key, where, pivot, value));
    }

    @Override
    public void lSet(byte[] key, long index, byte[] value) {
        actionWrapper.doInScope(TairAndRedisCommand.LSET, key, () -> tairConnection.lSet(key, index, value));
    }

    @Override
    public Long lRem(byte[] key, long count, byte[] value) {
        return actionWrapper.doInScope(TairAndRedisCommand.LREM, key, () -> tairConnection.lRem(key, count, value));
    }

    @Override
    public byte[] lPop(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.LPOP, key, () -> tairConnection.lPop(key));
    }

    @Override
    public byte[] rPop(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.RPOP, key, () -> tairConnection.rPop(key));
    }

    @Override
    public List<byte[]> bLPop(int timeout, byte[]... keys) {
        return actionWrapper.doInScope(TairAndRedisCommand.BLPOP, keys, () -> tairConnection.bLPop(timeout, keys));
    }

    @Override
    public List<byte[]> bRPop(int timeout, byte[]... keys) {
        return actionWrapper.doInScope(TairAndRedisCommand.BRPOP, keys, () -> tairConnection.bRPop(timeout, keys));
    }

    @Override
    public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.RPOPLPUSH, srcKey, () -> tairConnection.rPopLPush(srcKey, dstKey));
    }

    @Override
    public byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey) {
        return actionWrapper.doInScope(TairAndRedisCommand.BRPOPLPUSH, srcKey,
                () -> tairConnection.bRPopLPush(timeout, srcKey, dstKey));
    }

    @Override
    public boolean isSubscribed() {
        return tairConnection.isSubscribed();
    }

    @Override
    public Subscription getSubscription() {
        return tairConnection.getSubscription();
    }

    @Override
    public Long publish(byte[] channel, byte[] message) {
        return actionWrapper.doInScope(TairAndRedisCommand.PUBLISH, () -> tairConnection.publish(channel, message));
    }

    @Override
    public void subscribe(MessageListener listener, byte[]... channels) {
        actionWrapper.doInScope(TairAndRedisCommand.SUBSCRIBE, () -> tairConnection.subscribe(listener, channels));
    }

    @Override
    public void pSubscribe(MessageListener listener, byte[]... patterns) {
        actionWrapper.doInScope(TairAndRedisCommand.PSUBSCRIBE, () -> tairConnection.pSubscribe(listener, patterns));
    }

    @Override
    public void scriptFlush() {
        actionWrapper.doInScope(TairAndRedisCommand.SCRIPT_FLUSH, () -> tairConnection.scriptFlush());
    }

    @Override
    public void scriptKill() {
        actionWrapper.doInScope(TairAndRedisCommand.SCRIPT_KILL, () -> tairConnection.scriptKill());
    }

    @Override
    public String scriptLoad(byte[] script) {
        return actionWrapper.doInScope(TairAndRedisCommand.SCRIPT_LOAD, () -> tairConnection.scriptLoad(script));
    }

    @Override
    public List<Boolean> scriptExists(String... scriptShas) {
        return actionWrapper.doInScope(TairAndRedisCommand.SCRIPT_EXISTS, () -> tairConnection.scriptExists(scriptShas));
    }

    @Override
    public <T> T eval(byte[] script, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
        return actionWrapper.doInScope(TairAndRedisCommand.EVAL,
                () -> tairConnection.eval(script, returnType, numKeys, keysAndArgs));
    }

    @Override
    public <T> T evalSha(String scriptSha, ReturnType returnType, int numKeys,
                         byte[]... keysAndArgs) {
        return actionWrapper.doInScope(TairAndRedisCommand.EVALSHA,
                () -> tairConnection.evalSha(scriptSha, returnType, numKeys, keysAndArgs));
    }

    @Override
    public <T> T evalSha(byte[] scriptSha, ReturnType returnType, int numKeys,
                         byte[]... keysAndArgs) {
        return actionWrapper.doInScope(TairAndRedisCommand.EVALSHA,
                () -> tairConnection.evalSha(scriptSha, returnType, numKeys, keysAndArgs));
    }

    @Override
    public void bgWriteAof() {
        actionWrapper.doInScope(TairAndRedisCommand.BGWRITEAOF, () -> tairConnection.bgWriteAof());
    }

    @Override
    public void bgReWriteAof() {
        actionWrapper.doInScope(TairAndRedisCommand.BGREWRITEAOF, () -> tairConnection.bgReWriteAof());
    }

    @Override
    public void bgSave() {
        actionWrapper.doInScope(TairAndRedisCommand.BGSAVE, () -> tairConnection.bgSave());
    }

    @Override
    public Long lastSave() {
        return actionWrapper.doInScope(TairAndRedisCommand.LASTSAVE, () -> tairConnection.lastSave());
    }

    @Override
    public void save() {
        actionWrapper.doInScope(TairAndRedisCommand.SAVE, () -> tairConnection.save());
    }

    @Override
    public Long dbSize() {
        return actionWrapper.doInScope(TairAndRedisCommand.DBSIZE, () -> tairConnection.dbSize());
    }

    @Override
    public void flushDb() {
        actionWrapper.doInScope(TairAndRedisCommand.FLUSHDB, () -> tairConnection.flushDb());
    }

    @Override
    public void flushAll() {
        actionWrapper.doInScope(TairAndRedisCommand.FLUSHALL, () -> tairConnection.flushAll());
    }

    @Override
    public Properties info() {
        return actionWrapper.doInScope(TairAndRedisCommand.INFO, () -> tairConnection.info());
    }

    @Override
    public Properties info(String section) {
        return actionWrapper.doInScope(TairAndRedisCommand.INFO, () -> tairConnection.info(section));
    }

    @Override
    public void shutdown() {
        actionWrapper.doInScope(TairAndRedisCommand.SHUTDOWN, () -> tairConnection.shutdown());
    }

    @Override
    public void shutdown(ShutdownOption option) {
        actionWrapper.doInScope(TairAndRedisCommand.SHUTDOWN, () -> tairConnection.shutdown(option));
    }

    @Override
    public Properties getConfig(String pattern) {
        return actionWrapper.doInScope(TairAndRedisCommand.CONFIG_GET, () -> tairConnection.getConfig(pattern));
    }

    @Override
    public void setConfig(String param, String value) {
        actionWrapper.doInScope(TairAndRedisCommand.CONFIG_SET, () -> tairConnection.setConfig(param, value));
    }

    @Override
    public void resetConfigStats() {
        actionWrapper.doInScope(TairAndRedisCommand.CONFIG_RESETSTAT, () -> tairConnection.resetConfigStats());
    }

    @Override
    public Long time() {
        return actionWrapper.doInScope(TairAndRedisCommand.TIME, () -> tairConnection.time());
    }

    @Override
    public void killClient(String host, int port) {
        actionWrapper.doInScope(TairAndRedisCommand.CLIENT_KILL, () -> tairConnection.killClient(host, port));
    }

    @Override
    public void setClientName(byte[] name) {
        actionWrapper.doInScope(TairAndRedisCommand.CLIENT_SETNAME, () -> tairConnection.setClientName(name));
    }

    @Override
    public String getClientName() {
        return actionWrapper.doInScope(TairAndRedisCommand.CLIENT_GETNAME, () -> tairConnection.getClientName());
    }

    @Override
    public List<RedisClientInfo> getClientList() {
        return actionWrapper.doInScope(TairAndRedisCommand.CLIENT_LIST, () -> tairConnection.getClientList());
    }

    @Override
    public void slaveOf(String host, int port) {
        actionWrapper.doInScope(TairAndRedisCommand.SLAVEOF, () -> tairConnection.slaveOf(host, port));
    }

    @Override
    public void slaveOfNoOne() {
        actionWrapper.doInScope(TairAndRedisCommand.SLAVEOFNOONE, () -> tairConnection.slaveOfNoOne());
    }

    @Override
    public void migrate(byte[] key, RedisNode target, int dbIndex, MigrateOption option) {
        actionWrapper.doInScope(TairAndRedisCommand.MIGRATE, key,
                () -> tairConnection.migrate(key, target, dbIndex, option));
    }

    @Override
    public void migrate(byte[] key, RedisNode target, int dbIndex, MigrateOption option,
                        long timeout) {
        actionWrapper.doInScope(TairAndRedisCommand.MIGRATE,
                key, () -> tairConnection.migrate(key, target, dbIndex, option, timeout));
    }

    @Override
    public Long sAdd(byte[] key, byte[]... values) {
        return actionWrapper.doInScope(TairAndRedisCommand.SADD, key, () -> tairConnection.sAdd(key, values));
    }

    @Override
    public Long sRem(byte[] key, byte[]... values) {
        return actionWrapper.doInScope(TairAndRedisCommand.SREM, key, () -> tairConnection.sRem(key, values));
    }

    @Override
    public byte[] sPop(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.SPOP, key, () -> tairConnection.sPop(key));
    }

    @Override
    public List<byte[]> sPop(byte[] key, long count) {
        return actionWrapper.doInScope(TairAndRedisCommand.SPOP, key, () -> tairConnection.sPop(key, count));
    }

    @Override
    public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.SMOVE, srcKey, () -> tairConnection.sMove(srcKey, destKey, value));
    }

    @Override
    public Long sCard(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.SCARD, key, () -> tairConnection.sCard(key));
    }

    @Override
    public Boolean sIsMember(byte[] key, byte[] value) {
        return actionWrapper.doInScope(TairAndRedisCommand.SISMEMBER, key, () -> tairConnection.sIsMember(key, value));
    }

    @Override
    public Set<byte[]> sInter(byte[]... keys) {
        return actionWrapper.doInScope(TairAndRedisCommand.SINTER, keys, () -> tairConnection.sInter(keys));
    }

    @Override
    public Long sInterStore(byte[] destKey, byte[]... keys) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.SINTERSTORE, keys, () -> tairConnection.sInterStore(destKey, keys));
    }

    @Override
    public Set<byte[]> sUnion(byte[]... keys) {
        return actionWrapper.doInScope(TairAndRedisCommand.SUNION, keys, () -> tairConnection.sUnion(keys));
    }

    @Override
    public Long sUnionStore(byte[] destKey, byte[]... keys) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.SUNIONSTORE, keys, () -> tairConnection.sUnionStore(destKey, keys));
    }

    @Override
    public Set<byte[]> sDiff(byte[]... keys) {
        return actionWrapper.doInScope(TairAndRedisCommand.SDIFF, keys, () -> tairConnection.sDiff(keys));
    }

    @Override
    public Long sDiffStore(byte[] destKey, byte[]... keys) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.SDIFFSTORE, keys, () -> tairConnection.sDiffStore(destKey, keys));
    }

    @Override
    public Set<byte[]> sMembers(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.SMEMBERS, key, () -> tairConnection.sMembers(key));
    }

    @Override
    public byte[] sRandMember(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.SRANDMEMBER, key, () -> tairConnection.sRandMember(key));
    }

    @Override
    public List<byte[]> sRandMember(byte[] key, long count) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.SRANDMEMBER, key, () -> tairConnection.sRandMember(key, count));
    }

    @Override
    public Cursor<byte[]> sScan(byte[] key, ScanOptions options) {
        return actionWrapper.doInScope(TairAndRedisCommand.SSCAN, key, () -> tairConnection.sScan(key, options));
    }

    @Override
    public byte[] get(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.GET, key, () -> tairConnection.get(key));
    }

    @Override
    public byte[] getSet(byte[] key, byte[] value) {
        return actionWrapper.doInScope(TairAndRedisCommand.GETSET, key, () -> tairConnection.getSet(key, value));
    }

    @Override
    public List<byte[]> mGet(byte[]... keys) {
        return actionWrapper.doInScope(TairAndRedisCommand.MGET, keys, () -> tairConnection.mGet(keys));
    }

    @Override
    public Boolean set(byte[] key, byte[] value) {
        return actionWrapper.doInScope(TairAndRedisCommand.SET, key, () -> tairConnection.set(key, value));
    }

    @Override
    public Boolean set(byte[] key, byte[] value, Expiration expiration, SetOption option) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.SET, key, () -> tairConnection.set(key, value, expiration, option));
    }

    @Override
    public Boolean setNX(byte[] key, byte[] value) {
        return actionWrapper.doInScope(TairAndRedisCommand.SETNX, key, () -> tairConnection.setNX(key, value));
    }

    @Override
    public Boolean setEx(byte[] key, long seconds, byte[] value) {
        return actionWrapper.doInScope(TairAndRedisCommand.SETEX, key, () -> tairConnection.setEx(key, seconds, value));
    }

    @Override
    public Boolean pSetEx(byte[] key, long milliseconds, byte[] value) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.PSETEX, key, () -> tairConnection.pSetEx(key, milliseconds, value));
    }

    @Override
    public Boolean mSet(Map<byte[], byte[]> tuple) {
        return actionWrapper.doInScope(TairAndRedisCommand.MSET, () -> tairConnection.mSet(tuple));
    }

    @Override
    public Boolean mSetNX(Map<byte[], byte[]> tuple) {
        return actionWrapper.doInScope(TairAndRedisCommand.MSETNX, () -> tairConnection.mSetNX(tuple));
    }

    @Override
    public Long incr(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.INCR, key, () -> tairConnection.incr(key));
    }

    @Override
    public Long incrBy(byte[] key, long value) {
        return actionWrapper.doInScope(TairAndRedisCommand.INCRBY, key, () -> tairConnection.incrBy(key, value));
    }

    @Override
    public Double incrBy(byte[] key, double value) {
        return actionWrapper.doInScope(TairAndRedisCommand.INCRBY, key, () -> tairConnection.incrBy(key, value));
    }

    @Override
    public Long decr(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.DECR, key, () -> tairConnection.decr(key));
    }

    @Override
    public Long decrBy(byte[] key, long value) {
        return actionWrapper.doInScope(TairAndRedisCommand.DECRBY, key, () -> tairConnection.decrBy(key, value));
    }

    @Override
    public Long append(byte[] key, byte[] value) {
        return actionWrapper.doInScope(TairAndRedisCommand.APPEND, key, () -> tairConnection.append(key, value));
    }

    @Override
    public byte[] getRange(byte[] key, long begin, long end) {
        return actionWrapper.doInScope(TairAndRedisCommand.GETRANGE, key, () -> tairConnection.getRange(key, begin, end));
    }

    @Override
    public void setRange(byte[] key, byte[] value, long offset) {
        actionWrapper.doInScope(TairAndRedisCommand.SETRANGE, key, () -> tairConnection.setRange(key, value, offset));
    }

    @Override
    public Boolean getBit(byte[] key, long offset) {
        return actionWrapper.doInScope(TairAndRedisCommand.GETBIT, key, () -> tairConnection.getBit(key, offset));
    }

    @Override
    public Boolean setBit(byte[] key, long offset, boolean value) {
        return actionWrapper.doInScope(TairAndRedisCommand.SETBIT, key, () -> tairConnection.setBit(key, offset, value));
    }

    @Override
    public Long bitCount(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.BITCOUNT, key, () -> tairConnection.bitCount(key));
    }

    @Override
    public Long bitCount(byte[] key, long begin, long end) {
        return actionWrapper.doInScope(TairAndRedisCommand.BITCOUNT, key, () -> tairConnection.bitCount(key, begin, end));
    }

    @Override
    public List<Long> bitField(byte[] key, BitFieldSubCommands subCommands) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.BITFIELD, key, () -> tairConnection.bitField(key, subCommands));
    }

    @Override
    public Long bitOp(BitOperation op, byte[] destination, byte[]... keys) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.BITOP, keys, () -> tairConnection.bitOp(op, destination, keys));
    }

    @Override
    public Long bitPos(byte[] key, boolean bit, org.springframework.data.domain.Range<Long> range) {
        return actionWrapper.doInScope(TairAndRedisCommand.BITPOS, key, () -> tairConnection.bitPos(key, bit, range));
    }

    @Override
    public Long strLen(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.STRLEN, key, () -> tairConnection.strLen(key));
    }

    @Override
    public void multi() {
        actionWrapper.doInScope(TairAndRedisCommand.MULTI, () -> tairConnection.multi());
    }

    @Override
    public List<Object> exec() {
        return actionWrapper.doInScope(TairAndRedisCommand.EXEC, () -> tairConnection.exec());
    }

    @Override
    public void discard() {
        actionWrapper.doInScope(TairAndRedisCommand.DISCARD, () -> tairConnection.discard());
    }

    @Override
    public void watch(byte[]... keys) {
        actionWrapper.doInScope(TairAndRedisCommand.WATCH, () -> tairConnection.watch(keys));
    }

    @Override
    public void unwatch() {
        actionWrapper.doInScope(TairAndRedisCommand.UNWATCH, () -> tairConnection.unwatch());
    }

    @Override
    public Boolean zAdd(byte[] key, double score, byte[] value) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZADD, key, () -> tairConnection.zAdd(key, score, value));
    }

    @Override
    public Long zAdd(byte[] key, Set<Tuple> tuples) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZADD, key, () -> tairConnection.zAdd(key, tuples));
    }

    @Override
    public Long zRem(byte[] key, byte[]... values) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZREM, key, () -> tairConnection.zRem(key, values));
    }

    @Override
    public Double zIncrBy(byte[] key, double increment, byte[] value) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.ZINCRBY, key, () -> tairConnection.zIncrBy(key, increment, value));
    }

    @Override
    public Long zRank(byte[] key, byte[] value) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZRANK, key, () -> tairConnection.zRank(key, value));
    }

    @Override
    public Long zRevRank(byte[] key, byte[] value) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZREVRANK, key, () -> tairConnection.zRevRank(key, value));
    }

    @Override
    public Set<byte[]> zRange(byte[] key, long start, long end) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZRANGE, key, () -> tairConnection.zRange(key, start, end));
    }

    @Override
    public Set<byte[]> zRevRange(byte[] key, long start, long end) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.ZREVRANGE, key, () -> tairConnection.zRevRange(key, start, end));
    }

    @Override
    public Set<Tuple> zRevRangeWithScores(byte[] key, long start, long end) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZREVRANGE_WITHSCORES,
                key, () -> tairConnection.zRevRangeWithScores(key, start, end));
    }

    @Override
    public Set<Tuple> zRangeWithScores(byte[] key, long start, long end) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZRANGE_WITHSCORES,
                key, () -> tairConnection.zRangeWithScores(key, start, end));
    }

    @Override
    public Set<byte[]> zRangeByScore(byte[] key, double min, double max) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.ZRANGEBYSCORE, key, () -> tairConnection.zRangeByScore(key, min, max));
    }

    @Override
    public Set<byte[]> zRangeByScore(byte[] key, double min, double max, long offset, long count) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZRANGEBYSCORE,
                key, () -> tairConnection.zRangeByScore(key, min, max, offset, count));
    }

    @Override
    public Set<byte[]> zRangeByScore(byte[] key, String min, String max) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.ZRANGEBYSCORE, key, () -> tairConnection.zRangeByScore(key, min, max));
    }

    @Override
    public Set<byte[]> zRangeByScore(byte[] key, Range range) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.ZRANGEBYSCORE, key, () -> tairConnection.zRangeByScore(key, range));
    }

    @Override
    public Set<byte[]> zRangeByScore(byte[] key, String min, String max, long offset, long count) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZRANGEBYSCORE,
                key, () -> tairConnection.zRangeByScore(key, min, max, offset, count));
    }

    @Override
    public Set<byte[]> zRangeByScore(byte[] key, Range range, Limit limit) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZRANGEBYSCORE, key,
                () -> tairConnection.zRangeByScore(key, range, limit));
    }

    @Override
    public Set<Tuple> zRangeByScoreWithScores(byte[] key, Range range) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZRANGEBYSCORE_WITHSCORES,
                key, () -> tairConnection.zRangeByScoreWithScores(key, range));
    }

    @Override
    public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZRANGEBYSCORE_WITHSCORES,
                key, () -> tairConnection.zRangeByScoreWithScores(key, min, max));
    }

    @Override
    public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max, long offset,
                                              long count) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZRANGEBYSCORE_WITHSCORES,
                key, () -> tairConnection.zRangeByScoreWithScores(key, min, max, offset, count));
    }

    @Override
    public Set<Tuple> zRangeByScoreWithScores(byte[] key, Range range, Limit limit) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZRANGEBYSCORE_WITHSCORES,
                key, () -> tairConnection.zRangeByScoreWithScores(key, range, limit));
    }

    @Override
    public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZREVRANGEBYSCORE,
                key, () -> tairConnection.zRevRangeByScore(key, min, max));
    }

    @Override
    public Set<byte[]> zRevRangeByScore(byte[] key, Range range) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZREVRANGEBYSCORE, key,
                () -> tairConnection.zRevRangeByScore(key, range));
    }

    @Override
    public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max, long offset, long count) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZREVRANGEBYSCORE,
                key, () -> tairConnection.zRevRangeByScore(key, min, max, offset, count));
    }

    @Override
    public Set<byte[]> zRevRangeByScore(byte[] key, Range range, Limit limit) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZREVRANGEBYSCORE,
                key, () -> tairConnection.zRevRangeByScore(key, range, limit));
    }

    @Override
    public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZREVRANGEBYSCORE_WITHSCORES,
                key, () -> tairConnection.zRevRangeByScoreWithScores(key, min, max));
    }

    @Override
    public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max, long offset,
                                                 long count) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZREVRANGEBYSCORE_WITHSCORES,
                key, () -> tairConnection.zRevRangeByScoreWithScores(key, min, max, offset, count));
    }

    @Override
    public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, Range range) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZREVRANGEBYSCORE_WITHSCORES,
                key, () -> tairConnection.zRevRangeByScoreWithScores(key, range));
    }

    @Override
    public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, Range range, Limit limit) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZREVRANGEBYSCORE_WITHSCORES,
                key, () -> tairConnection.zRevRangeByScoreWithScores(key, range, limit));
    }

    @Override
    public Long zCount(byte[] key, double min, double max) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZCOUNT, key, () -> tairConnection.zCount(key, min, max));
    }

    @Override
    public Long zCount(byte[] key, Range range) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZCOUNT, key, () -> tairConnection.zCount(key, range));
    }

    @Override
    public Long zCard(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZCARD, key, () -> tairConnection.zCard(key));
    }

    @Override
    public Double zScore(byte[] key, byte[] value) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZSCORE, key, () -> tairConnection.zScore(key, value));
    }

    @Override
    public Long zRemRange(byte[] key, long start, long end) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.ZREMRANGE, key, () -> tairConnection.zRemRange(key, start, end));
    }

    @Override
    public Long zRemRangeByScore(byte[] key, double min, double max) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZREMRANGEBYSCORE,
                key, () -> tairConnection.zRemRangeByScore(key, min, max));
    }

    @Override
    public Long zRemRangeByScore(byte[] key, Range range) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZREMRANGEBYSCORE, key,
                () -> tairConnection.zRemRangeByScore(key, range));
    }

    @Override
    public Long zUnionStore(byte[] destKey, byte[]... sets) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZUNIONSTORE, () -> tairConnection.zUnionStore(destKey, sets));
    }

    @Override
    public Long zUnionStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZUNIONSTORE,
                destKey, () -> tairConnection.zUnionStore(destKey, aggregate, weights, sets));
    }

    @Override
    public Long zUnionStore(byte[] destKey, Aggregate aggregate, Weights weights, byte[]... sets) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZUNIONSTORE,
                destKey, () -> tairConnection.zUnionStore(destKey, aggregate, weights, sets));
    }

    @Override
    public Long zInterStore(byte[] destKey, byte[]... sets) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.ZINTERSTORE, destKey, () -> tairConnection.zInterStore(destKey, sets));
    }

    @Override
    public Long zInterStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZINTERSTORE,
                destKey, () -> tairConnection.zInterStore(destKey, aggregate, weights, sets));
    }

    @Override
    public Long zInterStore(byte[] destKey, Aggregate aggregate, Weights weights, byte[]... sets) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZINTERSTORE,
                destKey, () -> tairConnection.zInterStore(destKey, aggregate, weights, sets));
    }

    @Override
    public Cursor<Tuple> zScan(byte[] key, ScanOptions options) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZSCAN, key, () -> tairConnection.zScan(key, options));
    }

    @Override
    public Set<byte[]> zRangeByLex(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.ZRANGEBYLEX, key, () -> tairConnection.zRangeByLex(key));
    }

    @Override
    public Set<byte[]> zRangeByLex(byte[] key, Range range) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.ZRANGEBYLEX, key, () -> tairConnection.zRangeByLex(key, range));
    }

    @Override
    public Set<byte[]> zRangeByLex(byte[] key, Range range, Limit limit) {
        return actionWrapper
                .doInScope(TairAndRedisCommand.ZRANGEBYLEX, key, () -> tairConnection.zRangeByLex(key, range, limit));
    }

    @Override
    public TairStringCommands tairStringCommands() {
        return tairConnection.tairStringCommands();
    }

    @Override
    public Long cas(byte[] key, byte[] oldvalue, byte[] newvalue) {
        return actionWrapper.doInScope(TairAndRedisCommand.CAS, key, () -> tairConnection.cas(key, oldvalue, newvalue));
    }

    @Override
    public Long cas(byte[] key, byte[] oldvalue, byte[] newvalue, CasParams params) {
        return actionWrapper.doInScope(TairAndRedisCommand.CAS, key, () -> tairConnection.cas(key, oldvalue, newvalue, params));
    }

    @Override
    public Long cad(byte[] key, byte[] value) {
        return actionWrapper.doInScope(TairAndRedisCommand.CAD, key, () -> tairConnection.cad(key, value));

    }

    @Override
    public String exset(byte[] key, byte[] value) {
        return actionWrapper.doInScope(TairAndRedisCommand.EXSET, key, () -> tairConnection.exset(key, value));
    }

    @Override
    public String exset(byte[] key, byte[] value, ExsetParams params) {
        return actionWrapper.doInScope(TairAndRedisCommand.EXSET, key, () -> tairConnection.exset(key, value, params));

    }

    @Override
    public ExgetResult<byte[]> exget(byte[] key) {
        return actionWrapper.doInScope(TairAndRedisCommand.EXGET, key, () -> tairConnection.exget(key));
    }

    @Override
    public Long exsetver(byte[] key, long version) {
        return actionWrapper.doInScope(TairAndRedisCommand.EXSETVER, key, () -> tairConnection.exsetver(key, version));
    }

    @Override
    public Long exincrBy(byte[] key, long incr) {
        return actionWrapper.doInScope(TairAndRedisCommand.EXINCRBY, key, () -> tairConnection.exincrBy(key, incr));
    }

    @Override
    public Long exincrBy(byte[] key, long incr, ExincrbyParams params) {
        return actionWrapper.doInScope(TairAndRedisCommand.EXINCRBY, key, () -> tairConnection.exincrBy(key, incr, params));
    }

    @Override
    public Double exincrByFloat(byte[] key, Double incr) {
        return actionWrapper.doInScope(TairAndRedisCommand.EXINCRBYFLOAT, key, () -> tairConnection.exincrByFloat(key, incr));
    }

    @Override
    public Double exincrByFloat(byte[] key, Double incr, ExincrbyFloatParams params) {
        return actionWrapper.doInScope(TairAndRedisCommand.EXINCRBYFLOAT, key, () -> tairConnection.exincrByFloat(key, incr, params));
    }

    @Override
    public ExcasResult<byte[]> excas(byte[] key, byte[] newvalue, long version) {
        return actionWrapper.doInScope(TairAndRedisCommand.EXCAS, key, () -> tairConnection.excas(key, newvalue, version));
    }

    @Override
    public Long excad(byte[] key, long version) {
        return actionWrapper.doInScope(TairAndRedisCommand.EXCAD, key, () -> tairConnection.excad(key, version));
    }

    @Override
    public Long xAck(byte[] bytes, String s, RecordId... recordIds) {
        return actionWrapper.doInScope(TairAndRedisCommand.XACK,
                () -> tairConnection.xAck(bytes, s, recordIds));
    }

    @Override
    public RecordId xAdd(MapRecord<byte[], byte[], byte[]> mapRecord) {
        return actionWrapper.doInScope(TairAndRedisCommand.XADD,
                () -> tairConnection.xAdd(mapRecord));
    }

    @Override
    public Long xDel(byte[] bytes, RecordId... recordIds) {
        return actionWrapper.doInScope(TairAndRedisCommand.XDEL,
                () -> tairConnection.xDel(bytes, recordIds));
    }

    @Override
    public String xGroupCreate(byte[] bytes, String s, ReadOffset readOffset) {
        return actionWrapper.doInScope(TairAndRedisCommand.XGROUPCREATE,
                () -> tairConnection.xGroupCreate(bytes, s, readOffset));
    }

    @Override
    public Boolean xGroupDelConsumer(byte[] bytes, Consumer consumer) {
        return actionWrapper.doInScope(TairAndRedisCommand.XGROUPCREATE,
                () -> tairConnection.xGroupDelConsumer(bytes, consumer));
    }

    @Override
    public Boolean xGroupDestroy(byte[] bytes, String s) {
        return actionWrapper.doInScope(TairAndRedisCommand.XGROUPDESTORY,
                () -> tairConnection.xGroupDestroy(bytes, s));
    }

    @Override
    public Long xLen(byte[] bytes) {
        return actionWrapper.doInScope(TairAndRedisCommand.XLEN,
                () -> tairConnection.xLen(bytes));
    }

    @Override
    public List<ByteRecord> xRange(byte[] bytes,
                                   org.springframework.data.domain.Range<String> range, Limit limit) {
        return actionWrapper.doInScope(TairAndRedisCommand.XRANGE,
                () -> tairConnection.xRange(bytes, range, limit));
    }

    @Override
    public List<ByteRecord> xRead(StreamReadOptions streamReadOptions,
                                  StreamOffset<byte[]>... streamOffsets) {
        return actionWrapper.doInScope(TairAndRedisCommand.XRANGE,
                () -> tairConnection.xRead(streamReadOptions, streamOffsets));
    }

    @Override
    public List<ByteRecord> xReadGroup(Consumer consumer, StreamReadOptions streamReadOptions,
                                       StreamOffset<byte[]>... streamOffsets) {
        return actionWrapper.doInScope(TairAndRedisCommand.XREADGROUP,
                () -> tairConnection.xReadGroup(consumer, streamReadOptions, streamOffsets));
    }

    @Override
    public List<ByteRecord> xRevRange(byte[] bytes,
                                      org.springframework.data.domain.Range<String> range,
                                      Limit limit) {
        return actionWrapper.doInScope(TairAndRedisCommand.XREADGROUP,
                () -> tairConnection.xRevRange(bytes, range, limit));
    }

    @Override
    public Long xTrim(byte[] bytes, long l) {
        return actionWrapper.doInScope(TairAndRedisCommand.XREADGROUP,
                () -> tairConnection.xTrim(bytes, l));
    }
}
