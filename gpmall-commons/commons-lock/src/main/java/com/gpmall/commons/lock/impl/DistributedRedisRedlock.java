package com.gpmall.commons.lock.impl;

import com.gpmall.commons.lock.DistributedLockException;
import org.redisson.Redisson;
import org.redisson.RedissonRedLock;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.util.concurrent.TimeUnit;

/**
 * @author cb
 * @title: DistributedRedisRedlock 基于Redlock
 * @projectName gpmall
 * @description: TODO
 * @date 2019/10/31 11:14
 */
public class DistributedRedisRedlock extends DistributedRedisLock{

    private RedissonClient redissonClient1;
    private RedissonClient redissonClient2;
    private RedissonClient redissonClient3;
    public DistributedRedisRedlock() {
        Config config1 = new Config();
        config1.useSingleServer().setAddress("redis://127.0.0.1:6379").setDatabase(0);
        redissonClient1 = Redisson.create(config1);
        Config config2 = new Config();
        config2.useSingleServer().setAddress("redis://127.0.0.1:6380").setDatabase(0);
        redissonClient2 = Redisson.create(config2);
        Config config3 = new Config();
        config3.useSingleServer().setAddress("redis://127.0.0.1:6381").setDatabase(0);
        redissonClient3 = Redisson.create(config3);
    }

    @Override
    public void lock(String key) {
        RLock lock1 = redissonClient1.getLock(key);
        RLock lock2 = redissonClient2.getLock(key);
        RLock lock3 = redissonClient3.getLock(key);
        RedissonRedLock redissonRedLock = new RedissonRedLock(lock1,lock2,lock3);
        redissonRedLock.lock();
    }

    @Override
    public boolean tryLock(String key) {
        RLock lock1 = redissonClient1.getLock(key);
        RLock lock2 = redissonClient2.getLock(key);
        RLock lock3 = redissonClient3.getLock(key);
        RedissonRedLock redissonRedLock = new RedissonRedLock(lock1,lock2,lock3);
       return redissonRedLock.tryLock();
    }

    @Override
    public void lock(String lockKey, TimeUnit unit, int leaseTime) {
        RLock lock1 = redissonClient1.getLock(lockKey);
        RLock lock2 = redissonClient2.getLock(lockKey);
        RLock lock3 = redissonClient3.getLock(lockKey);
        RedissonRedLock redissonRedLock = new RedissonRedLock(lock1,lock2,lock3);
        redissonRedLock.lock(leaseTime, unit);
    }

    @Override
    public boolean tryLock(String lockKey, TimeUnit unit, int waitTime, int leaseTime) throws DistributedLockException {
        return super.tryLock(lockKey, unit, waitTime, leaseTime);
    }

    @Override
    public void unlock(String lockKey) {
        super.unlock(lockKey);
    }
}
