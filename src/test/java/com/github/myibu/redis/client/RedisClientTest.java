package com.github.myibu.redis.client;

import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RedisClientTest {
   static RedisClient redisClient;
   static RedisClient.RedisReply redisReply;

    @BeforeClass
    public static void init() {
        redisClient = new RedisClient();
    }

    @Test
    public void test01SimpleStrings() {
        redisReply = redisClient.execAndReturn("set foo bar");
        System.out.println(redisReply);
    }

    @Test
    public void test02Errors() {
        redisReply = redisClient.execAndReturn("hello");
        System.out.println(redisReply);
    }

    @Test
    public void test03Integers() {
        redisReply = redisClient.execAndReturn("incr 1");
        System.out.println(redisReply);
    }

    @Test
    public void test04BulkStrings() {
        redisReply = redisClient.execAndReturn("get foo");
        System.out.println(redisReply);
    }

    @Test
    public void test05Arrays() {
        redisClient.exec("lpush kk 2 3");
        redisReply = redisClient.execAndReturn("lrange kk 0 10");
        System.out.println(redisReply);
    }
}
