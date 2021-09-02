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

    /**
     * RedisReply{type=SIMPLE_STRINGS, data=OK, raw=+OK\r\n}
     */
    @Test
    public void test01SimpleStrings() {
        redisReply = redisClient.execAndReturn("set foo bar");
        System.out.println(redisReply);
    }

    /**
     * RedisReply{type=ERRORS, data=wrong number of arguments for 'client' command, raw=-ERR wrong number of arguments for 'client' command\r\n}
     */
    @Test
    public void test02Errors() {
        redisReply = redisClient.execAndReturn(RedisClient.RedisCommand.CLIENT);
        System.out.println(redisReply);
    }

    /**
     * RedisReply{type=INTEGERS, data=1, raw=:1\r\n}
     */
    @Test
    public void test03Integers() {
        redisReply = redisClient.execAndReturn("incr 1");
        System.out.println(redisReply);
    }

    /**
     * RedisReply{type=BULK_STRINGS, data=bar, raw=$3\r\nbar\r\n}
     */
    @Test
    public void test04BulkStrings() {
        redisReply = redisClient.execAndReturn(RedisClient.RedisCommand.GET,"foo");
        System.out.println(redisReply);
    }

    /**
     * RedisReply{type=ARRAYS, data=[RedisReply{type=BULK_STRINGS, data=3, raw=$1\r\n3\r\n}, RedisReply{type=BULK_STRINGS, data=2, raw=$1\r\n2\r\n}], raw=*2\r\n$1\r\n3\r\n$1\r\n2\r\n}
     */
    @Test
    public void test05Arrays() {
        redisClient.exec("lpush kk 2 3");
        redisReply = redisClient.execAndReturn("lrange kk 0 10");
        System.out.println(redisReply);
    }
}
