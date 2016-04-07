package com.hortonworks.iotas.cache.redis.test;

import com.lambdaworks.redis.RedisConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisClients {
//    protected static final Logger LOG = LoggerFactory.getLogger(RedisClients.class);
    private static RedisConnection<String, String> connection = RedisCacheTestMain.getConnection();

    public RedisClients() {
    }

    public static class Client1 implements Runnable {
        protected static final Logger LOG = LoggerFactory.getLogger(Client1.class);

        @Override
        public void run() {
            while (true) {
                LOG.info(connection.get("hugo"));
//                RedisCacheTestMain.readInput();
                sleep();
            }
        }

    }

    public static class Client2 implements Runnable {
        protected static final Logger LOG = LoggerFactory.getLogger(Client2.class);

        @Override
        public void run() {
            while (true) {
                LOG.info("{}", connection.hgetall("h"));
//                RedisCacheTestMain.readInput();
                sleep();
            }
        }
    }

    private static void sleep() {
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
