package com.redis;

import redis.clients.jedis.Jedis;

/**
 * Created by shen_ on 2017/8/9.
 */
public class RedisTest {
    static String constr = "192.168.1.11" ;
    public static Jedis getRedis(){
        Jedis jedis = new Jedis(constr) ;
        return jedis ;
    }
    public static void main(String[] args){
        Jedis jedis = RedisTest.getRedis();
        jedis.set("shenyucong", "maxuetao");
        System.out.println(jedis.get("shenyucong"));

    }
}
