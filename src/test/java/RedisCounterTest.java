import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;


public class RedisCounterTest {

    @Test
    public void serialTest() {
        int num = 0;
        int total = 2000;
        Jedis jedis = new Jedis();
        RedisCounter redisCounter = new RedisCounter(jedis, total);
        for (int i = 0; i < 3000; i++) {
            if (redisCounter.countDown()) {
                num++;
            }
        }
        Assert.assertEquals(total, num);
    }

    @Test
    public void parallelTest() throws InterruptedException {
        int total = 2000;
        int threadNum = 100;
        ExecutorService threadPool = Executors.newFixedThreadPool(threadNum);
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(threadNum + 1);
        for (int pieces = 1; pieces <= 10; pieces++) {
            JedisPool jedisPool = new JedisPool(jedisPoolConfig);
//            int pieces = 1;
            final RedisCounter redisCounter = new RedisCounter(jedisPool.getResource(), total, pieces);

            AtomicInteger i = new AtomicInteger(total);
            final CountDownLatch start = new CountDownLatch(1);
            final CountDownLatch end = new CountDownLatch(threadNum);
            for (int j = 0; j < threadNum; j++) {
                threadPool.execute(() -> {
                    Jedis resource = jedisPool.getResource();
                    try {
                        start.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    while (redisCounter.countDown(resource)) i.decrementAndGet();
                    resource.close();
                    end.countDown();
                });
            }
            long startTime = System.currentTimeMillis();
            start.countDown();
            end.await();
            long endTime = System.currentTimeMillis();
            jedisPool.close();

            Assert.assertEquals(0, i.get());
            System.out.printf("total: %d threadNum: %d pieces: %d time: %d%n", total, threadNum, pieces, endTime - startTime);
            Thread.sleep(5000);
        }
    }
}
