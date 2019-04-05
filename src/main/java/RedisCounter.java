import redis.clients.jedis.Jedis;

import java.util.concurrent.atomic.AtomicInteger;

public class RedisCounter {
    private static final String DEFAULT_KEY = "Latch";
    private static final int DEFAULT_PIECES = 5;
    private final long total;
    private final int pieces;
    private boolean[] ends;
    private String key;
    private Jedis jedis;
    private AtomicInteger current;

    public RedisCounter(Jedis jedis, long total) {
        this(jedis, DEFAULT_KEY, total);
    }

    public RedisCounter(Jedis jedis, String key, long total) {
        this(jedis, key, total, DEFAULT_PIECES);
    }

    public RedisCounter(Jedis jedis, int total, int pieces) {
        this(jedis, DEFAULT_KEY, total, pieces);
    }

    public RedisCounter(Jedis jedis, String key, long total, int pieces) {
        this.jedis = jedis;
        this.key = key;
        this.total = total;
        this.pieces = pieces;
        init();
    }

    private void init() {
        long total = this.total;
        long val = ((long) Math.ceil(total * 1.0 / pieces));
        for (int i = 0; i < pieces; i++) {
            jedis.set(getKey(i), String.valueOf(Math.min(val, total)));
            total -= val;
        }
        ends = new boolean[pieces];
        current = new AtomicInteger(0);
    }

    private String getKey(int i) {
        return key + "_" + i;
    }

    public boolean countDown() {
        return countDown(jedis);
    }

    public boolean countDown(Jedis jedis) {
        int i = current.getAndIncrement() % pieces;
        for (int j = 0; j < pieces; j++, i = (i + 1) % pieces) {
            if (ends[i]) continue;
            Long decr = jedis.decr(getKey(i));
            if (decr >= 0) return true;
            ends[i] = true;
        }
        return false;
    }

    public boolean isEmpty() {
        for (int i = 0; i < pieces; i++) {
            String s = jedis.get(getKey(i % pieces));
            if (Long.valueOf(s) >= 0) {
                return false;
            }
        }
        return true;
    }
}
