package click.dailyfeed.kafka.domain.activity.redis;

import click.dailyfeed.code.global.cache.RedisKeyPrefix;
import click.dailyfeed.code.global.redis.RedisKeyExistPredicate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class KafkaMessageKeyMemberActivityRedisService {
    private final RedisTemplate<String, String> redisTemplate;

    public KafkaMessageKeyMemberActivityRedisService(
            @Qualifier("kafkaMessageKeyMemberActivityRedisTemplate") RedisTemplate<String, String> redisTemplate
    ) {
        this.redisTemplate = redisTemplate;
    }

    public void addAndExpireIn(String messageKey, Duration expireIn) {
        String redisKey = redisKey(messageKey);
        redisTemplate.opsForValue().set(redisKey, messageKey, expireIn);
    }

    public RedisKeyExistPredicate checkExist(String messageKey){
        String redisKey = redisKey(messageKey);
        if(redisTemplate.hasKey(redisKey)){
            return RedisKeyExistPredicate.EXIST;
        }
        return RedisKeyExistPredicate.NOT_EXIST;
    }

    public String redisKey(String messageKey){
        String kafkaKeyPrefix = RedisKeyPrefix.MEMBER_ACTIVITY_KAFKA_KEY.getKeyPrefix();
        return new StringBuffer().append(kafkaKeyPrefix)
                .append(messageKey)
                .toString();
    }
}
