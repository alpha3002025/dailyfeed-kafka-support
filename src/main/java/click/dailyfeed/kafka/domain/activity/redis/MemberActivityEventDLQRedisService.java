package click.dailyfeed.kafka.domain.activity.redis;

import click.dailyfeed.code.domain.activity.transport.MemberActivityTransportDto;
import click.dailyfeed.code.domain.activity.type.MemberActivityType;
import click.dailyfeed.code.global.cache.RedisKeyPrefix;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@RequiredArgsConstructor
@Service
public class MemberActivityEventDLQRedisService {
//    private static final Duration TTL = Duration.ofMinutes(30);

    @Qualifier("memberActivityTransportDtoRedisTemplate")
    private final RedisTemplate<String, MemberActivityTransportDto.MemberActivityEvent> redisTemplate;


    public String deadLetterKey(MemberActivityType memberActivityType) {
        return new StringBuffer()
                .append(RedisKeyPrefix.MEMBER_ACTIVITY_KAFKA_DLQ.getKeyPrefix())
                .append(memberActivityType.name())
                .toString();
    }

    public void rPush(MemberActivityTransportDto.MemberActivityEvent memberActivityEvent) {
        String key = deadLetterKey(memberActivityEvent.getMemberActivityType());
        redisTemplate.opsForList().rightPush(key, memberActivityEvent);
//        redisTemplate.expire(key, TTL);
    }

    public void rPushList(List<MemberActivityTransportDto.MemberActivityEvent> memberActivityEvents) {
        memberActivityEvents.stream().forEach(event -> {
            String deadLetterKey = deadLetterKey(event.getMemberActivityType());
            redisTemplate.opsForList().rightPush(deadLetterKey, event);
        });
    }

    public MemberActivityTransportDto.MemberActivityEvent lPop(MemberActivityType memberActivityType) {
        return redisTemplate.opsForList().leftPop(deadLetterKey(memberActivityType));
    }

    public List<MemberActivityTransportDto.MemberActivityEvent> lPopTopN(MemberActivityType memberActivityType, int size){
        String key = deadLetterKey(memberActivityType);
        return redisTemplate.opsForList().leftPop(key, size);
    }

    public void evictAll(MemberActivityTransportDto.MemberActivityEvent memberActivityEvent) {
        redisTemplate.delete(deadLetterKey(memberActivityEvent.getMemberActivityType()));
    }
}
