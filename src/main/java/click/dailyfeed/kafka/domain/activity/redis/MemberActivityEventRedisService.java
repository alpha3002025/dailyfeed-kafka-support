package click.dailyfeed.kafka.domain.activity.redis;

import click.dailyfeed.code.domain.activity.transport.MemberActivityTransportDto;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

@RequiredArgsConstructor
@Service
public class MemberActivityEventRedisService {
    private static final Duration TTL = Duration.ofSeconds(15);

    @Qualifier("memberActivityTransportDtoRedisTemplate")
    private final RedisTemplate<String, MemberActivityTransportDto.MemberActivityEvent> redisTemplate;

    public boolean checkExist(MemberActivityTransportDto.MemberActivityMessage message){
        return Boolean.TRUE.equals(redisTemplate.opsForSet().isMember(message.getKey(), message.getEvent()));
    }

    public void put(MemberActivityTransportDto.MemberActivityMessage message){
        redisTemplate.opsForSet().add(message.getKey(), message.getEvent());
        redisTemplate.expire(message.getKey(), TTL);
    }

    public void evict(MemberActivityTransportDto.MemberActivityMessage message){
        redisTemplate.delete(message.getKey());
    }

}
