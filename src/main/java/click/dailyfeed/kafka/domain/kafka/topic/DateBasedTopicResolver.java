package click.dailyfeed.kafka.domain.kafka.topic;

import click.dailyfeed.code.global.kafka.type.DateBasedTopicType;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
public class DateBasedTopicResolver {
    public String resolveDateBasedTopicName(DateBasedTopicType topicType, LocalDateTime createdAt) {
        return topicType.generateTopicName(createdAt);
    }
}
