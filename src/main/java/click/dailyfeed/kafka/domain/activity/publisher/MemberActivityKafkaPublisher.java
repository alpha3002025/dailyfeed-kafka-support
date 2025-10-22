package click.dailyfeed.kafka.domain.activity.publisher;

import click.dailyfeed.code.domain.activity.factory.MemberActivityTransferDtoFactory;
import click.dailyfeed.code.domain.activity.transport.MemberActivityTransportDto;
import click.dailyfeed.code.domain.activity.type.MemberActivityType;
import click.dailyfeed.code.global.kafka.exception.KafkaMessageKeyCreationException;
import click.dailyfeed.code.global.kafka.exception.KafkaNetworkErrorException;
import click.dailyfeed.code.global.kafka.type.DateBasedTopicType;
import click.dailyfeed.kafka.domain.activity.redis.MemberActivityEventDLQRedisService;
import click.dailyfeed.kafka.domain.kafka.service.KafkaHelper;
import click.dailyfeed.kafka.domain.kafka.topic.DateBasedTopicResolver;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Slf4j
@RequiredArgsConstructor
@Component
public class MemberActivityKafkaPublisher {
    private final KafkaHelper kafkaHelper;
    private final DateBasedTopicResolver dateBasedTopicResolver;
    private final MemberActivityEventDLQRedisService memberActivityEventDLQRedisService;
    private final ObjectMapper objectMapper;

    /// post
    public void publishPostReadEvent(Long memberId, Long postId){
        LocalDateTime now = kafkaHelper.currentDateTime();
        String topicName = dateBasedTopicResolver.resolveDateBasedTopicName(DateBasedTopicType.MEMBER_ACTIVITY, now);

        MemberActivityTransportDto.MemberActivityEvent event = MemberActivityTransferDtoFactory
                .newPostMemberActivityTransportDto(memberId, postId, MemberActivityType.POST_READ, now);

        MemberActivityTransportDto.MemberActivityMessage message = MemberActivityTransferDtoFactory
                .newMemberActivityMessage(event, MemberActivityType.POST_READ, now);

        if (message.getKey() == null){
            throw new KafkaMessageKeyCreationException();
        }

        try{
            kafkaHelper.send(topicName, message.getKey(), message.getEvent());
        }
        catch (Exception e){
            throw new KafkaNetworkErrorException();
        }
    }

    public void publishPostCUDEvent(Long memberId, Long postId, MemberActivityType activityType){
        LocalDateTime now = kafkaHelper.currentDateTime();
        String topicName = dateBasedTopicResolver.resolveDateBasedTopicName(DateBasedTopicType.MEMBER_ACTIVITY, now);

        MemberActivityTransportDto.MemberActivityEvent event = MemberActivityTransferDtoFactory
                .newPostMemberActivityTransportDto(memberId, postId, activityType, now);

        MemberActivityTransportDto.MemberActivityMessage message = MemberActivityTransferDtoFactory
                .newMemberActivityMessage(event, activityType, now);

        if (message.getKey() == null){
            throw new KafkaMessageKeyCreationException();
        }

        try{
            kafkaHelper.send(topicName, message.getKey(), message.getEvent());
        }
        catch (Exception e){
            throw new KafkaNetworkErrorException();
        }
    }

    /// comment
    public void publishCommentReadEvent(Long memberId, Long postId, Long commentId){
        LocalDateTime now = kafkaHelper.currentDateTime();
        String topicName = dateBasedTopicResolver.resolveDateBasedTopicName(DateBasedTopicType.MEMBER_ACTIVITY, now);

        MemberActivityTransportDto.MemberActivityEvent event = MemberActivityTransferDtoFactory
                .newCommentMemberActivityTransportDto(memberId, postId, commentId, MemberActivityType.COMMENT_READ, now);

        MemberActivityTransportDto.MemberActivityMessage message = MemberActivityTransferDtoFactory
                .newMemberActivityMessage(event, MemberActivityType.COMMENT_READ, now);

        if (message.getKey() == null){
            throw new KafkaMessageKeyCreationException();
        }

        try{
            kafkaHelper.send(topicName, message.getKey(), message.getEvent());
        }
        catch (Exception e){
            throw new KafkaNetworkErrorException();
        }
    }

    public void publishCommentCUDEvent(Long memberId, Long postId, Long commentId, MemberActivityType activityType){
        LocalDateTime now = kafkaHelper.currentDateTime();
        String topicName = dateBasedTopicResolver.resolveDateBasedTopicName(DateBasedTopicType.MEMBER_ACTIVITY, now);

        MemberActivityTransportDto.MemberActivityEvent event = MemberActivityTransferDtoFactory
                .newCommentMemberActivityTransportDto(memberId, postId, commentId, activityType, now);

        MemberActivityTransportDto.MemberActivityMessage message = MemberActivityTransferDtoFactory
                .newMemberActivityMessage(event, activityType, now);

        if (message.getKey() == null){
            throw new KafkaMessageKeyCreationException();
        }

        try{
            kafkaHelper.send(topicName, message.getKey(), message.getEvent());
        }
        catch (Exception e){
            throw new KafkaNetworkErrorException();
        }
    }

    /// post (like)
    public void publishPostLikeEvent(Long memberId, Long postId, MemberActivityType activityType){
        LocalDateTime now = kafkaHelper.currentDateTime();
        String topicName = dateBasedTopicResolver.resolveDateBasedTopicName(DateBasedTopicType.MEMBER_ACTIVITY, now);

        MemberActivityTransportDto.MemberActivityEvent event = MemberActivityTransferDtoFactory
                .newPostLikeMemberActivityTransportDto(memberId, postId, activityType, now);

        MemberActivityTransportDto.MemberActivityMessage message = MemberActivityTransferDtoFactory
                .newMemberActivityMessage(event, activityType, now);

        if (message.getKey() == null){
            throw new KafkaMessageKeyCreationException();
        }

        try{
            kafkaHelper.send(topicName, message.getKey(), message.getEvent());
        }
        catch (Exception e){
            throw new KafkaNetworkErrorException();
        }
    }

    /// comment (like)
    public void publishCommentLikeEvent(Long memberId, Long postId, Long commentId, MemberActivityType activityType){
        LocalDateTime now = kafkaHelper.currentDateTime();
        String topicName = dateBasedTopicResolver.resolveDateBasedTopicName(DateBasedTopicType.MEMBER_ACTIVITY, now);

        MemberActivityTransportDto.MemberActivityEvent event = MemberActivityTransferDtoFactory
                .newCommentLikeMemberActivityTransportDto(memberId, postId, commentId, activityType, now);

        MemberActivityTransportDto.MemberActivityMessage message = MemberActivityTransferDtoFactory
                .newMemberActivityMessage(event, activityType, now);

        if (message.getKey() == null){
            throw new KafkaMessageKeyCreationException();
        }

        try{
            kafkaHelper.send(topicName, message.getKey(), message.getEvent());
        }
        catch (Exception e){
            throw new KafkaNetworkErrorException();
        }
    }

    /// member
}
