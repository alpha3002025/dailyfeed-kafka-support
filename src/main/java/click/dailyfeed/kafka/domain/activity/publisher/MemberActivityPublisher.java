package click.dailyfeed.kafka.domain.activity.publisher;

import click.dailyfeed.code.domain.activity.factory.MemberActivityTransferDtoFactory;
import click.dailyfeed.code.domain.activity.transport.MemberActivityTransportDto;
import click.dailyfeed.code.domain.activity.type.MemberActivityType;
import click.dailyfeed.code.global.kafka.exception.KafkaNetworkErrorException;
import click.dailyfeed.code.global.kafka.type.DateBasedTopicType;
import click.dailyfeed.kafka.domain.kafka.service.KafkaHelper;
import click.dailyfeed.kafka.domain.kafka.topic.DateBasedTopicResolver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Slf4j
@RequiredArgsConstructor
@Component
public class MemberActivityPublisher {
    private final KafkaHelper kafkaHelper;
    private final DateBasedTopicResolver dateBasedTopicResolver;

    /// post
    public void publishPostReadEvent(Long memberId, Long postId){
        try{
            LocalDateTime now = kafkaHelper.currentDateTime();
            String topicName = dateBasedTopicResolver.resolveDateBasedTopicName(DateBasedTopicType.MEMBER_ACTIVITY, now);

            MemberActivityTransportDto.MemberActivityEvent memberPostLikeActivityEvent = MemberActivityTransferDtoFactory
                    .newPostMemberActivityTransportDto(memberId, postId, MemberActivityType.POST_READ, now);

            kafkaHelper.send(topicName, postId.toString(), memberPostLikeActivityEvent);
        }
        catch (Exception e){
            log.error("Error publishing post activity event: ", e);
            throw new KafkaNetworkErrorException();
        }
    }

    public void publishPostCUDEvent(Long memberId, Long postId, MemberActivityType activityType){
        try{
            LocalDateTime now = kafkaHelper.currentDateTime();
            String topicName = dateBasedTopicResolver.resolveDateBasedTopicName(DateBasedTopicType.MEMBER_ACTIVITY, now);

            MemberActivityTransportDto.MemberActivityEvent memberPostLikeActivityEvent = MemberActivityTransferDtoFactory
                    .newPostMemberActivityTransportDto(memberId, postId, activityType, now);

            kafkaHelper.send(topicName, postId.toString(), memberPostLikeActivityEvent);
        }
        catch (Exception e){
            log.error("Error publishing post activity event: ", e);
            throw new KafkaNetworkErrorException();
        }
    }

    /// comment
    public void publishCommentReadEvent(Long memberId, Long postId, Long commentId){
        try{
            LocalDateTime now = kafkaHelper.currentDateTime();
            String topicName = dateBasedTopicResolver.resolveDateBasedTopicName(DateBasedTopicType.MEMBER_ACTIVITY, now);

            MemberActivityTransportDto.MemberActivityEvent memberPostLikeActivityEvent = MemberActivityTransferDtoFactory
                    .newCommentMemberActivityTransportDto(memberId, postId, commentId, MemberActivityType.COMMENT_READ, now);

            kafkaHelper.send(topicName, commentId.toString(), memberPostLikeActivityEvent);
        }
        catch (Exception e){
            log.error("Error publishing post activity event: ", e);
            throw new KafkaNetworkErrorException();
        }
    }

    public void publishCommentCUDEvent(Long memberId, Long postId, Long commentId, MemberActivityType activityType){
        try{
            LocalDateTime now = kafkaHelper.currentDateTime();
            String topicName = dateBasedTopicResolver.resolveDateBasedTopicName(DateBasedTopicType.MEMBER_ACTIVITY, now);

            MemberActivityTransportDto.MemberActivityEvent memberPostLikeActivityEvent = MemberActivityTransferDtoFactory
                    .newCommentMemberActivityTransportDto(memberId, postId, commentId, activityType, now);

            kafkaHelper.send(topicName, postId.toString(), memberPostLikeActivityEvent);
        }
        catch (Exception e){
            log.error("Error publishing post activity event: ", e);
            throw new KafkaNetworkErrorException();
        }
    }

    /// post (like)

    /// comment (like)

    /// member
}
