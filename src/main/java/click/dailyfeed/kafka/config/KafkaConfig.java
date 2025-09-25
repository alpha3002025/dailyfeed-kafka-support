package click.dailyfeed.kafka.config;

import click.dailyfeed.code.domain.content.comment.dto.CommentDto;
import click.dailyfeed.code.domain.content.post.dto.PostDto;
import click.dailyfeed.code.domain.member.member.dto.MemberDto;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:29092,localhost:29093,localhost:29094}")
    private String bootstrapServers;

//    @Value("${infrastructure.kafka.topic.post-activity.prefix}")
//    private String postActivityPrefix;
//
//    @Value("${infrastructure.kafka.topic.post-activity.prefix-date-format}")
//    private String dateFormat;
//
//    @Value("${infrastructure.kafka.topic.post-activity.retention-ms:604800000}")
//    private String retentionMs;

    @Value("${KAFKA_USER:}")
    private String kafkaUser;

    @Value("${KAFKA_PASSWORD:}")
    private String kafkaPassword;

    @Value("${KAFKA_SASL_PROTOCOL:PLAINTEXT}")
    private String saslProtocol;

    @Value("${KAFKA_SASL_MECHANISM:PLAIN}")
    private String saslMechanism;

    /// consumers
    private Map<String, Object> getCommonConsumerProps() {
        Map<String, Object> props = new HashMap<>();
        /// 브로커 설정
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        /// Deserializer 설정
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);

        /// At Least Once 관련 설정
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // 수동 커밋
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        /// 성능 및 안정성 설정
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000); // 5분
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000); // 30초
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000); // 10초

        /// 재시도 설정
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000);
        props.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 100);

        /// JSON Deserializer 설정
//        props.put(JsonDeserializer.TRUSTED_PACKAGES, "click.dailyfeed.code.domain.content,click.dailyfeed.code.domain.member");
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        // SASL 설정 (local 프로필에서)
        if (!kafkaUser.isEmpty() && !kafkaPassword.isEmpty()) {
            props.put("security.protocol", saslProtocol);
            props.put("sasl.mechanism", saslMechanism);
            props.put("sasl.jaas.config",
                    "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                            "username=\"" + kafkaUser + "\" " +
                            "password=\"" + kafkaPassword + "\";");
        }

        return props;
    }

    // Post Activity Consumer 설정
    @Bean(name = "postActivityConsumerFactory")
    public ConsumerFactory<String, PostDto.PostActivityEvent> postActivityConsumerFactory() {
        Map<String, Object> props = getCommonConsumerProps();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "post-activity-consumer-group");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, PostDto.PostActivityEvent.class.getName());
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PostDto.PostActivityEvent> postActivityKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, PostDto.PostActivityEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(postActivityConsumerFactory());

        /// At Least Once 설정
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setSyncCommits(true);

        /// 에러 핸들링 (파티션 6개 기준)
        factory.setConcurrency(3); // 동시 처리 스레드 수
        return factory;
    }

    @Bean(name = "commentActivityConsumerFactory")
    public ConsumerFactory<String, CommentDto.CommentActivityEvent> commentActivityConsumerFactory() {
        Map<String, Object> props = getCommonConsumerProps();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "comment-activity-consumer-group");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, CommentDto.CommentActivityEvent.class.getName());
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, CommentDto.CommentActivityEvent> commentActivityKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, CommentDto.CommentActivityEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(commentActivityConsumerFactory());

        /// At Least Once 설정
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setSyncCommits(true);

        /// 에러 핸들링 (파티션 6개 기준)
        factory.setConcurrency(3); // 동시 처리 스레드 수
        return factory;
    }

    @Bean(name = "memberActivityConsumerFactory")
    public ConsumerFactory<String, MemberDto.MemberActivity> memberActivityConsumerFactory() {
        Map<String, Object> props = getCommonConsumerProps();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "member-activity-consumer-group");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, MemberDto.MemberActivity.class.getName());
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, MemberDto.MemberActivity> memberActivityKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, MemberDto.MemberActivity> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(memberActivityConsumerFactory());

        /// At Least Once 설정
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setSyncCommits(true);

        /// 에러 핸들링 (파티션 6개 기준)
        factory.setConcurrency(3); // 동시 처리 스레드 수
        return factory;
    }

    @Bean(name = "postLikeActivityConsumerFactory")
    public ConsumerFactory<String, PostDto.LikeActivityEvent> postLikeActivityConsumerFactory() {
        Map<String, Object> props = getCommonConsumerProps();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "post-like-activity-consumer-group");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, PostDto.LikeActivityEvent.class.getName());
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PostDto.LikeActivityEvent> postLikeActivityKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, PostDto.LikeActivityEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(postLikeActivityConsumerFactory());

        /// At Least Once 설정
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setSyncCommits(true);

        /// 에러 핸들링 (파티션 6개 기준)
        factory.setConcurrency(3); // 동시 처리 스레드 수

        return factory;
    }

    @Bean(name = "commentLikeActivityConsumerFactory")
    public ConsumerFactory<String, CommentDto.LikeActivityEvent> commentLikeActivityConsumerFactory() {
        Map<String, Object> props = getCommonConsumerProps();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "comment-like-activity-consumer-group");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, CommentDto.LikeActivityEvent.class.getName());
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, CommentDto.LikeActivityEvent> commentLikeActivityKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, CommentDto.LikeActivityEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(commentLikeActivityConsumerFactory());

        /// At Least Once 설정
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setSyncCommits(true);

        /// 에러 핸들링 (파티션 6개 기준)
        factory.setConcurrency(3); // 동시 처리 스레드 수

        return factory;
    }

    /// producers ///
    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> configs = new HashMap<>();

        // 기본 설정
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // ACK = 1 (리더 브로커만 확인)
        configs.put(ProducerConfig.ACKS_CONFIG, "1");
        // 멱등성 비활성화
        configs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);

        // 성능 최적화 설정
        configs.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        configs.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        configs.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768); // 32KB
//        configs.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        // 하나의 브로커 연결에서 응답을 기다리는 동안 전송할 수 있는 최대 요청 수
//        configs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        // 재시도 설정
        configs.put(ProducerConfig.RETRIES_CONFIG, 3);
        configs.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);

        // 타임아웃 설정
        configs.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        configs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60000);

        return new DefaultKafkaProducerFactory<>(configs);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public AdminClient adminClient() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return AdminClient.create(configs);
    }
}
