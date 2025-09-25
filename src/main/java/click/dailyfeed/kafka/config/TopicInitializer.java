package click.dailyfeed.kafka.config;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

@Slf4j
@RequiredArgsConstructor
@Component
public class TopicInitializer {
    private final KafkaAdmin kafkaAdmin;

    // todo yaml 기반으로 변경 예정
    private static final int PARTITIONS = 6;
    private static final int REPLICATION_FACTOR = 2;


    @PostConstruct
    public void initializeTopics() {
        createTodayTopicIfNotExists();
        createTomorrowTopicIfNotExists(); // 23시 이후 대비
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        // 애플리케이션 완전 구동 후 토픽 생성
        initializeTopics();
    }

    private void createTodayTopicIfNotExists() {
        String today = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        createTopicIfNotExists("post-activity-" + today);
    }

    private void createTomorrowTopicIfNotExists() {
        String tomorrow = LocalDate.now().plusDays(1)
                .format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        createTopicIfNotExists("post-activity-" + tomorrow);
    }

    private AdminClient getAdminClient() {
        Map<String, Object> configs = kafkaAdmin.getConfigurationProperties();
        return AdminClient.create(configs);
    }

    private void createTopicIfNotExists(String topicName) {
        try (AdminClient adminClient = getAdminClient()) {
            // 토픽 존재 여부 확인
            ListTopicsResult listTopics = adminClient.listTopics();
            Set<String> existingTopics = listTopics.names().get();

            if (!existingTopics.contains(topicName)) {
                NewTopic newTopic = TopicBuilder.name(topicName)
                        .partitions(PARTITIONS)
                        .replicas(REPLICATION_FACTOR)
                        .config(TopicConfig.CLEANUP_POLICY_CONFIG, "delete")
                        .config(TopicConfig.RETENTION_MS_CONFIG, "604800000")
                        .build();

                CreateTopicsResult result = adminClient.createTopics(Arrays.asList(newTopic));
                result.all().get(); // 완료 대기
                log.info("Created topic: {}", topicName);
            } else {
                log.info("Topic already exists: {}", topicName);
            }
        } catch (Exception e) {
            log.error("Failed to create topic: {}", topicName, e);
            throw new RuntimeException("Topic creation failed", e);
        }
    }
}
