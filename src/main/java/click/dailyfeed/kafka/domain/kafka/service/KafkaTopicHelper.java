package click.dailyfeed.kafka.domain.kafka.service;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
@RequiredArgsConstructor
@Component
public class KafkaTopicHelper {
    private final KafkaAdmin kafkaAdmin;
    private final AdminClient adminClient;

    @Value("${infrastructure.kafka.default.producer.partition:6}")
    private int partitions;

    @Value("${infrastructure.kafka.default.producer.replication-factor:2}")
    private short replicationFactor;

    @Value("${infrastructure.kafka.default.producer.retention-ms:604800000}")
    private long retentionMs;

    @Value("${infrastructure.kafka.default.producer.operation-timeout-seconds:604800000}")
    private long operationTimeoutSeconds;

    // 생성된 토픽을 캐시하여 중복 생성 방지
    private final Map<String, Boolean> createdTopics = new ConcurrentHashMap<>();

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    /**
     * 토픽이 존재하지 않으면 동적으로 생성
     */
    public void createTopicIfNotExists(String topicName) {
        if (createdTopics.containsKey(topicName)) {
            log.debug("Topic {} already created in cache", topicName);
            return;
        }

        try {
            // 토픽 존재 여부 확인
            if (isTopicExists(topicName)) {
                createdTopics.put(topicName, true);
                log.info("Topic {} already exists", topicName);
                return;
            }

            // 토픽 생성
            NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);

            // 토픽 설정 추가
            newTopic.configs(Map.of(
                    "retention.ms", String.valueOf(retentionMs),
                    "cleanup.policy", "delete",
                    "compression.type", "lz4"
            ));

            kafkaAdmin.createOrModifyTopics(newTopic);
            createdTopics.put(topicName, true);

            log.info("Successfully created topic: {} with {} partitions",
                    topicName, partitions);

        } catch (Exception e) {
            log.error("Failed to create topic: {}", topicName, e);
            throw new RuntimeException("Topic creation failed: " + topicName, e);
        }
    }

    /**
     * 토픽 존재 여부 확인
     */
    private boolean isTopicExists(String topicName) {
        try {
            DescribeTopicsResult result = adminClient.describeTopics(Collections.singletonList(topicName));
            Map<String, TopicDescription> topicDescriptions =
                    result.allTopicNames().get(operationTimeoutSeconds, TimeUnit.SECONDS);
            return topicDescriptions.containsKey(topicName);
        } catch (Exception e) {
            log.debug("Topic {} does not exist or error occurred: {}", topicName, e.getMessage());
            return false;
        }
    }

    /**
     * 여러 토픽의 존재 여부를 한 번에 확인
     */
    public Map<String, Boolean> checkTopicsExist(Collection<String> topicNames) {
        Map<String, Boolean> results = new HashMap<>();

        try {
            DescribeTopicsResult result = adminClient.describeTopics(topicNames);
            Map<String, TopicDescription> topicDescriptions =
                    result.allTopicNames().get(operationTimeoutSeconds, TimeUnit.SECONDS);

            for (String topicName : topicNames) {
                results.put(topicName, topicDescriptions.containsKey(topicName));
            }
        } catch (Exception e) {
            log.error("Error checking topics existence: {}", e.getMessage());
            // 에러 발생 시 모든 토픽을 존재하지 않는 것으로 처리
            for (String topicName : topicNames) {
                results.put(topicName, false);
            }
        }

        return results;
    }

    /**
     * 토픽 상세 정보 조회
     */
    public Optional<TopicDescription> getTopicDescription(String topicName) {
        try {
            DescribeTopicsResult result = adminClient.describeTopics(Collections.singletonList(topicName));
            Map<String, TopicDescription> descriptions =
                    result.allTopicNames().get(operationTimeoutSeconds, TimeUnit.SECONDS);
            return Optional.ofNullable(descriptions.get(topicName));
        } catch (Exception e) {
            log.error("Failed to get topic description for {}: {}", topicName, e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * 모든 토픽 목록 조회
     */
    public Set<String> listAllTopics() {
        try {
            return adminClient.listTopics().names().get(operationTimeoutSeconds, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("Failed to list topics: {}", e.getMessage());
            return Collections.emptySet();
        }
    }

    /**
     * post-activity prefix로 시작하는 토픽들만 조회
     */
    public Set<String> listPostActivityTopics(String topicPrefix) {
        return listAllTopics().stream()
                .filter(topic -> topic.startsWith(topicPrefix))
                .collect(java.util.stream.Collectors.toSet());
    }

    /**
     * 캐시 초기화 (테스트용)
     */
    public void clearCache() {
        createdTopics.clear();
    }

    /**
     * 설정 정보 조회용 메서드들
     */

    public int getPartitions() {
        return partitions;
    }

    public short getReplicationFactor() {
        return replicationFactor;
    }

    public long getRetentionMs() {
        return retentionMs;
    }

    public long getOperationTimeoutSeconds() {
        return operationTimeoutSeconds;
    }
}
