package click.dailyfeed.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.serializer.DeserializationException;

@Slf4j
public class CustomErrorHandler implements CommonErrorHandler {

    @Override
    public boolean handleOne(Exception thrownException, ConsumerRecord<?, ?> record, Consumer<?, ?> consumer, MessageListenerContainer container) {
        log.error("Error processing message from topic: {}, partition: {}, offset: {}",
                record.topic(), record.partition(), record.offset(), thrownException);

        if (thrownException instanceof DeserializationException) {
            // Deserialization 에러는 skip
            log.error("Skipping message due to deserialization error");
            return true;
        }

        // 재시도 가능한 에러는 false 반환
        return false;
    }

}