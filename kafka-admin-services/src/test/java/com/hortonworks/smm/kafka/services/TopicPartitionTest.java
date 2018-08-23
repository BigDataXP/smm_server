package com.hortonworks.smm.kafka.services;

import com.hortonworks.smm.kafka.services.management.dtos.TopicPartition;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Random;

public class TopicPartitionTest {

    @Test
    public void testTopicPartition() {
        Assertions.assertThrows(IllegalArgumentException.class,
                                () -> { TopicPartition topicPartition = new TopicPartition("", 1);});
        Assertions.assertThrows(NullPointerException.class,
                                () -> { TopicPartition topicPartition = new TopicPartition(null, 1);});

        Assertions.assertThrows(IllegalArgumentException.class,
                                () -> { TopicPartition topicPartition = new TopicPartition("foo", -1);});

        int partition = Math.abs(new Random().nextInt(Integer.MAX_VALUE));
        TopicPartition topicPartition = new TopicPartition("foo", partition);
        TopicPartition result = TopicPartition.from(topicPartition.toString());

        Assertions.assertEquals(topicPartition, result);
    }
}
