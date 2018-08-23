package com.hortonworks.smm.kafka.services.extension;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@TestInstance(PER_CLASS)
@ExtendWith(KafkaAdminServiceExtension.class)
public @interface KafkaAdminServiceTest {
    int numBrokerNodes() default 0;
}