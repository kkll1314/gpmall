package com.gpmall.user.kafkaConfig;

import com.gpmall.user.dal.entitys.User;
import com.gpmall.user.registerVerification.KafKaRegisterSuccConsumerFactory;
import com.gpmall.user.registerVerification.KafKaRegisterSuccProducer;
import com.gpmall.user.registerVerification.KafKaRegisterSuccProducerFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Map;

/**
 * 咕泡学员
 * Administrator
 * 2019/8/23 0023
 * 14:30
 * 这种方式可以很灵活的添加多个生产者工厂；
 * 用户针对不同的业务（主要方便与序列化方式的不同的生产者 或者 如果能统一序列化的方式也可以使用springboot autoconfig 里面提供的默认的工厂和template）
 */
@Configuration
public class KafKaConfig {
    /**该类是在spring boot autoconfig 中就初始化了**/
    @Autowired
    KafkaProperties kafkaProperties;

    /**
     * 自定义一个用于注册时发送消息的生产者工厂
     * @return
     */
    @Bean
    public KafKaRegisterSuccProducerFactory kafKaRegisterSuccProducerFactory(){
        Map<String,Object>  producerProperties = kafkaProperties.buildProducerProperties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaProperties.getBootstrapServers());
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,JsonSerializer.class);
        producerProperties.put(ProducerConfig.ACKS_CONFIG,"-1");
        return new KafKaRegisterSuccProducerFactory(producerProperties);
    }

    /**
     * 生产者就是使用KafkaTemplate进行消息的发送的，构造发送器的时候传递生产者工厂的实体就可以实现自定义的发送器
     * @return
     */
    @Bean
    public KafkaTemplate registerSuccInfoTemplate(){
        KafkaTemplate template =  new KafkaTemplate<>(kafKaRegisterSuccProducerFactory());
        //template.setDefaultTopic("user-register-succ-topic");
        return template;
    }

    /**
     * 自定义一个注册时的消费者的工厂
     * @return
     */
    @Bean
    public KafKaRegisterSuccConsumerFactory kafKaRegisterSuccConsumerFactory(){
        Map<String,Object>  consumerProperties = kafkaProperties.buildProducerProperties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaProperties.getBootstrapServers());
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG,"default");
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);//消费者的自动提交方式关闭
        // consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,10);//在一个的轮训方法中，返回的最大记录数
        consumerProperties.put("spring.json.trusted.packages" ,"com.gpmall.user.dal.entitys,com.gpmall.user.dal.*");
        /*
        earliest:当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
        latest: 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
        none: topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
        */
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        return new KafKaRegisterSuccConsumerFactory(consumerProperties);
    }

    /**
     * 批量监听的消费者工厂
     * @return
     */
   /* @Bean
    public KafKaRegisterSuccConsumerFactory kafKaRegisterSuccBatchConsumerFactory(){
        Map<String,Object>  consumerProperties = kafkaProperties.buildProducerProperties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaProperties.getBootstrapServers());
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG,"default");
        //消费者的自动提交方式关闭,这样才会启动Listener线程
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        // consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,10);//在一个的轮训方法中，返回的最大记录数
        consumerProperties.put("spring.json.trusted.packages" ,"com.gpmall.user.dal.entitys,com.gpmall.user.dal.*");
        *//*
        earliest:当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
        latest: 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
        none: topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
        *//*
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //设置每次接受Message 的数量为10
        consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,10);
        return new KafKaRegisterSuccConsumerFactory(consumerProperties);
    }*/

    /**
     * 自定义消费者的监听工厂，这里产生一个以注册时的消费者的配置为准的消息监听器
     * 用于消费注册时的消息 @kafkaListener注解可以指定监听者容器的生产工厂来使用这个自定义的监听工厂
     * @return
     */
    @Bean
    public KafkaListenerContainerFactory userRegisterSuccKafkaListenerContainerFactory(){
        ConcurrentKafkaListenerContainerFactory conFactory = new ConcurrentKafkaListenerContainerFactory<>();
        conFactory.setConsumerFactory(kafKaRegisterSuccConsumerFactory());
        conFactory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);//  设置消费者消费消息后的提交方式为手动提交
        return conFactory;
    }

  /*  @Bean
    public KafkaListenerContainerFactory userBatchRegisterSuccKafkaListenerContainerFactory(){
        ConcurrentKafkaListenerContainerFactory conFactory = new ConcurrentKafkaListenerContainerFactory<>();
        //注入支持批量的消费者工厂的配置
        conFactory.setConsumerFactory(kafKaRegisterSuccBatchConsumerFactory());
        //开启批量监听
        conFactory.setBatchListener(true);
        //30s为时间间隔拉取消息
        conFactory.getContainerProperties().setPollTimeout(30000);
        conFactory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);//  设置消费者消费消息后的提交方式为手动提交
        return conFactory;
    }*/

    /**
     * 并发消费
     * @return
     */
    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String,String>> concurrentKafkaListenerContainerFactory(){
        ConcurrentKafkaListenerContainerFactory<String,String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(kafKaRegisterSuccConsumerFactory());
        factory.setConcurrency(4);
       // factory.setBatchListener(true);
        factory.getContainerProperties().setPollTimeout(3000);
        return factory;
    }
}
