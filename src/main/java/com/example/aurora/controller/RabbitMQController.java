package com.example.aurora.controller;

import com.example.aurora.service.serviceImpl.RabbitSender;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.io.IOException;
import java.util.UUID;

/**
 * 测试rabbitMQ
 */
@RestController
@Slf4j
public class RabbitMQController {

    @Autowired
    private RabbitTemplate rabbitTemplate;
    @Autowired
    private RabbitSender rabbitSender;

    /**
     * @Description: 发送消息
     * 1.交换机
     * 2.key
     * 3.消息
     * 4.消息ID
     * rabbitTemplate.send(message);   发消息;参数对象为org.springframework.amqp.core.Message
     * rabbitTemplate.convertAndSend(message); 转换并发送消息;将参数对象转换为org.springframework.amqp.core.Message后发送,消费者不能有返回值
     * rabbitTemplate.convertSendAndReceive(message) //转换并发送消息,且等待消息者返回响应消息.消费者可以有返回值
     * @method: handleMessage
     * @Param: message
     * @return: void
     * @Date: 2018/11/18 21:40
     */
    @GetMapping("/directSend")
    public void directSend() {
        String message="direct 发送消息";
        rabbitSender.sendMessage("DirectExchange", "DirectKey", message);
    }

    @GetMapping("/topicSend")
    public void topicSend() {
        String message="topic 发送消息";
        rabbitSender.sendMessage("TopicExchange", "Topic.Key", message);
    }

    @GetMapping("/fanoutSend")
    public void fanoutSend() {
        String message="fanout 发送消息";
        rabbitSender.sendMessage("FanoutExchange", "", message);
    }

    @GetMapping("/headersSend")
    public void headersSend(){
        String msg="headers 发送消息";
        MessageProperties properties = new MessageProperties();
        properties.setHeader("headers1","value1");
        properties.setHeader("headers2","value2");
        Message message = new Message(msg.getBytes(),properties);
        rabbitSender.sendMessage("HeadersExchange", "", message);
    }

    /**
     * @Description: 消费消息
     * @method: handleMessage
     * @Param: message
     * @return: void
     * @Date: 2018/11/18 21:41
     */
    @RabbitListener(queues = "DirectQueue")
    @RabbitHandler
    public void directMessage(String message){
        log.info("DirectConsumer {} directMessage :"+message);
    }

    /**
     * @Description: 消费消息
     * @method: handleMessage
     * @Param: message
     * @return: void
     * @auther: LHL
     * @Date: 2018/11/18 21:41
     */
//    @RabbitListener(queues = "DirectQueue")
//    @RabbitHandler
//    public void directMessage(String sendMessage, Channel channel, Message message) throws Exception {
//        try {
//            Assert.notNull(sendMessage, "sendMessage 消息体不能为NULL");
//            log.info("处理MQ消息");
//            // prefetchCount限制每个消费者在收到下一个确认回执前一次可以最大接受多少条消息,通过basic.qos方法设置prefetch_count=1,这样RabbitMQ就会使得每个Consumer在同一个时间点最多处理一个Message
//            channel.basicQos(1);
//            log.info("DirectConsumer {} directMessage :" + message);
//            // 确认消息已经消费成功
//            channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
//        } catch (IOException e) {
//            log.error("MQ消息处理异常，消息ID：{}，消息体:{}", message.getMessageProperties().getCorrelationId(),sendMessage,e);
//            // 拒绝当前消息，并把消息返回原队列
//            channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, true);
//        }
//    }

    @RabbitListener(queues = "TopicQueue")
    @RabbitHandler
    public void topicMessage(String message){
        log.info("TopicConsumer {} topicMessage :"+message);
    }

    @RabbitListener(queues = "FanoutQueue")
    @RabbitHandler
    public void fanoutMessage(String message){
        log.info("FanoutConsumer {} fanoutMessage :"+message);
    }

    @RabbitListener(queues = "HeadersQueue")
    @RabbitHandler
    public void headersMessage(Message message){
        log.info("HeadersConsumer {} headersMessage :"+message);
    }
}
