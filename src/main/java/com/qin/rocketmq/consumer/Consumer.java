package com.qin.rocketmq.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.nio.charset.Charset;
import java.util.List;

/**
 * 消费者
 *
 * @author qcb
 * @date 2022/07/10 16:01.
 */
public class Consumer {

    public static void main(String[] args) throws Exception{
        //实例化消费者实例
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test_consumer_group");
        //定义name server地址
        consumer.setNamesrvAddr("192.168.3.99:9876;192.168.3.100:9876");
        //订阅主题
        consumer.subscribe("TopicTest","*");
//        consumer.subscribe("TestTopic","*");
        //注册消费者回调
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                for(int i=0;i<msgs.size();i++){
                    System.out.println("Receive Message Content: " + new String(msgs.get(i).getBody(), Charset.forName(RemotingHelper.DEFAULT_CHARSET)));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();

        System.out.printf("Consumer Started.%n");
    }
}
