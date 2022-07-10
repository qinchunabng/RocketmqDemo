package com.qin.rocketmq.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 异步生产者
 *
 * @author qcb
 * @date 2022/07/10 16:14.
 */
public class AsyncProducer {

    public static void main(String[] args) throws Exception{
        //实例化生产者
        DefaultMQProducer producer = new DefaultMQProducer("async_producer_group");
        //设置name server地址
        producer.setNamesrvAddr("192.168.3.99:9876;192.168.3.100:9876");
        //设置发送超时时间
        producer.setSendMsgTimeout(60000);
        //发送失败重试次数
        producer.setRetryTimesWhenSendAsyncFailed(0);
        producer.start();
        int messageCount = 100;
        CountDownLatch countDownLatch = new CountDownLatch(messageCount);
        for (int i = 0; i < messageCount; i++) {
            final int index = i;
            Message msg = new Message("TestTopic", "TagA", "OrderID188", ("Hello World " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            producer.send(msg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    countDownLatch.countDown();
                    System.out.printf("%-10d OK %s %n", index, sendResult.getMsgId());
                }

                @Override
                public void onException(Throwable e) {
                    countDownLatch.countDown();
                    System.out.printf("%-10d Exception %s %n", index, e);
                }
            });
            TimeUnit.SECONDS.sleep(5);
        }

        countDownLatch.await();
        producer.shutdown();
    }
}
