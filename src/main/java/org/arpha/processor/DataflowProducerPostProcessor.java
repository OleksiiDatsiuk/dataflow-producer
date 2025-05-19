package org.arpha.processor;

import org.arpha.Producer;
import org.arpha.annotation.DataflowProducer;
import org.arpha.properties.DataflowBrokerProperties;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

@EnableConfigurationProperties({DataflowBrokerProperties.class})
public class DataflowProducerPostProcessor implements BeanPostProcessor {

    private final Producer producer;

    public DataflowProducerPostProcessor(DataflowBrokerProperties props) {
        this.producer = new Producer(props.getHost(), props.getPort());
    }

    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        Field[] fields = bean.getClass().getDeclaredFields();
        int numberOfFields = fields.length;

        for(int fieldIndex = 0; fieldIndex < numberOfFields; ++fieldIndex) {
            Field field = fields[fieldIndex];
            DataflowProducer annotation = field.getAnnotation(DataflowProducer.class);
            if (annotation != null) {
                Class<?> fieldType = field.getType();
                Object proxy = Proxy.newProxyInstance(fieldType.getClassLoader(), new Class[]{fieldType}, new ProducerInvocationHandler(this.producer, annotation.topic()));

                try {
                    field.setAccessible(true);
                    field.set(bean, proxy);
                } catch (IllegalAccessException ex) {
                    throw new RuntimeException("Failed to inject DataflowProducer proxy", ex);
                }
            }
        }

        return bean;
    }

    private static class ProducerInvocationHandler implements InvocationHandler {
        private final Producer producer;
        private final String topic;

        public ProducerInvocationHandler(Producer producer, String topic) {
            this.producer = producer;
            this.topic = topic;
        }

        public Object invoke(Object proxy, Method method, Object[] args) {
            if (args != null && args.length == 1) {
                this.producer.send(this.topic, args[0]);
            }

            return null;
        }
    }
}
