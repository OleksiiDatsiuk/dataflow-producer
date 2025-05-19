package org.arpha.processor;

import org.arpha.Producer;
import org.arpha.annotation.DataflowProducer;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class ProducerFactoryBean<T> implements FactoryBean<T>, ApplicationContextAware {

    private Class<T> producerInterface;
    private ApplicationContext applicationContext;
    private Producer producerInstance;

    public ProducerFactoryBean(Class<T> producerInterface) {
        this.producerInterface = producerInterface;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    private Producer getProducer() {
        if (this.producerInstance == null) {
            this.producerInstance = applicationContext.getBean(Producer.class);
        }
        return this.producerInstance;
    }

    @Override
    public T getObject() throws Exception {
        DataflowProducer annotation = producerInterface.getAnnotation(DataflowProducer.class);
        if (annotation == null) {
            throw new IllegalStateException("Interface " + producerInterface.getName() + " must be annotated with @DataflowProducer");
        }
        String topic = annotation.topic();

        return (T) Proxy.newProxyInstance(
                producerInterface.getClassLoader(),
                new Class<?>[]{producerInterface},
                new ProducerInvocationHandler(getProducer(), topic)
        );
    }

    @Override
    public Class<?> getObjectType() {
        return producerInterface;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    private static class ProducerInvocationHandler implements InvocationHandler {
        private final Producer producer;
        private final String topic;

        public ProducerInvocationHandler(Producer producer, String topic) {
            this.producer = producer;
            this.topic = topic;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.getDeclaringClass().equals(Object.class)) {
                return method.invoke(this, args);
            }

            if (args != null && args.length > 0) {
                this.producer.send(this.topic, args[0]);
            } else if (method.getParameterCount() == 0 && method.getReturnType().equals(void.class)) {
                throw new IllegalArgumentException("Method " + method.getName() + " requires an argument to publish.");
            }
            return null;
        }
    }

}
