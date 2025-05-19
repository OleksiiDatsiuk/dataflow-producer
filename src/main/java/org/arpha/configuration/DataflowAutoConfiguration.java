package org.arpha.configuration;

import org.arpha.Producer;
import org.arpha.properties.DataflowBrokerProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnClass(Producer.class)
@EnableConfigurationProperties(DataflowBrokerProperties.class)
public class DataflowAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public Producer dataflowProducerCore(DataflowBrokerProperties props) {
        if (props.getHost() == null || props.getPort() == 0) {
            throw new IllegalStateException("Dataflow broker host and port must be configured.");
        }
        return new Producer(props.getHost(), props.getPort());
    }

}
