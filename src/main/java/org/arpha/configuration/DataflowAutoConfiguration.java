package org.arpha.configuration;

import org.arpha.Producer;
import org.arpha.processor.DataflowProducerPostProcessor;
import org.arpha.properties.DataflowBrokerProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnClass({Producer.class})
@EnableConfigurationProperties({DataflowBrokerProperties.class})
public class DataflowAutoConfiguration {

    @Bean
    public DataflowProducerPostProcessor dataflowProducerPostProcessor(DataflowBrokerProperties props) {
        return new DataflowProducerPostProcessor(props);
    }

}
