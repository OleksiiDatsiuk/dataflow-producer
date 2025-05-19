package org.arpha.processor;

import org.arpha.annotation.DataflowProducer;
import org.arpha.annotation.EnableDataflowProducers;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import java.util.Map;
import java.util.Set;

public class DataflowProducerRegistrar implements ImportBeanDefinitionRegistrar {

    @Override
    public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
        Map<String, Object> annotationAttributes = importingClassMetadata.getAnnotationAttributes(EnableDataflowProducers.class.getName());
        String[] basePackages = (String[]) annotationAttributes.get("basePackages");

        if (basePackages == null || basePackages.length == 0) {
            String basePackage = ClassUtils.getPackageName(importingClassMetadata.getClassName());
            basePackages = new String[]{basePackage};
        }


        ClassPathScanningCandidateComponentProvider scanner = new ClassPathScanningCandidateComponentProvider(false) {
            @Override
            protected boolean isCandidateComponent(AnnotatedBeanDefinition beanDefinition) {
                return beanDefinition.getMetadata().isInterface() && beanDefinition.getMetadata().isIndependent();
            }
        };
        scanner.addIncludeFilter(new AnnotationTypeFilter(DataflowProducer.class));

        for (String pkg : basePackages) {
            Set<BeanDefinition> candidateComponents = scanner.findCandidateComponents(pkg);
            for (BeanDefinition candidateComponent : candidateComponents) {
                if (candidateComponent instanceof AnnotatedBeanDefinition) {
                    AnnotatedBeanDefinition beanDefinition = (AnnotatedBeanDefinition) candidateComponent;
                    AnnotationMetadata annotationMetadata = beanDefinition.getMetadata();
                    Map<String, Object> attributes = annotationMetadata.getAnnotationAttributes(DataflowProducer.class.getCanonicalName());

                    String beanClassName = beanDefinition.getBeanClassName();
                    Class<?> producerInterface;
                    try {
                        producerInterface = Class.forName(beanClassName);
                    } catch (ClassNotFoundException e) {
                        throw new IllegalStateException("Could not load class for DataflowProducer interface: " + beanClassName, e);
                    }

                    BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(ProducerFactoryBean.class);
                    builder.addConstructorArgValue(producerInterface);
                    builder.setAutowireMode(AbstractBeanDefinition.AUTOWIRE_BY_TYPE);

                    String beanName = determineBeanName(attributes, producerInterface);

                    registry.registerBeanDefinition(beanName, builder.getBeanDefinition());
                }
            }
        }
    }

    private String determineBeanName(Map<String, Object> attributes, Class<?> interfaceType) {
        String explicitBeanName = (attributes != null) ? (String) attributes.get("beanName") : null;
        if (StringUtils.hasText(explicitBeanName)) {
            return explicitBeanName;
        }
        return StringUtils.uncapitalize(interfaceType.getSimpleName());
    }

}
