package org.arpha.annotation;

import org.arpha.processor.DataflowProducerRegistrar;
import org.springframework.context.annotation.Import;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(DataflowProducerRegistrar.class)
public @interface EnableDataflowProducers {
    String[] basePackages() default {};
}