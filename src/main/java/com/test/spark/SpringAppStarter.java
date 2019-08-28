package com.test.spark;

import com.test.spark.demo.RddDemo;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class SpringAppStarter implements ApplicationContextAware {

//    @Autowired
//    private WordCount wordCount;
    @Autowired
    private transient RddDemo rddDemo;
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        rddDemo.RDDOperate();
    }
}
