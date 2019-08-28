package com.test.spark.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

@Component
public class RddDemo {

    private static final Logger logger = LoggerFactory.getLogger(RddDemo.class);

    @Autowired
    private transient JavaSparkContext context;
    private transient String PATH = "hdfs://192.168.234.128:50070/group/bigdata/";
    private transient ExecutorService service = Executors.newFixedThreadPool(3);

    public void RDDOperate(){
        HiveContext hiveContext = new HiveContext(context);
        //获取HDFS的配置
        Configuration conf = context.hadoopConfiguration();
        try {
            FileSystem hdfs = FileSystem.get(URI.create(PATH),conf);
            FileStatus[] fileStatuses = hdfs.listStatus(new Path(PATH));
            for(FileStatus f: fileStatuses) {
                if(f.isFile()) {
                    JavaRDD<String> rdd = context.textFile(f.getPath().toString());
                    List<String> lines = rdd.collect();
                    for (String line: lines) {
                        Callable<DataFrame> r = () -> hiveContext.sql(line);
                        logger.info("Thread starts: ");
                        Future<DataFrame> result = service.submit(r);
                        DataFrame df = result.get();
                        logger.info(df.toString());
                    }
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        finally {
            service.shutdown();
        }
    }

}
