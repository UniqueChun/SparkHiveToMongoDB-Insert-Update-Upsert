package com.soul.sparkmg;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.spark.MongoSpark;
import com.soul.utils.MongoUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.bson.Document;

/**
 * @author soulChun
 * @create 2018-12-15-16:17
 */
public class SparkHiveToMg {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("SparkHiveToMg").setMaster("local[2]");
        //如何你的密码中有@符号 请用%40代替
        conf.set("spark.mongodb.output.uri", "mongodb://root:root@127.0.0.1/soul_db.emp");
        JavaSparkContext jsc =  new JavaSparkContext(conf);
        HiveContext hc = new HiveContext(jsc);
        Dataset<Row> df  =hc.table("soul.emp");

        //直接存DF到MongoDB
//        MongoSpark.save(df);
        JavaRDD<Row> rdd = df.toJavaRDD();


        //insert
//        JavaRDD<Document> rddDoc= rdd.map(new Function<Row, Document>() {
//            public Document call(Row row) throws Exception {
//                Document doc = new Document();
//                doc.put("empno",row.get(0));
//                doc.put("ename",row.get(1));
//                doc.put("job",row.get(2));
//                doc.put("age",row.get(3));
//                doc.put("deptno",row.get(4));
//                return doc;
//            }
//        });
//        MongoSpark.save(rddDoc);

        //update
//        JavaRDD<UpdateOneModel<Document>> rddUpdate= rdd.map(new Function<Row, UpdateOneModel<Document>>() {
//            public UpdateOneModel<Document> call(Row row) throws Exception {
//                Document doc = new Document();
//                doc.put("empno",row.get(0));
//                doc.put("ename",row.get(1));
//                doc.put("job",row.get(2));
//                doc.put("age",row.get(3));
//                doc.put("deptno",row.get(4));
//                Document modifiers = new Document();
//                modifiers.put("$set",doc);
//                return new UpdateOneModel<Document>(Filters.eq("empno",doc.get("empno")),modifiers,new UpdateOptions().upsert(true));
//            }
//        });
//        MongoUtils.updateSave(rddUpdate.rdd(),rddUpdate.classTag());

        //upsert
        JavaRDD<ReplaceOneModel<Document>> rddUpsert= rdd.map(new Function<Row, ReplaceOneModel<Document>>() {
            public ReplaceOneModel<Document> call(Row row) throws Exception {
                Document doc = new Document();
                doc.put("empno",row.get(0));
                doc.put("ename",row.get(1));
                doc.put("job",row.get(2));
                doc.put("age",row.get(3));
                doc.put("deptno",row.get(4));
//                Document modifiers = new Document();
//                modifiers.put("$set",doc);
                return new ReplaceOneModel<Document>(Filters.eq("empno",doc.get("empno")),doc,new UpdateOptions().upsert(true));
            }
        });

        MongoUtils.upsertSave(rddUpsert.rdd(),rddUpsert.classTag());
        jsc.stop();
    }
}
