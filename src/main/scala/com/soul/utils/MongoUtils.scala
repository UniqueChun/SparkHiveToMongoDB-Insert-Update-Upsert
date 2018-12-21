package com.soul.utils

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.{ReplaceOneModel, UpdateOneModel}
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.rdd.RDD
import org.bson.Document
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
  * @author soulChun
  * @create 2018-12-18-20:37
  */
object MongoUtils {


  val DefaultMaxBatchSize = 100000

  def updateSave[D: ClassTag](rdd: RDD[UpdateOneModel[Document]]): Unit = updateSave(rdd, WriteConfig(rdd.sparkContext))

  def updateSave[D: ClassTag](rdd: RDD[UpdateOneModel[D]], writeConfig: WriteConfig): Unit = {
    val mongoConnector = MongoConnector(writeConfig.asOptions)
    rdd.foreachPartition(iter => if (iter.nonEmpty) {
      mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[D] =>
        iter.grouped(DefaultMaxBatchSize).foreach(batch => collection.bulkWrite(batch.toList.asJava))
      })
    })
  }


  def upsertSave[D: ClassTag](rdd: RDD[ReplaceOneModel[Document]]): Unit = upsertSave(rdd, WriteConfig(rdd.sparkContext))

  def upsertSave[D: ClassTag](rdd: RDD[ReplaceOneModel[D]], writeConfig: WriteConfig): Unit = {
    val mongoConnector = MongoConnector(writeConfig.asOptions)
    rdd.foreachPartition(iter => if (iter.nonEmpty) {
      mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[D] =>
        iter.grouped(DefaultMaxBatchSize).foreach(batch => collection.bulkWrite(batch.toList.asJava))
      })
    })
  }


}
