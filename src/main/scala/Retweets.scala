import org.apache.avro.Schema
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.io.File

object Retweets {
  case class UserDir(USER_ID: Int, FIRST_NAME: String, LAST_NAME: String)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .appName("Retweet")
      .master("local[*]")
      .getOrCreate()

    //USER DIR
    val schemaUserDir = new Schema.Parser().parse(new File("src/main/resources/user_dir.avsc"))
    //println(schemaUserDir)

    //Building Spark schema (StructType) from Avro schema
    val sparkSchemaUserDir = schemaUserDir.getFields().toArray.map { field =>
      val fieldName = field.asInstanceOf[Schema.Field].name()
      val fieldType = field.asInstanceOf[Schema.Field].schema().getType match {
        case Schema.Type.INT => IntegerType
        case Schema.Type.STRING => StringType
        case _ => StringType
      }
      StructField(fieldName, fieldType, nullable = true)
    }
    val structTypeUserDir = StructType(sparkSchemaUserDir)

    val userDirJson = spark.read
      .format("json")
      .schema(structTypeUserDir)
      .load("src/main/resources/user_dir_data.json")
      .na.drop()
//    userDirJson.write.format("avro").save("src/main/resources/user_dir_data.avro")

    val userDir = spark.read.format("avro")
      .option("avroSchema", schemaUserDir.toString)
      .load("src/main/resources/user_dir_data.avro")
    userDir.show()


    //MESSAGE DIR
    val schemaMessageDir = new Schema.Parser().parse(new File("src/main/resources/message_dir.avsc"))
    //println(schemaMessageDir)

    val sparkSchemaMessageDir = schemaMessageDir.getFields().toArray.map { field =>
      val fieldName = field.asInstanceOf[Schema.Field].name()
      val fieldType = field.asInstanceOf[Schema.Field].schema().getType match {
        case Schema.Type.INT => IntegerType
        case Schema.Type.STRING => StringType
        case _ => StringType
      }
      StructField(fieldName, fieldType, nullable = true)
    }
    val structTypeMessageDir = StructType(sparkSchemaMessageDir)

    val messageDirJson = spark.read
      .format("json")
      .schema(structTypeMessageDir)
      .load("src/main/resources/message_dir_data.json")
      .na.drop()
    //messageDirJson.show()
    //messageDirJson.write.format("avro").save("src/main/resources/message_dir_data.avro")
    val messageDir = spark.read
      .format("avro")
      .option("avroSchema", schemaMessageDir.toString)
      .load("src/main/resources/message_dir_data.avro")
    messageDir.show()


    //MESSAGE
    val schemaMessage = new Schema.Parser().parse(new File("src/main/resources/message.avsc"))
//    println(schemaMessage)
    //    messageDir.write.format("avro").save("src/main/resources/message_dir_data.avro")

    val sparkSchemaMessage = schemaMessage.getFields().toArray.map { field =>
      val fieldName = field.asInstanceOf[Schema.Field].name()
      val fieldType = field.asInstanceOf[Schema.Field].schema().getType match {
        case Schema.Type.INT => IntegerType
        case Schema.Type.STRING => StringType
        case _ => StringType
      }
      StructField(fieldName, fieldType, nullable = true)
    }
    val structTypeMessage = StructType(sparkSchemaMessage)

    val messageJson = spark.read
      .format("json")
      .schema(structTypeMessage)
      .load("src/main/resources/message_data.json")
      .na.drop()
    messageJson.show()
    //messageJson.write.format("avro").save("src/main/resources/message_data.avro")

    val message  =spark.read
      .format("avro")
      .option("avroSchema", schemaMessage.toString)
      .load("src/main/resources/message_data.avro")
    message.show()


    val schemaRetweet = new Schema.Parser().parse(new File("src/main/resources/retweet.avsc"))
    //println(schemaRetweet)

    val sparkSchemaRetweet = schemaRetweet.getFields().toArray.map {field =>
      val fieldName = field.asInstanceOf[Schema.Field].name()
      val fieldType = field.asInstanceOf[Schema.Field].schema().getType match {
        case Schema.Type.INT => IntegerType
        case Schema.Type.STRING => StringType
        case _ => StringType
      }
      StructField(fieldName, fieldType, nullable = true)
    }
    val structTypeRetweet = StructType(sparkSchemaRetweet)

    val retweetJson = spark.read
      .format("json")
      .schema(structTypeRetweet)
      .load("src/main/resources/retweet_data.json")
      .na.drop()
    //retweetJson.show()

    //retweetJson.write.format("avro").save(("src/main/resources/retweet_data.avro"))
    val retweet = spark.read
      .format("avro")
      .option("avroSchema", schemaRetweet.toString)
      .load("src/main/resources/retweet_data.avro")
    retweet.show()


    val firstTweet = retweet.join(messageDir, retweet("MESSAGE_ID") === messageDir("MESSAGE_ID"))
      .join(userDir, retweet("USER_ID") === userDir("USER_ID"))
      .groupBy(retweet("USER_ID"), userDir("FIRST_NAME"), userDir("LAST_NAME"))
      .agg(count("*").alias("first_wave_retweets")).orderBy("USER_ID")
    firstTweet.show()

    val secondRetweet = retweet.alias("r1")
      .join(retweet.alias("r2"), col("r1.SUBSCRIBER_ID") === col("r2.USER_ID"))
      .join(messageDir, col("r1.MESSAGE_ID") === messageDir("MESSAGE_ID"))
      .join(userDir, col("r1.USER_ID") === userDir("USER_ID"))
      .groupBy(col("r1.USER_ID"), userDir("FIRST_NAME"), userDir("LAST_NAME"))
      .agg(count("*").alias("second_wave_retweets"))
    secondRetweet.show()

    val messageJoin = message.join(messageDir, Seq("MESSAGE_ID"))

    val total = secondRetweet.join(messageJoin, Seq("USER_ID"))
      .select("USER_ID", "FIRST_NAME", "LAST_NAME", "MESSAGE_ID" , "TEXT","second_wave_retweets")
      .orderBy(desc("second_wave_retweets"))

    total.show()

  }


}
