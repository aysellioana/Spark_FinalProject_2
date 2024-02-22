import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec

trait AppTest extends AnyFlatSpec with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .appName("Retweet")
      .master("local[*]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      spark.stop()
    } finally {
      super.afterAll()
    }
  }

  "Read and display data from Avro file" should "work correctly" in {
    val avroPath = "src/main/resources/user_dir_data.avro"
    val userDir: DataFrame = spark.read.format("avro")
      .load(avroPath)

    userDir.show()

    assert(userDir.columns.contains("USER_ID"))
    assert(userDir.columns.contains("FIRST_NAME"))
    assert(userDir.columns.contains("LAST_NAME"))
  }

}
