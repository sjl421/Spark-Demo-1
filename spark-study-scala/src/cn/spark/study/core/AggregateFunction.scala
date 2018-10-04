package cn.ibeifeng.spark

import org.apache.spark.sql.SparkSession

object AggregateFunction {
  
  def main(args: Array[String]) {
    val spark = SparkSession
        .builder()
        .appName("AggregateFunction") 
        .master("local") 
        .config("spark.sql.warehouse.dir", "C:\\Users\\Administrator\\Desktop\\spark-warehouse")
        .getOrCreate()
    
    import spark.implicits._
    import org.apache.spark.sql.functions._
    
    val employee = spark.read.json("C:\\Users\\Administrator\\Desktop\\employee.json")
    val department = spark.read.json("C:\\Users\\Administrator\\Desktop\\department.json")
    
    employee
        .join(department, $"depId" === $"id")  
        .groupBy(department("name"))
        .agg(avg(employee("salary")), sum(employee("salary")), max(employee("salary")), min(employee("salary")), count(employee("name")), countDistinct(employee("name")))
        .show()
  }
  
}