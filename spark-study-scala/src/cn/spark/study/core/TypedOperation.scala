package cn.ibeifeng.spark

import org.apache.spark.sql.SparkSession

/**
 * typed操作
 */
object TypedOperation {
  
  case class Employee(name: String, age: Long, depId: Long, gender: String, salary: Long)
  
  def main(args: Array[String]) {
    val spark = SparkSession
        .builder()
        .appName("TypedOperation") 
        .master("local") 
        .config("spark.sql.warehouse.dir", "C:\\Users\\Administrator\\Desktop\\spark-warehouse")
        .getOrCreate()
    
    import spark.implicits._
    
    val employee = spark.read.json("C:\\Users\\Administrator\\Desktop\\employee.json")
    
    val employeeDS = employee.as[Employee]  
    
    println(employeeDS.rdd.partitions.size)
    
    // coalesce和repartition操作
    // 都是用来重新定义分区的
    // 区别在于：coalesce，只能用于减少分区数量，而且可以选择不发生shuffle
    // repartiton，可以增加分区，也可以减少分区，必须会发生shuffle，相当于是进行了一次重分区操作
    
    val employeeDSRepartitioned = employeeDS.repartition(7);
    
    // 看一下它的分区情况
    println(employeeDSRepartitioned.rdd.partitions.size)
    
    val employeeDSCoalesced = employeeDSRepartitioned.coalesce(3);
    
    println(employeeDSCoalesced.rdd.partitions.size)
    
    employeeDSCoalesced.show()
  }
  
}