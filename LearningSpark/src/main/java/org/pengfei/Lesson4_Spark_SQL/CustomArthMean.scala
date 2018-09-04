package org.pengfei.Lesson4_Spark_SQL

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


/*
* This class extends UserDefinedAggregatedFunction to write custom arthmetic mean as aggregate function
* You can also specify any constructor arguments. For instance you can have CustomMean(arg1: Int, arg2: String)
* In this example, we just use an empty constructor
* */


class CustomArthMean extends UserDefinedAggregateFunction{
  //Define input data type schema, it's the data type of your column, in our case, we choose double for all numbers
  override def inputSchema: StructType = StructType(Array(StructField("item", DoubleType)))

  // intermediate value schema, it can be different from the input value. in our case, as we calculate mean, we need to
  // do sum first, then count the size of all number list. So we need sum:Double, count:Long
  override def bufferSchema: StructType = StructType(Array(
    StructField("sum",DoubleType),
    StructField("count",LongType)
  ))

  //Return type of the function
  override def dataType: DataType = DoubleType

  // The function is deterministic
  override def deterministic: Boolean = true

  // This function is called whenever key changes, we need to reset sum and count to 0 for each group in the groupBy
  // buffer(0) is for storing intermediate value of sum, buffer(1) is for count
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=0.toDouble
    buffer(1)=0L
  }
//Iterate over each entry of a group
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //aggregate the sum with input
    buffer(0)=buffer.getDouble(0)+input.getDouble(0)
    // increase count to 1
    buffer(1)=buffer.getLong(1)+1
  }
/*Merge two partial aggregates, buffer1 is the new intermediate aggregator, buffer2 is the result of each group update
* return value. We merge all these return values to buffer1*/
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0)=buffer1.getDouble(0)+buffer2.getDouble(0)
    buffer1(1)=buffer1.getLong(1)+buffer2.getLong(1)
  }
// called after all the entries has been visited. It returns the final output of the aggregate function. In our case,
// we use sum/count to return the mean.
  override def evaluate(buffer: Row): Any = {
    buffer.getDouble(0)/buffer.getLong(1)
  }
}
