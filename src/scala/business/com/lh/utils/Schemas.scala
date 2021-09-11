package com.lh.utils

import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

object Schemas {

  val renchouSchema = StructType(Nil)
    .add("database", StringType)
    .add("table", StringType)
    .add("type", StringType)
    .add("es", StringType)
    .add("data", ArrayType(
      StructType(Nil)
        .add("BookingGUID", StringType)
        .add("ProjGUID", StringType)
        .add("ProjNum", StringType)
        .add("ProjName", StringType)
        .add("x_IsTPFCustomer", StringType)
        .add("x_TPFCustomerTime", StringType)
        .add("x_IsThirdCustomer", StringType)
        .add("x_ThirdCustomerTime", StringType)
        .add("CreatedTime", StringType)
        .add("Status", StringType)
    )
    )
}
