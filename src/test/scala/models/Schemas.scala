package models

import org.apache.spark.sql.types.{ArrayType, DataTypes, IntegerType, LongType, StringType, StructField, StructType}

case class S_booking(database: String, table: String, ty: String,
                     bookingGuid: String, buguid: String, projGuid: String,
                     status: String, projNum: String, x_IsTPFCustomer: String, x_TPFCustomerTime: String,
                     x_IsThirdCustomer: String, x_ThirdCustomerTime: String, createdTime: String)


object Schemas {

  val SbookingSchema: StructType = StructType(
    List(
      StructField("database", DataTypes.StringType),
      StructField("table", DataTypes.StringType),
      StructField("type", DataTypes.StringType),
      StructField("data", //DataTypes.StringType),

        //        array<struct<BgnDate:string,BookingGUID:string,Bz:string,BzEnum:string,ClosePreson:string,CloseReason:string,CloseReasonEnum:string,CloseRemark:string,CloseTime:string,CreatedGUID:string,CreatedName:string,CreatedTime:string,CstAllCardID:string,CstAllName:string,EndDate:string,FullProjNum:string,IdCode:string,ModifiedGUID:string,ModifiedName:string,ModifiedTime:string,ProjGUID:string,ProjNum:string,ProjPrefix:string,Remark:string>>


        StructType(Array(
          StructField("BgnDate", DataTypes.StringType),
          StructField("buguid", DataTypes.StringType),
          StructField("projGuid", DataTypes.StringType),
          StructField("status", DataTypes.StringType),
          StructField("projNum", DataTypes.StringType),
          StructField("x_IsTPFCustomer", DataTypes.StringType),
          StructField("x_TPFCustomerTime", DataTypes.StringType),
          StructField("x_IsThirdCustomer", DataTypes.StringType),
          StructField("x_ThirdCustomerTime", DataTypes.StringType),
          StructField("createdTime", DataTypes.StringType)

        )))

    )
  )


  val sbookingSchema2: StructType = StructType(
    List(StructField("BgnDate", DataTypes.StringType)
    ))


  val sbookingSchema3 = StructType(Nil)
    .add("database", StringType)
    .add("table", StringType)
    .add("type", StringType)
    .add("data", ArrayType(
      StructType(Nil)
        .add("BookingGUID", StringType)
        .add("ProjGUID", StringType)
        .add("ProjNum", StringType)
        .add("x_IsTPFCustomer", StringType)
        .add("x_TPFCustomerTime", StringType)
        .add("x_IsThirdCustomer", StringType)
        .add("x_ThirdCustomerTime", StringType)
        .add("CreatedTime", StringType)
        .add("Status", StringType)


//        .add("BgnDate", StringType)
//        .add("buguid", StringType)





    )
    )


}


