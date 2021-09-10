package com.lh.utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

object RdbmsUtils {

  object mysql {

    var TidbjdbcMaps = Map("url" -> "jdbc:mysql://10.31.98.187:3306/ods_crm2?useSSL=false",
      "user" -> "odsadmin",
      "password" -> "dCc0r1y3mhseuHyX",
      "driver" -> "com.mysql.jdbc.Driver")


    //  import sqlContext.implicits._

    //要使用updateStateByKey方法，必须设置Checkpoint。
    //ssc.checkpoint("C:/tmp/checkPointPath")   10.31.98.187:3306


    def getDataFromTable(tableName: String, cols: String*): DataFrame = {
      TidbjdbcMaps = Map("url" -> "jdbc:mysql://10.31.98.187:3306/ods_crm2?useSSL=false",
        "user" -> "odsadmin",
        "password" -> "dCc0r1y3mhseuHyX",
        "dbtable" -> tableName,
        "driver" -> "com.mysql.jdbc.Driver")
      val sqlContext = SparkUtils.getSQLContextInstance()
      val df = sqlContext.read.options(TidbjdbcMaps).format("jdbc").load


      if (cols.length > 0) {
        val columns = cols.map(df.col(_))
        df.select(columns: _*).createOrReplaceTempView(tableName) //columns：_*  把列铺开
      }
      else {
        df.createOrReplaceTempView(tableName)
      }
      sqlContext.sql("select *  FROM  %S".format(tableName))
    }


    def getDataFromTable(sc: SparkContext, tableName: String, cols: String*): DataFrame = {
      TidbjdbcMaps = Map("url" -> "jdbc:mysql://10.31.98.187:3306/ods_crm2?useSSL=false",
        "user" -> "odsadmin",
        "password" -> "dCc0r1y3mhseuHyX",
        "dbtable" -> tableName,
        "driver" -> "com.mysql.jdbc.Driver")

      var sqlSc = SparkUtils.getSQLContextInstance(sc)
      val df = sqlSc.read.options(TidbjdbcMaps).format("jdbc").load


      if (cols.length > 0) {
        val columns = cols.map(df.col(_))
        df.select(columns: _*).createOrReplaceTempView(tableName) //columns：_*  把列铺开
      }
      else {
        df.createOrReplaceTempView(tableName)
      }

      sqlSc.sql("select *  FROM  %S".format(tableName))
    }

    def main(args: Array[String]): Unit = {
      //    getDataFromTable("P_PROJECT",List("p_projectId","parentguid")).show(10)
      getDataFromTable("P_PROJECT", "p_projectId", "parentguid").persist(StorageLevel.MEMORY_ONLY).show(10)

      //    val message = "{\"id\":0,\"database\":\"ods_crm2\",\"table\":\"s_booking\",\"pkNames\":[\"BookingGUID\"],\"isDdl\":false,\"type\":\"INSERT\",\"es\":1630550721824,\"ts\":0,\"sql\":\"\",\"sqlType\":{\"BgnDate\":93,\"BookingGUID\":12,\"Bz\":12,\"BzEnum\":-5,\"ClosePreson\":12,\"CloseReason\":12,\"CloseReasonEnum\":-5,\"CloseRemark\":12,\"CloseTime\":93,\"CreatedGUID\":12,\"CreatedName\":12,\"CreatedTime\":93,\"CstAllCardID\":12,\"CstAllName\":12,\"EndDate\":93,\"FullProjNum\":12,\"IdCode\":12,\"ModifiedGUID\":12,\"ModifiedName\":12,\"ModifiedTime\":93,\"ProjGUID\":12,\"ProjNum\":12,\"ProjPrefix\":12,\"Remark\":12,\"RoomGUIDs\":12,\"RoomNames\":12,\"SjNum\":-5,\"Status\":12,\"StatusEnum\":-5,\"TradeGUID\":12,\"YsAmount\":3,\"Zygw\":12,\"ZygwGUID\":12,\"createTime\":93,\"id\":3,\"is_deleted\":-5,\"is_testdata\":-5,\"updateTime\":93,\"versionNumber\":3,\"versionNumber_add\":93,\"x_ActivityID\":-5,\"x_BackType\":12,\"x_BackTypeEnum\":-5,\"x_DiscountRemark\":12,\"x_EcommerceD\":3,\"x_EcommercePayTime\":93,\"x_EcommercePosID\":12,\"x_EcommerceZk\":3,\"x_IsSend\":-5,\"x_IsTPFCustomer\":-5,\"x_IsThirdCustomer\":-5,\"x_IsUseEcommerce\":-5,\"x_TPFCustomerTime\":93,\"x_ThirdCustomerTime\":93,\"x_TransferCode\":12,\"x_XmFqGUID\":12,\"x_ZygwAllGUID\":12},\"mysqlType\":{\"BgnDate\":\"datetime\",\"BookingGUID\":\"varchar\",\"Bz\":\"varchar\",\"BzEnum\":\"int\",\"ClosePreson\":\"varchar\",\"CloseReason\":\"varchar\",\"CloseReasonEnum\":\"int\",\"CloseRemark\":\"mediumtext\",\"CloseTime\":\"datetime\",\"CreatedGUID\":\"varchar\",\"CreatedName\":\"varchar\",\"CreatedTime\":\"datetime\",\"CstAllCardID\":\"varchar\",\"CstAllName\":\"varchar\",\"EndDate\":\"datetime\",\"FullProjNum\":\"varchar\",\"IdCode\":\"varchar\",\"ModifiedGUID\":\"varchar\",\"ModifiedName\":\"varchar\",\"ModifiedTime\":\"datetime\",\"ProjGUID\":\"varchar\",\"ProjNum\":\"varchar\",\"ProjPrefix\":\"varchar\",\"Remark\":\"mediumtext\",\"RoomGUIDs\":\"varchar\",\"RoomNames\":\"varchar\",\"SjNum\":\"int\",\"Status\":\"varchar\",\"StatusEnum\":\"int\",\"TradeGUID\":\"varchar\",\"YsAmount\":\"decimal\",\"Zygw\":\"varchar\",\"ZygwGUID\":\"varchar\",\"createTime\":\"datetime\",\"id\":\"bigint\",\"is_deleted\":\"int\",\"is_testdata\":\"int\",\"updateTime\":\"datetime\",\"versionNumber\":\"bigint\",\"versionNumber_add\":\"datetime\",\"x_ActivityID\":\"int\",\"x_BackType\":\"mediumtext\",\"x_BackTypeEnum\":\"int\",\"x_DiscountRemark\":\"varchar\",\"x_EcommerceD\":\"decimal\",\"x_EcommercePayTime\":\"datetime\",\"x_EcommercePosID\":\"mediumtext\",\"x_EcommerceZk\":\"decimal\",\"x_IsSend\":\"int\",\"x_IsTPFCustomer\":\"int\",\"x_IsThirdCustomer\":\"int\",\"x_IsUseEcommerce\":\"int\",\"x_TPFCustomerTime\":\"datetime\",\"x_ThirdCustomerTime\":\"datetime\",\"x_TransferCode\":\"varchar\",\"x_XmFqGUID\":\"varchar\",\"x_ZygwAllGUID\":\"mediumtext\"},\"data\":[{\"BgnDate\":\"2021-09-02 10:44:36\",\"BookingGUID\":\"111\",\"Bz\":null,\"BzEnum\":null,\"ClosePreson\":null,\"CloseReason\":null,\"CloseReasonEnum\":null,\"CloseRemark\":null,\"CloseTime\":null,\"CreatedGUID\":\"222\",\"CreatedName\":null,\"CreatedTime\":\"2021-09-02 10:44:45\",\"CstAllCardID\":null,\"CstAllName\":null,\"EndDate\":\"2021-09-02 10:44:30\",\"FullProjNum\":null,\"IdCode\":null,\"ModifiedGUID\":\"333\",\"ModifiedName\":null,\"ModifiedTime\":\"2021-09-02 10:45:08\",\"ProjGUID\":\"444\",\"ProjNum\":\"22\",\"ProjPrefix\":null,\"Remark\":null,\"RoomGUIDs\":null,\"RoomNames\":null,\"SjNum\":\"1\",\"Status\":null,\"StatusEnum\":null,\"TradeGUID\":null,\"YsAmount\":null,\"Zygw\":null,\"ZygwGUID\":null,\"createTime\":\"2021-09-02 10:45:21.812\",\"id\":null,\"is_deleted\":\"0\",\"is_testdata\":\"0\",\"updateTime\":\"2021-09-02 10:45:21.812\",\"versionNumber\":\"0\",\"versionNumber_add\":\"2021-09-02 10:45:21\",\"x_ActivityID\":null,\"x_BackType\":null,\"x_BackTypeEnum\":null,\"x_DiscountRemark\":null,\"x_EcommerceD\":null,\"x_EcommercePayTime\":null,\"x_EcommercePosID\":null,\"x_EcommerceZk\":null,\"x_IsSend\":\"0\",\"x_IsTPFCustomer\":null,\"x_IsThirdCustomer\":null,\"x_IsUseEcommerce\":null,\"x_TPFCustomerTime\":null,\"x_ThirdCustomerTime\":null,\"x_TransferCode\":null,\"x_XmFqGUID\":null,\"x_ZygwAllGUID\":null}],\"old\":[null]}"
      //    val jsonObject = JsonUtils.gson(message)
      //    println(jsonObject.get("database"))
    }

  }

}
