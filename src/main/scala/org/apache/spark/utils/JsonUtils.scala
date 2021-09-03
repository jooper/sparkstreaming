package org.apache.spark.utils

import com.google.gson.{Gson, JsonObject, JsonParser}


object JsonUtils {
  def gson(jsonStr: String):JsonObject ={
    val json = new JsonParser()
    val obj = json.parse(jsonStr).asInstanceOf[JsonObject]
    obj
  }


//  def handleMessage2CaseClass(jsonStr: String): KafkaMessage = {
//    val gson = new Gson()
//    gson.fromJson(jsonStr, classOf[KafkaMessage])
//  }


}
