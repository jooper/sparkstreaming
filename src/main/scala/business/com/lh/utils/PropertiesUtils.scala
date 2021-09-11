package com.lh.utils

import java.util.Properties

object PropertiesUtils {
  val Init = new Properties()
  private val in = this.getClass().getClassLoader().getResourceAsStream("conf/config.properties");
  Init.load(in)
}
