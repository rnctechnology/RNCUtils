package com.rnctech.common.utils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object ScalaObjectMapper extends ObjectMapper {
  registerModule(DefaultScalaModule)
  
}
