package com.rnctech.common.utils

import java.io.InputStream
import java.nio.file.Paths

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.annotation.JsonInclude.Include
import scala.reflect.ClassTag


trait JsonUtil {

  object ScalaObjectMapper extends ObjectMapper {
    registerModule(DefaultScalaModule)
    setSerializationInclusion(Include.NON_NULL)
  }

  def readJson[T: ClassTag](path: String): T =
    ScalaObjectMapper.readValue(Paths.get(path).toFile, implicitly[ClassTag[T]].runtimeClass).asInstanceOf[T]

  def readJson[T: ClassTag](is: InputStream): T =
    ScalaObjectMapper.readValue(is, implicitly[ClassTag[T]].runtimeClass).asInstanceOf[T]

  def readJsonString[T: ClassTag](s: String): T =
    ScalaObjectMapper.readValue(s.getBytes("UTF-8"), implicitly[ClassTag[T]].runtimeClass).asInstanceOf[T]

  def toString(o: Object) : String = {
		ScalaObjectMapper.writeValueAsString(o)
  }
}
