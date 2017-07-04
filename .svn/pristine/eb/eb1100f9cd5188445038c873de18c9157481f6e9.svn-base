package com.lakala.finance.common

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.reflect.ClassTag

/**
  * Created by longxiaolei on 2017/6/12.
  */
object JsonSupport {
  @transient private val mapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  implicit class Obj2Json(o: AnyRef) {
    def toJson(none_empty: Boolean = false): String = {
      if (none_empty) mapper.setSerializationInclusion(Include.NON_EMPTY)
      mapper.writeValueAsString(o)
    }
  }

  implicit class Json2Obj(str: String) {
    def as[T: ClassTag]: T = {
      val vm = implicitly[ClassTag[T]]
      mapper.readValue(str, vm.runtimeClass.asInstanceOf[Class[T]])
    }
  }
}
