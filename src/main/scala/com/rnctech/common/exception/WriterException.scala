package com.rnctech.common.exception

import java.lang.Throwable

class WriterException(val message: String, val th: Throwable) extends Exception {
  override def getMessage: String = s"Error: $message with ${th.getMessage}"
}