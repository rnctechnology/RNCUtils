package com.rnctech.common.utils

class StackTraceData(val declaringClass: String, val methodName: String, val fileName: String, val lineNumber: Int) {
  def this(element: StackTraceElement) {
    this(element.getClassName, element.getMethodName, element.getFileName, element.getLineNumber)
  }
}