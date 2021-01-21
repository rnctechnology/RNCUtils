package com.rnctech.common.exception

class MetadataException(val attribute: String, val columns : Set[String]) extends Exception {
  override def getMessage: String = s"""Metadata exception for attribute $attribute with columns: $columns"""
}
