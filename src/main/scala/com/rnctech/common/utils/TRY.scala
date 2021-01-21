package com.rnctech.common.utils

import java.io.Closeable

import scala.util.{Failure, Success, Try}

object TRY {

  def apply[C <: AutoCloseable, A](closeable : C, onlyOnError : Boolean = false, onClose : Option[Throwable] => Unit = {_=>Unit} )(f : (C) =>A) : A = {
    def close(error : Option[Throwable]): Unit = {
      onClose(error)
      closeable.close
    }
    closeable match {
      case null => throw new NullPointerException
      case  _ =>
        val res = Try(f(closeable))
        res match {
          case Success(res) if (!onlyOnError) => close(None)
          case Failure(th) => close(Some(th))
            throw th
        }
        res.get
    }
  }
  def apply[C <: Closeable, A](closeable : C)(f : (C) =>A) : A = {
    closeable match {
      case null => throw new NullPointerException
      case _ => Try(f(closeable)) match {
        case Success(res) => closeable.close  ; res
        case Failure(th) => closeable.close ; throw th
      }
    }
  }

}