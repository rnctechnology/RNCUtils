package com.rnctech.common.utils

import com.rnctech.common.CryptUtils;

trait CipherUtil {
  
	  final val PWD_SALT = "pwd.salt"
    var salt: String = _
	  
	  def encrypt(data: String): String
    def decrypt(code: String): String
    
    def setSalt(s: String): Unit = {
      salt = Option(s).getOrElse(scala.util.Properties.envOrElse(PWD_SALT, "SALT_ADMIN!@#"))
    }

  }

  class Cipher(algorithmName: String) extends CipherUtil {

    def encrypt(data: String): String = {
      CryptUtils.encrypt(data, salt)
    }

    def decrypt(code: String): String = {
      CryptUtils.decrypt(code, salt)
    }

  }

  object AESCipher extends Cipher("AES")
