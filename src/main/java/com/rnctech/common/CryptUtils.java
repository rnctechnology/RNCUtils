package com.rnctech.common;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CryptUtils {

  private static final String ALGORITHM = "AES";
  private static final String MODE = "CBC";
  private static final String PADDING = "PKCS5Padding";
  private static final String CHARSET = "UTF-8";

  private static final Logger logger = LoggerFactory.getLogger(CryptUtils.class);
  
  private CryptUtils() {

  }

  public static String encrypt(String property, String saltFromMr) throws NoSuchAlgorithmException,
      NoSuchPaddingException, InvalidKeyException, InvalidAlgorithmParameterException,
      IllegalBlockSizeException, BadPaddingException, UnsupportedEncodingException {
    SecretKey aesKey = new SecretKeySpec(saltFromMr.getBytes(), ALGORITHM);
    Cipher aesCipher = Cipher.getInstance(ALGORITHM + "/" + MODE + "/" + PADDING);
    aesCipher.init(Cipher.ENCRYPT_MODE, aesKey, new IvParameterSpec(aesKey.getEncoded()));
    return base64Encode(aesCipher.doFinal(property.getBytes(CHARSET)));
  }

  public static String decrypt(String property, String SaltFromMr)
      throws IllegalBlockSizeException, BadPaddingException,
      IOException, InvalidKeyException, InvalidAlgorithmParameterException,
      NoSuchAlgorithmException, NoSuchPaddingException {
    SecretKey aesKey = new SecretKeySpec(SaltFromMr.getBytes(), ALGORITHM);
    Cipher aesCipher = Cipher.getInstance(ALGORITHM + "/" + MODE + "/" + PADDING);
    aesCipher.init(Cipher.DECRYPT_MODE, aesKey, new IvParameterSpec(aesKey.getEncoded()));
    return new String(aesCipher.doFinal(base64Decode(property)), CHARSET);
  }

  private static String base64Encode(byte[] bytes) {
    return new String(Base64.encodeBase64(bytes));
  }

  private static byte[] base64Decode(String property) throws IOException {
    return Base64.decodeBase64(property.getBytes());
  }
  

public static String generateMD5Hash(String msg) throws Exception {
      return generateString(msg, "MD5");
  }


public static String generateSHA1Hash(String msg) throws Exception {
      return generateString(msg, "SHA-1");
  }


public static String generateSHA256Hash(String msg) throws Exception {
      return generateString(msg, "SHA-256");
  }

private static String generateString(String msg, String algorithm) throws Exception {
	if (logger.isTraceEnabled()) logger.trace("generateHashString()");
      try {
          MessageDigest digest = MessageDigest.getInstance(algorithm);
          byte[] hashedBytes = digest.digest(msg.getBytes(CHARSET));
          if (logger.isTraceEnabled()) logger.trace("generateHashString()");
          return convertByteArrayToHexString(hashedBytes);
      } catch (NoSuchAlgorithmException | UnsupportedEncodingException ex) {
    	  logger.error(msg);
          throw new Exception("Could not generate hash value from received input", ex);
      }
  }

  /**
 * @param arrayBytes
 * @return String
 */
private static String convertByteArrayToHexString(byte[] arrayBytes) {
      StringBuffer stringBuffer = new StringBuffer();
      for (int i = 0; i < arrayBytes.length; i++) {
          stringBuffer.append(Integer.toString((arrayBytes[i] & 0xff) + 0x100, 16).substring(1));
      }
      return stringBuffer.toString();
  }
}
