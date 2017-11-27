package faker

import java.security.MessageDigest

object Hash {



  def sha256(s: String): String = {

    val m = java.security.MessageDigest.getInstance("SHA-256").digest(s.getBytes("UTF-8"))

    m.map("%02x".format(_)).mkString

  }

  def  md5(s: String): String = {

    val m = java.security.MessageDigest.getInstance("MD5").digest(s.getBytes("UTF-8"))

    m.map("%02x".format(_)).mkString

  }
}
