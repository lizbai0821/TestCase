object TransferUtil {

  def LongTransfer(numString: String): Long = {
    if ("".equals(numString)) {
      0
    } else {
      numString.toLong
    }
  }

  def intTransfer(numString: String): Int = {
    if ("".equals(numString)) {
      0
    } else {
      numString.toInt
    }
  }

  def patitionKeyTransfer(numString: String): String = {
    if ("".equals(numString)) {
      "NULL"
    } else {
      numString.substring(numString.length - 1)
    }

  }
}
