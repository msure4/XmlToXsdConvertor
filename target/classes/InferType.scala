import java.util.regex.Pattern
class InferType {

  private val P_URL = Pattern.compile("[a-z]+://.*")
  val XSD_URL = ":anyURI"

  private val P_BOOLEAN = Pattern.compile("(true|false)")
  val XSD_BOOLEAN = ":boolean"

  private val P_DATE = Pattern.compile("\\d{4}-\\d{2}-\\d{2}")
  val XSD_DATE = ":date"

  private val P_TIME = Pattern.compile("\\d{2}:\\d{2}:\\d{2}\\.\\d{2}")
  val XSD_TIME = ":time"

  private val P_DATE_TIME = Pattern.compile("^([\\+-]?\\d{4}(?!\\d{2}\\b))((-?)((0[1-9]|1[0-2])(\\3([12]\\d|0[1-9]|3[01]))?|W([0-4]\\d|5[0-2])(-?[1-7])?|(00[1-9]|0[1-9]\\d|[12]\\d{2}|3([0-5]\\d|6[1-6])))([T\\s]((([01]\\d|2[0-3])((:?)[0-5]\\d)?|24\\:?00)([\\.,]\\d+(?!:))?)?(\\17[0-5]\\d([\\.,]\\d+)?)?([zZ]|([\\+-])([01]\\d|2[0-3]):?([0-5]\\d)?)?)?)?$")
  val XSD_DATE_TIME = ":dateTime"

  private val P_INT = Pattern.compile("-?\\d{1,9}")
  val XSD_INT = ":int"

  private val P_LONG = Pattern.compile("-?\\d+")
  val XSD_LONG = ":long"

  private val P_DECIMAL = Pattern.compile("-?\\d+\\.\\d+")
  val XSD_DECIMAL = ":decimal"

  private val P_NORMALIZED_STRING = Pattern.compile("[^\\s]+")
  val XSD_NORMALIZED_STRING = ":normalizedString"

  val XSD_STRING = ":string"

  def getObjectType(content: String): String = {
    if (content == null) return XSD_STRING
    if (P_URL.matcher(content).matches) return XSD_URL
    else if (P_BOOLEAN.matcher(content).matches) return XSD_BOOLEAN
    else if (P_DATE.matcher(content).matches) return XSD_DATE
    else if (P_TIME.matcher(content).matches) return XSD_TIME
    else if (P_DATE_TIME.matcher(content).matches) return XSD_DATE_TIME
    else if (P_INT.matcher(content).matches) return XSD_INT
    else if (P_LONG.matcher(content).matches) return XSD_LONG
    else if (P_DECIMAL.matcher(content).matches) return XSD_DECIMAL
    else if (P_NORMALIZED_STRING.matcher(content).matches) return XSD_NORMALIZED_STRING
    XSD_STRING
  }
}
