import java.io._
import java.nio.charset.{Charset, StandardCharsets}
import java.util

import org.apache.commons.cli.HelpFormatter
import nu.xom._
import org.apache.commons
import org.apache.commons.cli.{CommandLineParser, DefaultParser}
import org.apache.spark.{SparkConf, SparkContext}
import org.wiztools.commons.{Charsets, StringUtil}
import sun.tools.jar.CommandLine
import org.apache.commons.cli.Options
import org.apache.hadoop.fs.Path
import org.apache.hadoop.{fs, io}
import org.apache.spark.rdd.RDD
import sun.misc.IOUtils

object ConvertXMLToXSD {
  var XSD_NS_URI = "http://www.w3.org/2001/XMLSchema"
  var xsdPrefix = "xs"
  var typeInferObj = new InferType
  var xsdDoc : Document = null

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("XMLToXSD")
    val sc = new SparkContext(conf)
    var hdfsOs : BufferedOutputStream = null
    var localOs : FileOutputStream = null


    val scFile = args(0).toString()
    val localFile = "file:///" + scFile
    var txtFile : RDD[String] = null

    if(args(2).toString().trim().toLowerCase().equalsIgnoreCase("hdfs")){
       txtFile = sc.textFile(scFile)
      val fsConf = fs.FileSystem.get(sc.hadoopConfiguration)
      val confPath = new Path(args(1).toString())
      val hadOutputFile = fsConf.create(confPath)
      hdfsOs = new BufferedOutputStream(hadOutputFile)
    }
    else
      {
         txtFile = sc.textFile(localFile.toString())
        localOs = new FileOutputStream(args(1).toString())
      }

    val fis = txtFile.collect().mkString
    val baip = commons.io.IOUtils.toInputStream(fis,"UTF-8")

    xsdDoc = ConvertXMLToXSD.getDocument(baip)

    val opString = xsdDoc.copy().getDocument().toXML().toString()

    if(args(2).toString().trim().toLowerCase().equalsIgnoreCase("hdfs")){
      try{
        hdfsOs.write(opString.getBytes())
      }
      finally {
        hdfsOs.flush()
        hdfsOs.close()
      }
    }
    else
    {
      ConvertXMLToXSD.writeOutput(localOs,Charsets.UTF_8)
    }


    println("Successfully converted XML to XSD")
  }

  @throws[IOException]
  @throws[ValidityException]
  @throws[ParsingException]
  private def getDocument(is: InputStream) : Document = {
    try {
      val parser = new Builder()
      val d = parser.build(is)
      val rootElement = d.getRootElement
      // output Document
      val outRoot = new Element(xsdPrefix + ":schema", XSD_NS_URI)
      val outDoc = new Document(outRoot)
      // setting targetNamespace
      val nsPrefix = rootElement.getNamespacePrefix
      val hasXmlns = rootElement.getNamespaceDeclarationCount > 0
      if (hasXmlns || StringUtil.isNotEmpty(nsPrefix)) {
        outRoot.addAttribute(new Attribute("targetNamespace", rootElement.getNamespaceURI))
        outRoot.addAttribute(new Attribute("elementFormDefault", "qualified"))
      }

      // adding all other namespace attributes
      for (i <- 0 to rootElement.getAttributeCount - 1) {
        val nsPrefix2 = rootElement.getNamespacePrefix(i)
        val nsURI = rootElement.getNamespaceURI(nsPrefix2)
        outRoot.addNamespaceDeclaration(nsPrefix2, nsURI)
      }
      // adding the root element
      val rootElementXsd = new Element(xsdPrefix + ":element", XSD_NS_URI)
      rootElementXsd.addAttribute(new Attribute("name", rootElement.getLocalName))
      outRoot.appendChild(rootElementXsd)
      ConvertXMLToXSD.recurseGen(rootElement, rootElementXsd)
      return outDoc
    }
    finally if (is != null) is.close
  }

  private def recurseGen(parent: Element, parentOutElement: Element): Unit = {
    // Adding complexType element:
    val complexType = new Element(xsdPrefix + ":complexType", XSD_NS_URI)
    complexType.addAttribute(new Attribute("mixed", "true"))
    val sequence = new Element(xsdPrefix + ":sequence", XSD_NS_URI)
    complexType.appendChild(sequence)
    processAttributes(parent, complexType)
    parentOutElement.appendChild(complexType)
    val children = parent.getChildElements
    val elementNamesProcessed = new util.HashSet[String]()
    for (z <- 0 to children.size() - 1)
    {
      val e = children.get(z)
      val localName = e.getLocalName
      val nsURI = e.getNamespaceURI
      val nsName = e.getQualifiedName
      if (!elementNamesProcessed.contains(nsName)) { // process an element first time only
        if (e.getChildElements.size > 0) { // Is complex type with children!
          val element = new Element(xsdPrefix + ":element", XSD_NS_URI)
          element.addAttribute(new Attribute("name", localName))
          processOccurences(element, parent, localName, nsURI)
          recurseGen(e, element) // recurse into children:

          sequence.appendChild(element)
        }
        else {
          val cnt = e.getValue
          val eValue = if (cnt == null) null
          else cnt.trim
          @SuppressWarnings(Array("static-access"))
          val xsdType = xsdPrefix + typeInferObj.getObjectType(eValue)
          val element = new Element(xsdPrefix + ":element", XSD_NS_URI)
          element.addAttribute(new Attribute("name", localName))
          processOccurences(element, parent, localName, nsURI)
          // Attributes
          val attrCount = e.getAttributeCount
          if (attrCount > 0) { // has attributes: complex type without sequence!
            val complexTypeCurrent = new Element(xsdPrefix + ":complexType", XSD_NS_URI)
            complexType.addAttribute(new Attribute("mixed", "true"))
            val simpleContent = new Element(xsdPrefix + ":simpleContent", XSD_NS_URI)
            val extension = new Element(xsdPrefix + ":extension", XSD_NS_URI)
            extension.addAttribute(new Attribute("base", xsdType))
            processAttributes(e, extension)
            simpleContent.appendChild(extension)
            complexTypeCurrent.appendChild(simpleContent)
            element.appendChild(complexTypeCurrent)
          }
          else { // if no attributes, just put the type:
            element.addAttribute(new Attribute("type", xsdType))
          }
          sequence.appendChild(element)
        }
      }
      elementNamesProcessed.add(nsName)

    }
  }

  private def processOccurences(element: Element, parent: Element, localName: String, nsURI: String): Unit = {
    if (parent.getChildElements(localName, nsURI).size > 1) element.addAttribute(new Attribute("maxOccurs", "unbounded"))
    else element.addAttribute(new Attribute("minOccurs", "0"))
  }

  private def processAttributes(inElement: Element, outElement: Element): Unit = {
    for (j <- 0 to inElement.getAttributeCount()-1)
      {
        val attr = inElement.getAttribute(j)
        val name = attr.getLocalName
        val value = attr.getValue
        var attrElement = new Element(xsdPrefix + ":attribute", XSD_NS_URI)
        attrElement.addAttribute(new Attribute("name", name))
        attrElement.addAttribute(new Attribute("type", xsdPrefix + typeInferObj.getObjectType(value)))
        attrElement.addAttribute(new Attribute("use", "required"))
        outElement.appendChild(attrElement)
      }
  }

  @throws[IOException]
  def writeOutput(os: OutputStream, charset: Charset): Unit = {
    if (null == xsdDoc) throw new IllegalStateException("Call parse() before calling this method!")
    // Display output:
    val serializer = new Serializer(os, charset.name)
    serializer.setIndent(4)
    serializer.write(xsdDoc)
  }

  private def printUsage(options: Options): Unit = {
    val formatter = new HelpFormatter
    formatter.printHelp("java -jar rest.jar [OPTIONS]", options)
  }
}