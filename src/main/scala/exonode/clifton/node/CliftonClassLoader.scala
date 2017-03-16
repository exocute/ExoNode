package exonode.clifton.node

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util.jar.{JarEntry, JarInputStream}

import exonode.distributer.{FlyClassEntry, FlyJarEntry}

import scala.collection.mutable

/**
  * Created by #GrowinScala
  */
class CliftonClassLoader() extends ClassLoader(getClass.getClassLoader) {

  private val CLASS_EXTENSION = ".class"
  private val classByteCodes = new mutable.HashMap[String, Array[Byte]]()

  def init(jar: Array[Byte]): Unit = {
    try {
      // set up the streams to process the jar
      val bais: ByteArrayInputStream = new ByteArrayInputStream(jar)

      val jis: JarInputStream = new JarInputStream(bais)

      // loop over the jar input stream and load all of the classes byte arrays
      // into hash map so that they can referenced in any order by the class loader.
      var je: JarEntry = null
      while ( {
        je = jis.getNextJarEntry
        je != null
      }) {
        // only load the classes
        if (je.getName.endsWith(CLASS_EXTENSION)) {
          var entrySize = je.getSize.toInt

          // Jar is probably compressed, so we don't know the size of it.
          if (entrySize == -1) {
            entrySize = 1024
          }

          var byteCode: Array[Byte] = Array.ofDim[Byte](entrySize)

          var readLen = -2
          var totalLen = 0
          while (readLen != -1 && readLen != 0) {
            readLen = jis.read(byteCode, totalLen, byteCode.length
              - totalLen)
            if (readLen != -1) {
              totalLen += readLen
            }
            if (totalLen == byteCode.length) {
              // Need to increase
              // the size of the
              // byteCodeBuffer
              val newByteCode = Array.ofDim[Byte](byteCode.length * 2)
              System.arraycopy(byteCode, 0, newByteCode, 0, byteCode.length)
              byteCode = newByteCode
            }
          }

          // resize the byteCode
          val newByteCode = Array.ofDim[Byte](totalLen)
          System.arraycopy(byteCode, 0, newByteCode, 0, totalLen)
          byteCode = newByteCode

          // make the class name compliant for post 1.4.2-02
          // implementations
          // of the class loader i.e. '.' not '/' and trim the .class
          // from the
          val className = {
            val name = je.getName.replace('/', '.').replace('\\', '.')
            if (name.endsWith(CLASS_EXTENSION))
              name.substring(0, name.length - CLASS_EXTENSION.length)
            else
              name
          }
          classByteCodes.put(className, byteCode)
        }
      }
    } catch {
      case e: Exception => e.printStackTrace(System.err)
    }
  }

  override def loadClass(name: String): Class[_] = {
    try {
      findClass(name)
    } catch {
      case _: ClassNotFoundException => getParent.loadClass(name)
      case _: SecurityException =>
        getParent.loadClass(name)
    }
  }

  private val cache = mutable.Map[String, Class[_]]()

  override def findClass(name: String): Class[_] = {
    cache.getOrElseUpdate(name, {
      if (!classByteCodes.contains(name))
        CliftonClassLoader.getJarFromSpace(name).foreach(init)

      loadClassData(name) match {
        case None =>
          throw new ClassNotFoundException("Required Class not available :" + name)
        case Some(b) =>
          defineClass(name, b, 0, b.length)
      }
    })
  }

  def loadClassData(name: String): Option[Array[Byte]] = {
    classByteCodes.get(name)
  }
}

object CliftonClassLoader {

  def getJarAsBytes(file: File): Option[Array[Byte]] = {
    try {
      val roChannel = new RandomAccessFile(file, "r").getChannel
      val roBuf: ByteBuffer = roChannel.map(FileChannel.MapMode.READ_ONLY, 0L, roChannel.size())
      roBuf.clear()
      val jarAsBytes = Array.ofDim[Byte](roBuf.capacity)
      roBuf.get(jarAsBytes, 0, jarAsBytes.length)
      roChannel.close()
      Some(jarAsBytes)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        None
    }
  }

  def getJarFromSpace(className: String): Option[Array[Byte]] = {
    val space = SpaceCache.getJarSpace

    val classTmpl: FlyClassEntry = FlyClassEntry(className, null)
    val fce = space.read(classTmpl, 200)
    fce.map { entry =>
      val jarTmpl: FlyJarEntry = FlyJarEntry(entry.jarName, null)
      val fje: FlyJarEntry = space.read(jarTmpl, 0L).get
      fje.bytes
    }
  }

}
