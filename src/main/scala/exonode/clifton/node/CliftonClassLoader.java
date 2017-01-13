package exonode.clifton.node;

import com.zink.fly.Fly;
import exonode.distributer.FlyClassEntry;
import exonode.distributer.FlyJarEntry;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

public class CliftonClassLoader extends ClassLoader {

    public HashMap<String, byte[]> classByteCodes = new HashMap<String, byte[]>();

    private static Fly space;

    public CliftonClassLoader() {
        super(CliftonClassLoader.class.getClassLoader());
    }

    public void init(byte[] jar) {
        try {
            // set up the streams to process the jar
            ByteArrayInputStream bais = new ByteArrayInputStream(jar);

            JarInputStream jis = new JarInputStream(bais);

            // loop over the jar input stream and load all of the classes byte
            // arrays
            // into hash map so that they can referenced in any order by the
            // class
            // loader.
            JarEntry je;

            while ((je = jis.getNextJarEntry()) != null) {
                // only load the classes
                if (je.getName().endsWith(".class")) {
                    int entrySize = (int) je.getSize();

                    // Jar is probably compressed, so we don't know the size of
                    // it.
                    if (entrySize == -1) {
                        entrySize = 1024;
                    }

                    byte[] byteCode = null;

                    byteCode = new byte[entrySize];

                    int readLen = 0;
                    int totalLen = 0;
                    while (readLen != -1) {
                        readLen = jis.read(byteCode, totalLen, byteCode.length
                                - totalLen);
                        if (readLen != -1) {
                            totalLen += readLen;
                        }
                        if (totalLen == byteCode.length) { // Need to increase
                            // the size of the
                            // byteCodeBuffer
                            byte[] newByteCode = new byte[byteCode.length * 2];
                            System.arraycopy(byteCode, 0, newByteCode, 0,
                                    byteCode.length);
                            byteCode = newByteCode;
                        }
                        if (readLen == 0) {
                            break;
                        }
                    }

                    // resize the byteCode
                    byte[] newByteCode = new byte[totalLen];
                    System.arraycopy(byteCode, 0, newByteCode, 0, totalLen);
                    byteCode = newByteCode;

                    // make the class name compliant for post 1.4.2-02
                    // implementations
                    // of the class loader i.e. '.' not '/' and trim the .class
                    // from the
                    // end - if (className.endsWith(".class"))
                    String className = je.getName().replace('/', '.');
                    className = className.replaceAll("\\.class", "");
                    classByteCodes.put(className, byteCode);
                }
            }
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        if (name.startsWith("com.exocute.clifton.node")
                || name.startsWith("com.zink.fly")) {
            String newName = name.replace('.', '/').concat(".class");
            // URL url = getParent().getResource(newName);
            InputStream resourceStream = getParent().getResourceAsStream(
                    newName);
            int length;
            try {
                length = resourceStream.available();

                byte[] classBytes = new byte[length];

                resourceStream.read(classBytes);

                return defineClass(name, classBytes, 0, classBytes.length);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        } else {
            try {
                Class c = findClass(name);
                return c;
            } catch (ClassNotFoundException ex) {
                return getParent().loadClass(name);
            }
        }
    }

    public final Class findClass(String name) throws ClassNotFoundException {
//        System.out.println("looking for :" + name);
        if (!classByteCodes.containsKey(name)) {
            byte[] b = getJarFromSpace(name);
            if (b != null) {
                init(b);
            }
        }
        byte[] b = loadClassData(name);
        if (b == null)
            throw new ClassNotFoundException("Required Class not available :"
                    + name);
        return defineClass(name, b, 0, b.length);
    }

    public final byte[] loadClassData(String name) {
        return (byte[]) classByteCodes.get(name);
    }

    /**
     * Use a spot of NIO to quickly load the jar from the given File into a byte
     * array.
     *
     * @param file
     * @return the whole file as an array of bytes
     */
    public static byte[] getJarAsBytes(File file) {
        FileChannel roChannel;
        byte[] jarAsBytes = null;

        try {
            roChannel = new RandomAccessFile(file, "r").getChannel();
            ByteBuffer roBuf = roChannel.map(FileChannel.MapMode.READ_ONLY, 0,
                    (int) roChannel.size());
            roBuf.clear();
            jarAsBytes = new byte[roBuf.capacity()];
            roBuf.get(jarAsBytes, 0, jarAsBytes.length);
            roChannel.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jarAsBytes;
    }

    public static byte[] getJarFromSpace(String className) {
        // get the JarSpace
        if (space == null) {
            space = SpaceCache.getJarSpace();
            if (space == null) {
                throw new RuntimeException("Cant find Jar Space");
            }
        }

        byte[] reply = null;
        FlyClassEntry classTmpl = new FlyClassEntry(className, null);
        FlyClassEntry fce = space.read(classTmpl, 200);
        if (fce != null) {
            FlyJarEntry jarTmpl = new FlyJarEntry(fce.jarName(), null);
            FlyJarEntry fje = space.read(jarTmpl, 0L);
            reply = fje.bytes();
        }
        return reply;
    }

}
