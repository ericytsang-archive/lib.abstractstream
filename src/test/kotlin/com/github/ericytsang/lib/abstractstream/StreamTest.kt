package com.github.ericytsang.lib.abstractstream

import org.junit.Test
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.InputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.OutputStream
import java.util.Arrays
import kotlin.concurrent.thread

abstract class StreamTest()
{
    abstract val src:OutputStream
    abstract val sink:InputStream

    @Test
    fun pipeByteArrays()
    {
        val written = byteArrayOf(0,2,5,6)
        val read = byteArrayOf(0,0,0,0)
        src.write(written)
        DataInputStream(sink).readFully(read)
        assert(Arrays.equals(written,read))
    }

    @Test
    fun pipeNegativeNumber()
    {
        src.write(-1)
        assert(sink.read() == 0xFF)
    }

    @Test
    fun pipeShorts()
    {
        DataOutputStream(src).writeShort(0)
        DataOutputStream(src).writeShort(1)
        DataOutputStream(src).writeShort(-1)
        assert(DataInputStream(sink).readShort() == 0.toShort())
        assert(DataInputStream(sink).readShort() == 1.toShort())
        assert(DataInputStream(sink).readShort() == (-1).toShort())
    }

    @Test
    fun pipeStringObjects()
    {
        ObjectOutputStream(src).writeObject("hello!!!")
        assert(ObjectInputStream(sink).readObject() == "hello!!!")
    }

    @Test
    fun pipeMultiFieldObjects()
    {
        ObjectOutputStream(src).writeObject(RuntimeException("blehh"))
        ObjectInputStream(sink).readObject()
    }

    @Test
    fun simpleReadFirstThenWriteTest()
    {
        thread {
            Thread.sleep(100)
            src.write(234)
        }
        assert(sink.read() == 234)
    }

    @Test
    fun complexReadFirstThenWriteTest()
    {
        thread {
            Thread.sleep(100)
            ObjectOutputStream(src).writeObject(RuntimeException("blehh"))
        }
        ObjectInputStream(sink).readObject()
    }

    @Test
    fun pipeTestWellBeyondEof()
    {
        src.write(0)
        src.write(2)
        src.write(5)
        src.write(6)
        src.write(127)
        src.write(128)
        src.write(129)
        src.write(254)
        src.write(255)
        src.close()
        assert(sink.read() == 0)
        assert(sink.read() == 2)
        assert(sink.read() == 5)
        assert(sink.read() == 6)
        assert(sink.read() == 127)
        assert(sink.read() == 128)
        assert(sink.read() == 129)
        assert(sink.read() == 254)
        assert(sink.read() == 255)
        assert(sink.read() == -1)
        assert(sink.read() == -1)
        assert(sink.read() == -1)
        assert(sink.read() == -1)
    }

    @Test
    fun pipeTestThreadInterrupt()
    {
        src.write(0)
        src.write(2)
        src.write(5)
        src.write(6)
        src.write(127)
        src.write(128)
        src.write(129)
        src.write(254)
        src.write(255)
        thread {
            Thread.sleep(500)
            src.close()
        }
        assert(sink.read() == 0)
        assert(sink.read() == 2)
        assert(sink.read() == 5)
        assert(sink.read() == 6)
        assert(sink.read() == 127)
        assert(sink.read() == 128)
        assert(sink.read() == 129)
        assert(sink.read() == 254)
        assert(sink.read() == 255)
        assert(sink.read() == -1)
        assert(sink.read() == -1)
        assert(sink.read() == -1)
        assert(sink.read() == -1)
    }
}
