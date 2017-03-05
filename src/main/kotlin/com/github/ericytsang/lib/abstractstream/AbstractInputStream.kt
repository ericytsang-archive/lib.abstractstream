package com.github.ericytsang.lib.abstractstream

import com.github.ericytsang.lib.onlysetonce.OnlySetOnce
import java.io.IOException
import java.io.InputStream
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

abstract class AbstractInputStream:InputStream()
{
    final override fun read():Int = readMutex.withLock {doRead()}

    final override fun read(b:ByteArray):Int = readMutex.withLock {doRead(b,0,b.size)}

    final override fun read(b:ByteArray,off:Int,len:Int):Int = readMutex.withLock {doRead(b,off,len)}

    final override fun close()
    {
        try
        {
            closeStackTrace = Thread.currentThread().stackTrace
            oneShotClose()
        }
        catch (ex:OnlySetOnce.Exception)
        {
            // ignore subsequent calls to close.
        }
    }

    var closeStackTrace:Array<StackTraceElement>? by OnlySetOnce()
        private set

    private val readMutex = ReentrantLock()

    /**
     * guaranteed that calls to this method are mutually exclusive.
     *
     * Reads the next byte of data from the input stream. The value byte is
     * returned as an <code>int</code> in the range <code>0</code> to
     * <code>255</code>. If no byte is available because the end of the stream
     * has been reached, the value <code>-1</code> is returned. This method
     * blocks until input data is available, the end of the stream is detected,
     * or an exception is thrown.
     *
     * <p> A subclass must provide an implementation of this method.
     *
     * @return     the next byte of data, or <code>-1</code> if the end of the
     *             stream is reached.
     * @exception  IOException  if an I/O error occurs.
     */
    protected open fun doRead():Int
    {
        val data = ByteArray(1)
        val result = read(data,0,1)

        when (result)
        {
        // if EOF, return -1 as specified by java docs
            -1 -> return -1

        // if data was actually read, return the read data
            1 -> return data[0].toInt().and(0xFF)

        // throw an exception in all other cases
            else -> throw RuntimeException("unhandled case in when statement!")
        }
    }

    /**
     * guaranteed that calls to this method are mutually exclusive.
     *
     * Reads up to len bytes of data from the input stream into an array of
     * bytes. An attempt is made to read as many as len bytes, but a smaller
     * number may be read. The number of bytes actually read is returned as an
     * integer. This method blocks until input data is available, end of file is
     * detected, or an exception is thrown.
     *
     * @param b the buffer into which the data is read.
     * @param off the start offset in array b at which the data is written.
     * @param len the maximum number of bytes to read.
     */
    protected open fun doRead(b:ByteArray,off:Int,len:Int):Int
    {
        return super.read(b,off,len)
    }

    /**
     * guaranteed to only be called once.
     */
    protected open fun oneShotClose() = Unit
}
