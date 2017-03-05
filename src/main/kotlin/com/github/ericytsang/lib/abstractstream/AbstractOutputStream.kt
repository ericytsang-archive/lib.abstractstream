package com.github.ericytsang.lib.abstractstream

import com.github.ericytsang.lib.onlysetonce.OnlySetOnce
import java.io.OutputStream
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

abstract class AbstractOutputStream:OutputStream()
{
    final override fun write(b:Int) = writeMutex.withLock {doWrite(b)}

    final override fun write(b:ByteArray) = writeMutex.withLock {doWrite(b,0,b.size)}

    final override fun write(b:ByteArray,off:Int,len:Int) = writeMutex.withLock {doWrite(b,off,len)}

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

    val isClosed get() = closeStackTrace != null

    var closeStackTrace:Array<StackTraceElement>? by OnlySetOnce()
        private set

    private val writeMutex = ReentrantLock()

    /**
     * guaranteed that calls to this method are mutually exclusive. should be
     * implemented in a way that when [doClose] is called, the thread currently
     * executing this method would return immediately.
     *
     * @param b [Int] whose least significant byte will be sent.
     */
    protected open fun doWrite(b:Int)
    {
        writeManager.delegateWrite(b)
    }

    /**
     * guaranteed that calls to this method are mutually exclusive.
     *
     * guaranteed that calls to this method are mutually exclusive. should be
     * implemented in a way that when [doClose] is called, the thread currently
     * executing this method would return immediately.
     *
     * @param b [ByteArray] of data that will be sent from.
     * @param off specifies a starting index in [b].
     * @param len specifies an ending index in [b].
     */
    protected open fun doWrite(b:ByteArray,off:Int,len:Int)
    {
        super.write(b,off,len)
    }

    /**
     * guaranteed to only be called once.
     */
    protected open fun oneShotClose() = Unit

    private val writeManager = object
    {
        private val singleElementByteArray = byteArrayOf(0)
        fun delegateWrite(b:Int)
        {
            singleElementByteArray[0] = b.toByte()
            write(singleElementByteArray,0,1)
        }
    }
}
