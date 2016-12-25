package com.github.ericytsang.lib.abstractstream

import java.io.IOException
import java.io.OutputStream
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.withLock
import kotlin.concurrent.write

abstract class AbstractBulkOpOutputStream:OutputStream()
{
    final override fun write(b:Int) = writeManager.write(b)

    final override fun write(b:ByteArray) = writeManager.write(b)

    final override fun write(b:ByteArray,off:Int,len:Int) = writeManager.write(b,off,len)

    final override fun close() = closeManager.close()

    protected val activeThread:Thread? get() = threadManager.thread

    protected val isClosed:Boolean get() = state is Closed

    /**
     * guaranteed that calls to this method are mutually exclusive.
     *
     * consider invoking [setClosed] when implementing.
     *
     * @param b [ByteArray] of data that will be sent from.
     * @param off specifies a starting index in [b].
     * @param len specifies an ending index in [b].
     */
    protected abstract fun doWrite(b:ByteArray,off:Int,len:Int)

    /**
     * guaranteed to only be called once.
     *
     * when implementing, either do not override or override and do all of the
     * following:
     * - release resources held by the stream
     * - immediately cause threads executing [doWrite] to throw an exception
     * - call [setClosed]
     */
    protected open fun doClose():Unit = doNothing()

    /**
     * all subsequent calls to [write] will no longer be delegated to [doWrite]
     * and shall throw an [IOException].
     */
    protected fun setClosed() = closeManager.setClosed()

    protected fun doNothing() = closeManager.doNothing()

    private var state:State = Opened()

    private interface State
    {
        fun write(b:ByteArray,off:Int,len:Int)
    }

    private inner class Opened:State
    {
        override fun write(b:ByteArray,off:Int,len:Int)
        {
            threadManager.setThread {
                threadManager.useThread {
                    doWrite(b,off,len)
                }
            }
        }
    }

    private inner class Closed:State
    {
        override fun write(b:ByteArray,off:Int,len:Int) = throw IOException("stream closed; cannot write.")
    }

    private val threadManager = object
    {
        private val lock = ReentrantReadWriteLock()
        var thread:Thread? = null
        fun <R> setThread(b:()->R):R
        {
            lock.write {
                thread = Thread.currentThread()
            }
            try
            {
                return b()
            }
            finally
            {
                lock.write {
                    thread = null
                }
            }
        }
        fun <R> useThread(b:(Thread)->R):R
        {
            return lock.read {
                b(Thread.currentThread())
            }
        }
    }

    private val writeManager = object
    {
        private val singleElementByteArray = byteArrayOf(0)
        fun write(b:Int) = synchronized(this)
        {
            singleElementByteArray[0] = b.toByte()
            write(singleElementByteArray,0,1)
        }
        fun write(b:ByteArray) = synchronized(this)
        {
            write(b,0,b.size)
        }
        fun write(b:ByteArray,off:Int,len:Int) = synchronized(this)
        {
            state.write(b,off,len)
        }
    }

    private val closeManager = object
    {
        private val closeLock = ReentrantLock()
        private var firstCall = true
        private var actionCount = 0
        fun close() = closeLock.withLock()
        {
            if (firstCall)
            {
                firstCall = false
                actionCount = 0
                threadManager.useThread {
                    doClose()
                }
                require(actionCount == 1)
                {
                    "implementation of doClose must call either setClosed or doNothing exactly once before returning"
                }
            }
        }
        fun setClosed()
        {
            checkCall()
            state = Closed()
        }
        fun doNothing()
        {
            checkCall()
        }
        private fun checkCall()
        {
            require(closeLock.isHeldByCurrentThread)
            {
                "method must be called by the same thread that executes the doClose method"
            }
            require(actionCount++ == 0)
            {
                "implementation of doClose must call either setClosed or doNothing exactly once before returning"
            }
        }
    }
}
