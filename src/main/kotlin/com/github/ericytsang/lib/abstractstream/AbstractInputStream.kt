package com.github.ericytsang.lib.abstractstream

import java.io.IOException
import java.io.InputStream
import java.util.concurrent.BlockingQueue
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.withLock
import kotlin.concurrent.write

/**
 * [InputStream] that wraps any [BlockingQueue]. it is a [InputStream] adapter
 * for a [BlockingQueue]; data put into the [sourceQueue] may be read out of
 * this [AbstractBulkOpInputStream].
 *
 * Created by Eric Tsang on 12/13/2015.
 */
abstract class AbstractInputStream:InputStream()
{
    final override fun read():Int = state.read()

    final override fun read(b:ByteArray):Int = super.read(b)

    final override fun read(b:ByteArray,off:Int,len:Int):Int = super.read(b,off,len)

    final override fun close() = closeManager.close()

    val isClosed:Boolean get() = state is Closed

    /**
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
    protected abstract fun doRead():Int

    protected open fun doClose():Unit = doNothing()

    /**
     * all subsequent calls to [read] will no longer be delegated to [doRead]
     * and shall throw an [IOException].
     */
    protected fun setClosed() = closeManager.setClosed()

    protected fun doNothing() = closeManager.doNothing()

    private var state:State = Opened()

    private interface State
    {
        fun read():Int
    }

    private inner class Opened:State
    {
        private val readLock = ReentrantLock()
        override fun read():Int = readLock.withLock()
        {
            val readResult = threadManager.setThread {
                threadManager.useThread {
                    doRead()
                }
            }
            if (readResult == -1)
            {
                close()
            }
            return readResult
        }
    }

    private inner class Closed:State
    {
        override fun read():Int = -1
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
