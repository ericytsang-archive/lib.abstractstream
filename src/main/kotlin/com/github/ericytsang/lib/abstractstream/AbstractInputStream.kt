package com.github.ericytsang.lib.abstractstream

import java.io.IOException
import java.io.InputStream
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.withLock
import kotlin.concurrent.write

abstract class AbstractInputStream:InputStream()
{
    final override fun read():Int = state.read()

    final override fun read(b:ByteArray):Int = read(b,0,b.size)

    final override fun read(b:ByteArray,off:Int,len:Int):Int = state.read(b,off,len)

    final override fun close() = closeManager.close()

    /**
     * executes function, passing it a reference to the [Thread] that is
     * currently executing [doRead].
     */
    protected fun <R> useActiveThread(block:(Thread?)->R):R
    {
        return threadManager.useThread(block)
    }

    /**
     * reference to the [Thread] that is currently executing [doRead].
     */
    protected val activeThread:Thread? get() = threadManager.thread

    /**
     * true if [setClosed] was called; false otherwise.
     */
    protected val isClosed:Boolean get() = state is Closed

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
     *
     * must be implemented to call either [doNothing] or [setClosed] exactly one
     * time before returning.
     */
    protected open fun doClose():Unit = doNothing()

    /**
     * once [setClosed] returns, all subsequent calls to [read] will no longer
     * be delegated to [doRead] and shall throw an [IOException].
     */
    protected fun setClosed() = closeManager.setClosed()

    /**
     * acknowledges that the [doClose] method is a nop implementation that does
     * not release stream resources. subsequent calls to [read] will still be
     * delegated to [doRead].
     */
    protected fun doNothing() = closeManager.doNothing()

    private var state:State = Opened()

    private interface State
    {
        fun read():Int
        fun read(b:ByteArray,off:Int,len:Int):Int
    }

    private inner class Opened:State
    {
        override fun read():Int = synchronized(this)
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
        override fun read(b:ByteArray,off:Int,len:Int):Int = synchronized(this)
        {
            val readResult = threadManager.setThread {
                threadManager.useThread {
                    doRead(b,off,len)
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
        override fun read(b:ByteArray,off:Int,len:Int):Int = -1
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
        fun <R> useThread(b:(Thread?)->R):R
        {
            return lock.read {
                b(thread)
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
