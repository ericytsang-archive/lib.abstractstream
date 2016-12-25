package com.github.ericytsang.lib.abstractstream

import java.io.IOException
import java.io.OutputStream
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.withLock
import kotlin.concurrent.write

abstract class AbstractOutputStream:OutputStream()
{
    final override fun write(b:Int) = writeManager.write(b)

    final override fun write(b:ByteArray) = super.write(b)

    final override fun write(b:ByteArray,off:Int,len:Int) = super.write(b,off,len)

    final override fun close() = closeManager.close()

    val isClosed:Boolean get() = state is Closed

    protected abstract fun doWrite(b:Int)

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
        fun write(b:Int)
    }

    private inner class Opened:State
    {
        override fun write(b:Int)
        {
            threadManager.setThread {
                threadManager.useThread {
                    doWrite(b)
                }
            }
        }
    }

    private inner class Closed:State
    {
        override fun write(b:Int) = throw IOException("stream closed; cannot write.")
    }

    private val writeManager = object
    {
        fun write(b:Int):Unit = synchronized(this)
        {
            state.write(b)
        }
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
