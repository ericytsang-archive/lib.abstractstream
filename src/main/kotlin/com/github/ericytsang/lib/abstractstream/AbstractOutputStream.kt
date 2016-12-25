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
    final override fun write(b:Int) = state.write(b)

    final override fun write(b:ByteArray) = write(b,0,b.size)

    final override fun write(b:ByteArray,off:Int,len:Int) = state.write(b,off,len)

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
     *
     * must be implemented to call either [doNothing] or [setClosed] exactly one
     * time before returning.
     */
    protected open fun doClose():Unit = doNothing()

    /**
     * once [setClosed] returns, all subsequent calls to [read] will no longer
     * be delegated to [doWrite] and shall indicate EOF.
     */
    protected fun setClosed() = closeManager.setClosed()

    /**
     * acknowledges that the [doClose] method is a nop implementation that does
     * not release stream resources. subsequent calls to [read] will still be
     * delegated to [doWrite].
     */
    protected fun doNothing() = closeManager.doNothing()

    private var state:State = Opened()

    private interface State
    {
        fun write(b:Int)
        fun write(b:ByteArray,off:Int,len:Int)
    }

    private inner class Opened:State
    {
        override fun write(b:Int) = synchronized(this)
        {
            threadManager.setThread {
                threadManager.useThread {
                    doWrite(b)
                }
            }
        }
        override fun write(b:ByteArray,off:Int,len:Int) = synchronized(this)
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
        override fun write(b:Int) = throw IOException("stream closed; cannot write.")
        override fun write(b:ByteArray,off:Int,len:Int) = throw IOException("stream closed; cannot write.")
    }

    private val writeManager = object
    {
        private val singleElementByteArray = byteArrayOf(0)
        fun delegateWrite(b:Int)
        {
            singleElementByteArray[0] = b.toByte()
            write(singleElementByteArray,0,1)
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
