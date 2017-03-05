package com.github.ericytsang.lib.abstractstream

import java.util.concurrent.LinkedBlockingQueue

class QueueOutputStream:AbstractOutputStream()
{
    private val buffer = LinkedBlockingQueue<Byte>()
    override fun doWrite(b:Int)
    {
        buffer.put(b.toByte())
    }
    private var readThread:Thread? = null
    internal fun doRead():Int
    {
        readThread = Thread.currentThread()
        try
        {
            return if (!isClosed)
            {
                buffer.take().toInt().and(0xFF)
            }
            else
            {
                buffer.poll()?.toInt()?.and(0xFF) ?: -1
            }
        }
        catch (ex:InterruptedException)
        {
            return -1
        }
        finally
        {
            readThread = null
        }
    }
    var closeCount = 0
    override fun oneShotClose()
    {
        check(closeCount++ == 0)
        readThread?.interrupt()
    }
}
