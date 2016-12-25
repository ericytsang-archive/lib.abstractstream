package com.github.ericytsang.lib.abstractstream

import java.util.concurrent.LinkedBlockingQueue

class BulkOpQueueOutputStream:AbstractOutputStream()
{
    private val buffer = LinkedBlockingQueue<Byte>()
    override fun doWrite(b:ByteArray,off:Int,len:Int)
    {
        for (i in off..off+len-1)
        {
            buffer.put(b[i])
        }
    }
    private var readThread:Thread? = null
    internal fun doRead(b:ByteArray,off:Int,len:Int):Int
    {
        readThread = Thread.currentThread()
        try
        {
            var read = 0
            for (i in off..off+len-1)
            {
                if (!isClosed)
                {
                    b[i] = buffer.take()
                }
                else
                {
                    b[i] = buffer.poll() ?: break
                }
                read++
            }
            return if (isClosed && read == 0)
            {
                -1
            }
            else
            {
                read
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
    override fun doClose()
    {
        setClosed()
        readThread?.interrupt()
    }
}
