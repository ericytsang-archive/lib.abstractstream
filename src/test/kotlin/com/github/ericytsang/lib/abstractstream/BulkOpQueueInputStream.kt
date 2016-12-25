package com.github.ericytsang.lib.abstractstream

class BulkOpQueueInputStream(val src:BulkOpQueueOutputStream):AbstractInputStream()
{
    override fun doRead(b:ByteArray,off:Int,len:Int):Int
    {
        return src.doRead(b,off,len)
    }
}
