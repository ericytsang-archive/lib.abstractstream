package com.github.ericytsang.lib.abstractstream

/**
 * Created by surpl on 12/24/2016.
 */
class QueueInputStream(val src:QueueOutputStream):AbstractInputStream()
{
    override fun doRead():Int
    {
        return src.doRead()
    }
}
