package com.github.ericytsang.lib.abstractstream

class QueueStreamTest:StreamTest()
{
    override val src:QueueOutputStream = QueueOutputStream()
    override val sink:QueueInputStream = QueueInputStream(src)
}
