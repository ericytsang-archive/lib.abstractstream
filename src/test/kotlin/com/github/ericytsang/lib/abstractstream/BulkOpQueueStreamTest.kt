package com.github.ericytsang.lib.abstractstream

class BulkOpQueueStreamTest:StreamTest()
{
    override val src:BulkOpQueueOutputStream = BulkOpQueueOutputStream()
    override val sink:BulkOpQueueInputStream = BulkOpQueueInputStream(src)
}
