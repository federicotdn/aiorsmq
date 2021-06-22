Examples
========

Once you have `installed <install.html>`_ aiorsmq, you can test out some of its features.

**Note**: The examples in this page assume there is a Redis instance
running at ``localhost:6379``.

Sending Messages
----------------

In this example, we first create a connection to Redis by using the
``from_url`` function of the `aioredis
<https://github.com/aio-libs/aioredis-py>`_ package. Then, we create a
new queue called ``my-queue``, and we send one message to it.

.. literalinclude:: example1.py

Receiving Messages
------------------

To receive a message, we need to call the ``receive_message`` method,
like so:

.. literalinclude:: example2.py

Running this code will print:

.. code-block:: text

   The message is: Hello, world!

Calling ``delete_message`` is necessary to ensure that the message is
removed from the queue. By default, receiving a message from a queue
will make the message 'invisible' for 30 seconds (this is called the
visiblity timer or ``vt``). After this period has elapsed, the message
will be re-queued again. The idea of this mechanism is to ensure no
messages are lost: if your program crashes or errors out after calling
``receive_message`` but before calling ``delete_message``, you will
have a chance to receive the message again.

If you wish to automatically delete a message immediately after receiving
it, you can use the ``pop_message`` method instead.

Full Reference
--------------
To see the full documentation for every public class, method and function in
aiorsmq, please see the `API Documentation <aiorsmq.html>`_ page.
