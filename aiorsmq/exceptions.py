class AIORSMQException(Exception):
    """Base class for all `aiorsmq` exceptions."""


class QueueExistsException(AIORSMQException):
    """Exception raised when a queue name is already taken."""


class QueueNotFoundException(AIORSMQException):
    """Exception raised when a queue does not exist."""


class MessageNotFoundException(AIORSMQException):
    """Exception raised when a message does not exist."""
