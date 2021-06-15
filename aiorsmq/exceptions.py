class AIORSMQException(Exception):
    """Base class for all `aiorsmq` exceptions."""


class QueueExistsException(AIORSMQException):
    """Exception raised when a queue name is already taken."""


class QueueNotFoundException(AIORSMQException):
    """Exception raised when a queue does not exist."""


class MessageNotFoundException(AIORSMQException):
    """Exception raised when a message does not exist."""


class NoAttributesSpecified(AIORSMQException):
    """Exception raised when no queue attributes were specified."""


class InvalidValueException(AIORSMQException):
    """Raised when an argument has the right type but its value is out of range."""
