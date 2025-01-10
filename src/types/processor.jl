"""
    Processor

An abstract type representing a processing device and associated memory, where
data can be stored and operated on. Subtypes should be immutable, and
instances should compare equal if they represent the same logical processing
device/memory. Subtype instances should be serializable between different
nodes. Subtype instances may contain a "parent" `Processor` to make it easy to
transfer data to/from other types of `Processor` at runtime.
"""
abstract type Processor end