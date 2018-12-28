[![Build Status](https://travis-ci.org/philipbl/python-persistent-queue.svg?branch=master)](https://travis-ci.org/philipbl/python-persistent-queue) [![Coverage Status](https://coveralls.io/repos/github/philipbl/python-persistent-queue/badge.svg?branch=master)](https://coveralls.io/github/philipbl/python-persistent-queue?branch=master)

# Description

This is an implementation of a persistent queue in Python. By persistent, I mean that every item that is added to the queue is saved to disk. I had a specific usage pattern in mind, and I couldn't find any existing libraries that fit my needs, so I made my own. I tried to make it a drop in replacement for Python's [queue.Queue](https://docs.python.org/3/library/queue.html) object. However, I've added a few extra methods that Python's queue.Queue class does not: `peek`, `flush`, `clear`, and `close`.

I created Python persistent queue with the following workflow in mind:

```python

data = queue.peek()

success = upload_data_somewhere(data)

if success:
    queue.delete()

```

In this use case, the data is only deleted from the queue after it has been successfully processed (in this example, uploaded).

Typically, I have two threads, one that is writing data to the queue and another thread that is reading data from the queue and processing it. Something like this:

```python

queue = PersistentQueue('queue')

def consumer():
    while True:
        data = queue.peek()  # Blocking call
        success = upload_data_somewhere(data)
        if success:
            queue.delete()


def producer():
    while True:
        data = read_from_sensor()
        queue.put(data)
        time.sleep(1)
```

Objects that are added to the queue must be serializeable (and with default parameters, pickle-able). A file is saved to the file system based on the name given to the queue.

By default, `pickle` is used to serialize objects. This can be changed depending on your needs by setting the `dumps` and `loads` options (see Parameters). [dill](http://trac.mystic.cacr.caltech.edu/project/pathos/wiki/dill.html) and [msgpack](https://github.com/msgpack/msgpack-python) have been tested (see tests as an example).

When `get()` or `delete()` are called, the data isn't actually deleted. Instead a pointer is moved to the place in the file with valid data. This is to help reduce I/O costs. As a result, the file will continue to grow even if items are removed. `flush()` reclaims this space. **You must call `flush` as you see fit!**


# Extended Example

```python
from persistent_queue import PersistentQueue

queue = PersistentQueue('queue')

# Add stuff
queue.push(1)
queue.push(2)
queue.push(3)
queue.push(['a', 'b', 'c'])

data = queue.peek()  # 1
size = len(queue)  # 6

queue.push('foobar')

data = queue.get()  # Returns 1

queue.delete()  # Deletes 2
data = queue.get()  # Returns 3
data = queue.get()  # Returns ['a', 'b', 'c']

queue.clear()  # Deletes everything from queue
```

# Install

```
pip install python-persistent-queue
```

# Parameters

A persistent queue takes the following parameters:

- `filename` (*required*): The name of the file that will keep the data.
- `path` (*optional*, default='.'): The directory to put the file.
- `dumps` (*optional*, default=`pickle.dumps`): The method used to convert a Python object into bytes.
- `loads` (*optional*, default=`pickle.loads`): The method used to convert bytes into a Python object.
- `flush_limit` (*optional*, default=1048576): When the amount of empty space in the file is greater than `flush_limit`, the file will be flushed. This balances file I/O and storage space.

# Methods
