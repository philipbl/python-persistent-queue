[![Build Status](https://travis-ci.org/philipbl/python-persistent-queue.svg?branch=master)](https://travis-ci.org/philipbl/python-persistent-queue) [![Coverage Status](https://coveralls.io/repos/github/philipbl/python-persistent-queue/badge.svg?branch=master)](https://coveralls.io/github/philipbl/python-persistent-queue?branch=master)


Implementation of a persistent queue in Python. I looked around and couldn't find anything that fit my needs, so I made my own. Example usage:

```python
from persistent_queue import PersistentQueue

queue = PersistentQueue('queue')

# Add stuff
queue.push(1)
queue.push(2)
queue.push(3)
queue.push(['a', 'b', 'c'])

data = queue.peek()  # 1
data = queue.peek(4)  # [1, 2, 3, 'a']
size = len(queue)  # 6

queue.push('foobar')

data = queue.pop()  # 1

queue.delete(2)
data = queue.pop()  # 3

queue.clear()
```

Objects that are added to the queue must be pickle-able. A file is saved to the file system based on the name given to the queue. The same name must be given if you want the data to persist.

I created this with the following workflow in mind:

```python

data = queue.peek(5)

success = upload_data_somewhere(data)

if success:
    queue.delete(5)

```

