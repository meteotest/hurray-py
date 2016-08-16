Tutorial
========

hurray-py has an API very similar to that of the h5py library.

For this tutorial, make sure you have a hurray server running on ``localhost``
on port ``2222``.

Make sure you import ``numpy`` and ``hurraypy``:
   .. code-block:: python

    >>> import numpy as np
    >>> import hurraypy as hp

Creating a database/file
------------------------

Establish a server connection and create an hdf5 file:
   .. code-block:: python

    >>> conn = hp.connect('localhost', '2222')
    >>> conn.create_db('mydatabase.h5')

Connect to the created file:
   .. code-block:: python

    >>> db = conn.connect_db('mydatabase.h5')
    >>> db
    <HDF5 Group (db=mydatabase, path=/)>

Note that the database is a ``Group`` (the root group ``/``).


Working with groups and datasets
--------------------------------

To create a subgroup ``/mygroup`` run:
   .. code-block:: python

    >>> grp = db.create_group("mygrp")
    >>> grp
    <HDF5 Group (db=mydatabase, path=/mygrp)>

Now let's store a 2D array in that group:
   .. code-block:: python

    >>> data = np.array([[1, 2, 3], [4, 5, 6]])
    >>> dataset = grp.create_dataset('myarray', data=data)
    >>> dataset
    <HDF5 Dataset (db=mydatabase, path=/mygrp/myarray)>

Let's fetch the array from the server and read only its first dimension (note
that only the requested portion of the array is transferred over the network):
   .. code-block:: python

    >>> dataset = db['/mygrp/myarray']
    >>> dataset[:]
    array([[1, 2, 3],
           [4, 5, 6]])
    >>> dataset[0, :]
    array([1, 2, 3])

Numpy-like broadcasting allows us to overwrite only a portion of the array:
   .. code-block:: python

    >>> x = np.array([8, 9, 10])
    >>> dataset[0, :] = x
    >>> dataset[:]
    array([[ 8,  9, 10],
           [ 4,  5,  6]])


Node attributes (i.e., meta-data)
---------------------------------

Every node (Group or Dataset) can have a number of so-called *attributes*. An
attribute is a key/value pair, where the value can either be a single value
(string or number) or itself an n-dimensional array.

It works very much like a dictionary:
   .. code-block:: python

    >>> dataset.attrs['foo'] = "helloworld"
    >>> dataset.attrs['foo']
    "helloworld"
    >>> 'foo' in dst.attrs
    True
    >>> dst.attrs.keys()
    ['foo']

Using array values is also straightforward:
   .. code-block:: python

    >>> dst.attrs['num'] = np.array([0.1, 0.2, 0.5])
    >>> dst.attrs['num']
    array([0.1, 0.2, 0.5])
