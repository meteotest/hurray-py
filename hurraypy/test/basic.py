import numpy as np
import hurraypy as hp

conn = hp.connect('localhost', '2222')
conn.create_db('mydatabase.h5')

db = conn.connect_db('mydatabase.h5')

grp = db.create_group("mygrp")

data = np.array([[1, 2, 3], [4, 5, 6]])
ds = grp.create_dataset('myarray', data=data)

dataset = db['/mygrp/myarray']
print(dataset[:])
print(dataset[0, :])

x = np.array([8, 9, 10])
dataset[0, :] = x
dataset[:]
