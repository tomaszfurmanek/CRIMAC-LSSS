import xarray as xr

import numpy as np

# test code for reading an annotation zarr file

z = xr.open_zarr('/Users/tf/work/crimac/herringtest/S2019842_PVENDLA_work_annot_460000ping.zarr' )

print(z)
print("----------")

dataannot = z.annotation.isel(category=slice(1, 2), ping_time=slice(0, 20000), range=slice(0, 2643))

annot= np.asarray(dataannot)
data_ping = np.asarray(dataannot.ping_time)
data_range = np.asarray(dataannot.range)
print(annot)

dataobject = z.object.isel(  ping_time=slice(0, 20000), range=slice(0, 2643))

object= np.asarray(dataobject)
ip=0
for p in data_ping:
    ir=0
    for r in data_range:
        if(annot[0][ip][ir]>0):
            print(str(p)+" "+str(r)+" "+str(object[ip][ir])+" "+str(annot[0][ip][ir]))
        ir = ir + 1
    ip=ip+1
