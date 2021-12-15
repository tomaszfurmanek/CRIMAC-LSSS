import xarray as xr
import math
import numpy as np
from numcodecs import Blosc
import pyarrow.parquet as pq
import dask.array as dask

import sys


shipID="847"
rawfile = '/Users/tf/work/crimac/2019/S2019847.zarr'
parquetfile2 = '/Users/tf/work/crimac/2019/S2019847_work.parquet'
savefile2 = '/Users/tf/work/crimac/2019/S2019847_annot4.zarr'


pingchunk = 20000

if("-shipID" in  sys.argv):
    shipID = str(sys.argv[sys.argv.index("-shipID") + 1])
if("-rawfile" in  sys.argv):
    rawfile = sys.argv[sys.argv.index("-rawfile") + 1]
if("-parquet" in  sys.argv):
    parquetfile2  = sys.argv[sys.argv.index("-parquet") + 1]
if("-savefile" in  sys.argv):
    savefile2 = sys.argv[sys.argv.index("-savefile") + 1]
if("-pings" in  sys.argv):
    pingchunk = int(sys.argv[sys.argv.index("-pings") + 1])




rangechunk = 2643

def write_annot(rawzarrfile,start,end,rangeend,savefile,writemode ):
    pingerror = 0
    pingok = 0
    z = xr.open_zarr(rawzarrfile, chunks={'ping_time':'50000'})
    data1 = z.sv.isel(frequency=slice(0, 1), ping_time=slice(start, end), range=slice(0, rangeend))
    data_ping = np.asarray(data1.ping_time)
    data_range = np.asarray(data1.range)
    print(data_ping[0])
    print(data_ping[len(data_ping)-1])

    rawpinglist = np.asarray(data_ping)

    lsss_tmp = np.empty([len(category),len(data_ping), len(data_range)] )
    lsssobject_tmp = np.empty([len(data_ping), len(data_range)], dtype=str)

    pingnum = 0
    hits = 0

    for pingx in rawpinglist:
        p3 = str(pingx).replace('T', ' ')[0:26]

        rows=[]
        if p3 in work:
            rows=work[p3]
            hits=hits+1
            pingok = pingok + 1
            del work[p3]
        else:
            pingerror=pingerror+1

        for row in rows:
            up = float(str(row['mask_depth_upper']))
            lo = float(str(row['mask_depth_lower']))
            scale = (float(len(data_range)) / 500.0)
            up2 = up * scale
            lo2 = lo * scale
            rangepos = int(up2 - 5)
            end=int(lo2 + 5)
            if end >= len(data_range):
                end=len(data_range);

            while rangepos < end:
                if float(data_range[rangepos]) > float(row['mask_depth_upper']) and float(data_range[rangepos]) < float(row['mask_depth_lower']):
                    if pingnum > -1:
                        ct=0;
                        for ctg in category:
                            if str(row['acoustic_category'])== ctg:
                                lsss_tmp[ct][pingnum][rangepos] = float(row['proportion'])
                            ct=ct+1
                    lsssobject_tmp[pingnum][rangepos] = shipID+"__"+str(row['object_id'])
                rangepos += 1
        pingnum = pingnum + 1

    lsss_dask = dask.from_array(lsss_tmp, chunks=(-1,-1,15))
    lsss = xr.DataArray(name="lsss", data=lsss_dask,
                        dims=['category','ping_time', 'range'],
                        coords={'category': category,'ping_time': data_ping, 'range': data_range})
    lsssobject_dask = dask.from_array(lsssobject_tmp, chunks=(-1,15))
    lsssobject = xr.DataArray(name="lsssobject", data=lsssobject_dask, dims=['ping_time', 'range'],
                    coords={'ping_time': data_ping, 'range': data_range})
    ds4 = xr.Dataset(
        data_vars=dict(
            annotation=(["category","ping_time", "range"], lsss),
            object=(["ping_time", "range"], lsssobject),
        ),
        coords=dict(
            category=category,
            ping_time=data_ping,
            range=data_range,
        )
    )
    ds4 = ds4.chunk({"category": 1, "range": ds4.range.shape[0], "ping_time": pingchunk})
    compressor = Blosc(cname='zstd', clevel=3, shuffle=Blosc.BITSHUFFLE)
    encoding = {var: {"compressor": compressor} for var in ds4.data_vars}

    if writemode==0:
        ds4.to_zarr(savefile, mode="w", encoding=encoding)
    else:
        ds4.to_zarr(savefile, mode="a",   append_dim="ping_time")

table1 = pq.read_table( parquetfile2)
t1 = table1.to_pandas()

work = {}
acoustic_category = {}


for index, row in t1.iterrows():
    if str(row['ping_time'])[0:26] in work:
        work[str(row['ping_time'])[0:26]].append(row)
    else:
        annotlist = []
        annotlist.append(row)
        work[str(row['ping_time'])[0:26]] = annotlist
    acoustic_category[str(row['acoustic_category'])] = str(row['priority'])


category=[]
for key1 in acoustic_category:
    category.append(key1)

z = xr.open_zarr(rawfile, chunks={'ping_time':'50000'})

totalpings = z.sv.shape[1]
rangechunk = z.sv.shape[2]

print(totalpings)
print(rangechunk)
numberofreads = math.ceil(totalpings / pingchunk)
print(numberofreads)
i = 0
print("remaining annotation pings " + str(len(work)))
while( i < totalpings):

    print(str(i)+" "+ str(i+pingchunk))

    if i==0:
        print(str(i) + " 1 " + str(i + pingchunk))
        write_annot(rawfile, i, (i + pingchunk),rangechunk, savefile2, 0)
    else:
        print(str(i) + " x " + str(i + pingchunk))
        write_annot(rawfile, i, (i + pingchunk),rangechunk, savefile2, 1)
    i += pingchunk
    print("remaining annotation pings "+str(len(work)))

z2 = xr.open_zarr(savefile2 )
print(z2)