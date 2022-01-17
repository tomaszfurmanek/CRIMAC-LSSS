import time
from timeit import timeit

import xarray as xr
import math
import numpy as np
from numcodecs import Blosc
import pyarrow.parquet as pq
import dask.array as dask

import sys

#  to run on pallas.hi.no activate the crimac conda environment
#  source /localscratch_hdd/tomasz/anaconda3/
#  conda activate crimac



shipID="847"
rawfile = '/Users/tf/work/crimac/test/out/out.zarr'
parquetfile2 = '/Users/tf/work/crimac/test/out/out_work.parquet'
savefile2 = '/Users/tf/work/crimac/test/out/out_annot7600ck.zarr'

# chunk size for ping dimension
pingchunk = 20000
rangechunk = 2643


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


# Create annotation zarr for chunk
#
def write_annot(rawzarrfile,start,end,rangeend,savefile,writemode ):
    pingerror = 0
    pingok = 0
    z = xr.open_zarr(rawzarrfile, chunks={'ping_time':'50000'})
    data1 = z.sv.isel(frequency=slice(0, 1), ping_time=slice(start, end), range=slice(0, rangeend))
    #array with pingtime
    data_ping = np.asarray(data1.ping_time)
    #array with range depth
    data_range = np.asarray(data1.range)

    rawpinglist = np.asarray(data_ping)

    lsss_tmp = np.zeros([len(category),len(data_ping), len(data_range)] , dtype=np.float32)
    lsssobject_tmp = np.zeros([len(data_ping), len(data_range)], dtype=int)
    lsssobjecttype_tmp = np.zeros([len(data_ping), len(data_range)], dtype=int)

    objectnum={}
    objectnumcounter=1

    pingnum = 0
    hits = 0
    # loop through pings in chunk
    for pingx in rawpinglist:

        p3 = str(pingx).replace('T', ' ')[0:26]
        #program_starts = time.time()

        # Start with priority 3 : layers
        # then priority 2 boxes
        # in the end overite all category layers with exclude priority 1
        # workannot dictionaries are in the above order
        type=3
        for work in workannot:
            rows=[]
            if p3 in work:
                # get all the annotations in a list from the parquet file for the current ping
                rows = work[p3]
                hits = hits+1
                pingok = pingok + 1
                del work[p3]
                #print("remaining annotation pings : " + str(len(work)))
            else:
                pingerror=pingerror+1
            # for each annotation registered for the ping set the correct value for each category
            # between mask_depth_upper and mask_depth_lower
            for row in rows:

                up = float(str(row['mask_depth_upper']))
                lo = float(str(row['mask_depth_lower']))
                scale = (float(len(data_range)) / 500.0)
                up2 = up * scale
                lo2 = lo * scale
                #rangepos = int(up2 - 5)
                #end=int(lo2 + 5)
                startpos = int(up2)
                endpos = int(lo2)
                if endpos >= len(data_range):
                    endpos = len(data_range)
                size = endpos - startpos
                if pingnum > -1:
                    ct = 0;
                    # write the annotation for the correct category
                    for ctg in category:
                        if str(row['acoustic_category']) == ctg:
                            propval= float(row['proportion'])
                            lsss_tmp[ct, pingnum, startpos:endpos] = np.full((size),propval)
                        ct = ct + 1
                    # get the unique number for each object_id
                    o1=shipID + "__" + str(row['object_id'])
                    if o1 in objectnum :
                        objn=objectnum[o1]
                    else:
                        objectnumcounter=objectnumcounter+1
                        objn=objectnumcounter
                        objectnum[o1]=objn

                    # set the object number in lsssobject
                    lsssobject_tmp[pingnum,startpos:endpos] = objn
                    lsssobjecttype_tmp[pingnum,startpos:endpos] = type

                    # Set the exclude masks for all categories
                    if type == 0 :
                        ct = 0;
                        for ctg in category:
                            lsss_tmp[ct, pingnum, startpos:endpos] = np.nan
                            ct = ct + 1
                        lsssobject_tmp[pingnum, startpos:endpos] = -1
                        lsssobjecttype_tmp[pingnum, startpos:endpos] = type


            #now = time.time()
            #print(len(rows))
            #print("time : {0} seconds  ".format(now - program_starts))
            type = type - 1
        pingnum = pingnum + 1



    print("dask")
    lsss_dask = dask.from_array(lsss_tmp, chunks=(-1,-1,15))
    print("xarray")
    lsss = xr.DataArray(name="lsss", data=lsss_dask,
                        dims=['category','ping_time', 'range'],
                        coords={'category': category,'ping_time': data_ping, 'range': data_range})
    lsssobject_dask = dask.from_array(lsssobject_tmp, chunks=(-1,15))
    lsssobjecttype_dask = dask.from_array(lsssobjecttype_tmp, chunks=(-1,15))
    lsssobject = xr.DataArray(name="lsssobject", data=lsssobject_dask, dims=['ping_time', 'range'],
                    coords={'ping_time': data_ping, 'range': data_range})
    lsssobjecttype = xr.DataArray(name="lsssobjecttype", data=lsssobjecttype_dask, dims=['ping_time', 'range'],
                    coords={'ping_time': data_ping, 'range': data_range})
    print("dataset")
    ds4 = xr.Dataset(
        data_vars=dict(
            annotation=(["category","ping_time", "range"], lsss),
            object=(["ping_time", "range"], lsssobject),
            objecttype=(["ping_time", "range"], lsssobjecttype),
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

    print("write start")
    if writemode==0:
        # first write to a new file
        ds4.to_zarr(savefile, mode="w", encoding=encoding)
    else:
        # append
        ds4.to_zarr(savefile, mode="a",   append_dim="ping_time")
    print("write end")




# open parquet file with annotations
table1 = pq.read_table( parquetfile2)
t1 = table1.to_pandas()
print("___")

# open raw data zarr to get the dimensions
z = xr.open_zarr(rawfile, chunks={'ping_time':'50000'})

totalpings = z.sv.shape[1]
rangechunk = z.sv.shape[2]

print(totalpings)
print(rangechunk)

# Dictionaries to store annotation based on priority
work3 = {}
work2 = {}
work1 = {}
workexclude = {}

workannot=[]
workannot.append(work3)
workannot.append(work2)
workannot.append(work1)
workannot.append(workexclude)

acoustic_category = {}
print(t1)

pcount=0
for index, row in t1.iterrows():
    pcount=pcount+1
    c1 = int(float(row['acoustic_category']))
    p1 = int(row['priority'])
    #put all annotations in a list and save the list in the dictionary for each pingtime key
    if c1 > 0 or c1 == -1:
        if str(row['ping_time'])[0:26] in workannot[3-p1]:
            workannot[3-p1][str(row['ping_time'])[0:26]].append(row)
        else:
            annotlist = []
            annotlist.append(row)
            workannot[3-p1][str(row['ping_time'])[0:26]] = annotlist
        acoustic_category[str(row['acoustic_category'])] = str(row['priority'])
    else:
        # save exclude annotation in its own dictionary  , pingtime key
        if str(row['ping_time'])[0:26] in workannot[3]:
            workannot[3][str(row['ping_time'])[0:26]].append(row)
        else:
            annotlist = []
            annotlist.append(row)
            workannot[3][str(row['ping_time'])[0:26]] = annotlist

    if pcount%500000 ==0:
        print("  annotation pings "+str(len(workannot[0])) +" "+str(len(workannot[1])) +" "+str(len(workannot[2])) +" "+str(len(workannot[3])))


category=[]
# we want to save each category as an int value
for key1 in acoustic_category:
    category.append(int(float(key1)))
print(category)
print(acoustic_category)

numberofreads = math.ceil(totalpings / pingchunk)
print(numberofreads)
i = 0
print("remaining annotation pings " + str(len(workannot[0])) + " " + str(len(workannot[1])) + " " + str( len(workannot[2])) + " " + str(len(workannot[3])))

#Loop though all pings in the raw file in chunks and save the annotation
while( i < totalpings):

    print(str(i)+" "+ str(i+pingchunk))

    if i==0:
        # first write
        print(str(i) + " 1 " + str(i + pingchunk))
        write_annot(rawfile, i, (i + pingchunk),rangechunk, savefile2, 0)
    else:
        # append to file
        print(str(i) + " x " + str(i + pingchunk))
        write_annot(rawfile, i, (i + pingchunk),rangechunk, savefile2, 1)
    i += pingchunk
    print("remaining annotation pings "+str(len(workannot[0])) +" "+str(len(workannot[1])) +" "+str(len(workannot[2])) +" "+str(len(workannot[3])))

z2 = xr.open_zarr(savefile2 )
print(z2)
