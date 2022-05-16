
import xarray as xr
import pandas as pd
import os
import csv

import numpy as np


import matplotlib.pyplot as plt

imgsave = '/Users/tf/work/crimac/workshop/img/'

# Data  files
d = '/Users/tf/work/crimac/workshop'
f = '/2019/S2019847_0511/ACOUSTIC/GRIDDED/S2019847_0511'
sv_fname = d+ f + '_sv.zarr'
annotations_fname = d + f + '_labels.zarr'
schools_fname = d + f + '_labels.parquet.csv'

# Check if files are available
print(os.path.isdir(sv_fname))
print(os.path.isdir(annotations_fname))
print(os.path.isfile(schools_fname))

# Open the files
zarr_grid = xr.open_zarr(sv_fname)
zarr_pred = xr.open_zarr(annotations_fname)
#df = pd.read_csv(schools_fname)

annot0 = zarr_pred.annotation.shape[0]
annot1 = zarr_pred.annotation.shape[1]
annot2 = zarr_pred.annotation.shape[2]
print(zarr_pred.annotation.category)

print(annot0)
print(annot1)
print(annot2)
channels = zarr_grid.sv.shape[0]
totalpings = zarr_grid.sv.shape[1]
range = zarr_grid.sv.shape[2]
print("channels: "+ str(channels) )
print("pings: "+ str(totalpings ) )
print("range: "+ str(range) )

file = open(schools_fname)
csvreader = csv.reader(file)
header = next(csvreader)
print(header)
for row in csvreader:
    if(row[3]=="27"):
        print(row[0]+" "+row[3]+" "+row[7]+" "+row[8]+" "+row[11]+" "+row[12])
        #data_sv = zarr_grid.sv.isel(frequency=slice(0, 1), ping_time=slice(int(row[7]), int(row[8])), range=slice(int(row[11]), int(row[12])))
        data_annot = zarr_pred.annotation.isel(category=slice(3, 4), ping_time=slice(int(row[7]), int(row[8])), range=slice(int(row[11]), int(row[12])))
        #print(data_sv)
        #print(data_annot)
        extrapixels=10
        x1=int(row[7])-extrapixels
        x2=int(row[8])+extrapixels
        y1=int(row[11])-extrapixels
        y2=int(row[12])+extrapixels
        if(x1<0):
            x1=0
        if(x2>totalpings):
            x2=totalpings
        if (y1 < 0):
            y1 = 0
        if (y2 > range):
            y2 = range

        fig, axs = plt.subplots(nrows=2, ncols=4)
        axs[0, 0].imshow(10 * np.log10(zarr_grid.sv.isel(frequency=slice(0, 1), ping_time=slice(x1, x2), range=slice(y1,y2)) )[0], cmap='hot')
        axs[0, 1].imshow(10 * np.log10(zarr_grid.sv.isel(frequency=slice(1, 2), ping_time=slice(x1, x2), range=slice(y1,y2)) )[0], cmap='hot')
        axs[0, 2].imshow(10 * np.log10(zarr_grid.sv.isel(frequency=slice(2, 3), ping_time=slice(x1, x2), range=slice(y1,y2)) )[0], cmap='hot')
        axs[1, 0].imshow(10 * np.log10(zarr_grid.sv.isel(frequency=slice(3, 4), ping_time=slice(x1, x2), range=slice(y1,y2)) )[0], cmap='hot')
        axs[1, 1].imshow(10 * np.log10(zarr_grid.sv.isel(frequency=slice(4, 5), ping_time=slice(x1, x2), range=slice(y1,y2)) )[0], cmap='hot')
        axs[1, 2].imshow(10 * np.log10(zarr_grid.sv.isel(frequency=slice(5, 6), ping_time=slice(x1, x2), range=slice(y1,y2)) )[0], cmap='hot')

        axs[0, 3].imshow(10 * (zarr_pred.annotation.isel(category=slice(3, 4), ping_time=slice(x1, x2), range=slice(y1, y2 )))[0],cmap='hot')

        axs[1, 3].imshow(10 * (zarr_pred.annotation.isel(category=slice(3, 4), ping_time=slice(x1, x2), range=slice(y1, y2 )))[0],cmap='hot')

        # axs[1].set_ylabel('$y_{mf,auto,red}$')

        # axs[1].set_xlabel('Range (m)')

        #plt.show()
        plt.savefig(imgsave+"_"+row[0]+'sv.png', dpi=300)
        plt.close()

file.close()

#data1 = zarr_grid.sv.isel(frequency=slice(0, 1), ping_time=slice(0, 3000), range=slice(0,range))
