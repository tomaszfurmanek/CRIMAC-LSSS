# CRIMAC-LSSS

writeannotzarr.py
Reads a zarr file with raw data and creates an annotation zarr with the same dimensions from a LSSS parquet file

Each 'acoustic_category' is saved in its own layer
All values between 'mask_depth_upper' and 'mask_depth_lower' are set to 'proportion' for each 'acoustic_category' layer

'acoustic_category' = 0 is exclude and is not in its own layer but masks each of the other layers
Excluded areas are set to NaN in all 'acoustic_category' layers 


object array saves a unique object counter for each ( shipID and ObjectID) form annotation
Excluded areas are set to -1 in the object layer

objecttype is currently priority that indicates (layers = priority 3) , (school boxes = priority 2) and (Exclude = 0 )
(exclude is priority 1 in the work files) 


Example of created zarr:
________________________

```
<xarray.Dataset>
Dimensions:     (category: 3, ping_time: 898732, range: 2650)
Coordinates:
  * category    (category) int64 1 5018 12
  * ping_time   (ping_time) datetime64[ns] 2019-02-13T15:31:33.203000 ... 201...
  * range       (range) float64 -0.1888 0.0 0.1888 0.3776 ... 499.5 499.7 499.9
Data variables:
    annotation  (category, ping_time, range) float32 dask.array<chunksize=(1, 20000, 2650), meta=np.ndarray>
    object      (ping_time, range) int64 dask.array<chunksize=(20000, 2650), meta=np.ndarray>
    objecttype  (ping_time, range) int64 dask.array<chunksize=(20000, 2650), meta=np.ndarray>
    
```
________________________




Commandline parameters:

 -pings  sets the size of the chunks. The scripts writes data in the selected chunk size

```
python writeannot.py -shipID 847 -rawfile S2019847.zarr -parquet S2019847_work.parquet -savefile S2019847_work_annot2.zarr -pings 20000
```

to run on pallas.hi.no activate the crimac conda environment
```
source /localscratch_hdd/tomasz/anaconda3/
conda activate crimac
```
________________________


| Examples                  |objecttype| object | annotation layers|
| -------------             |--------  | ------ |     ----         |
| Sandeel (school)          | 1        |  1     |  1.0             |
| Herring (school)          | 1        |  2     |  1.0             |
| Mix herring/other (layer) | 1        |  2     |  0.7 / 0.3       |
| Bubbles (layer)           | 1        |  2     |  1.0             |
| Background                | 0        |  0     |  0.              |
| Exclude                   | 0        |  -1.   | NaN              |  

Should bubbles annotation be in its own layer? how is this labeled today in the work files?

TODO:

Add Channel dimension or layer , as annotation can be linked to layers

Add array with index position of the data range in the raw data . Each ping will have a value of where the data ends in the range dimension

Optional rules for objecttype
objecttype now has information if the coordinate is part of alayer or a schoolbox.
Currently the objecttype uses 3 for layer and 2 for schoolbox , this is the same as Priority variable in the work files
- Layer (2)
- Schoolbox(1)
- other (0)

 
Optional changes for  'object'
Today (category = -1) are saved in its own layer
- Schoolbox or layer without category (category = -1) : set value (-2)
- missing Work-file  : set value (-3)
- error reading Work-file  : set value (-4)
- missing data (NaN) due to error in range range (-5)
