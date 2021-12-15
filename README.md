# CRIMAC-LSSS

writeannotzarr.py
Reads a zarr file with raw data and creates an annotation zarr with the same dimensions from a LSSS parquet file

Each 'acoustic_category' is saved in its own layer

Script is setting all values between 'mask_depth_upper' and 'mask_depth_lower' to 'proportion' for each 'acoustic_category' layer
Object array saves shipID and ObjectID form annotation

Example of created zarr:
<xarray.Dataset>
Dimensions:     (category: 5, ping_time: 3986452, range: 2634)
Coordinates:
  * category    (category) <U4 '-1' '1' '27' '6009' '-1.0'
  * ping_time   (ping_time) datetime64[ns] 2019-05-07T19:08:06.188000 ... 201...
  * range       (range) float64 -0.19 0.0 0.19 0.3799 ... 499.6 499.8 500.0
Data variables:
    annotation  (category, ping_time, range) float64 dask.array<chunksize=(1, 20000, 2634), meta=np.ndarray>
    object      (ping_time, range) <U1 dask.array<chunksize=(20000, 2634), meta=np.ndarray>


python writeannotzarr.py -shipID 847 -rawfile S2019847.zarr -parquet S2019847_work.parquet -savefile S2019847_work_annot2.zarr -pings 20000
