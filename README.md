# CRIMAC-LSSS

writeannot2.py
Reads a zarr file with raw data and creates an annotation zarr with the same dimensions from a LSSS parquet file

python writeannot2.py -shipID 847 -rawfile S2019847.zarr -parquet S2019847_work.parquet -savefile S2019847_work_annot2.zarr -pings 20000
