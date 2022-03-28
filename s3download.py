import boto3
import os, sys

# downloads a folder from s3 bucket (works only on linux or osx) 
# for windows you have to convert the path separator in the save path
# as s3 do not have folders the folders are generated on the local drive from the key prefix of each file
def boto3download(host,access_key,secret_key,bucketname, s3folder,savefolder):
    s3=boto3.resource('s3',aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name='us-east-1',endpoint_url=host,)
    bucket = s3.Bucket(bucketname)

    for my_bucket_object in bucket.objects.filter(Prefix=s3folder):
        savefile=my_bucket_object.key.replace("gpfs0-crimac-scratch/", "")
        path = os.path.join(savefolder, savefile)
        dirname = os.path.dirname(path)+os.sep
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        print(path)
        bucket.download_file(my_bucket_object.key, path)

        
        
        
# boto3download(host,access_key,secret_key,bucketname, s3folder,savefolder)
#
# access_key : is the username
# secret_key : is the password 
# bucketname : is the s3 bucket (crimac-scratch at CES at HI)
# s3folder : all s3  crimac folders at HI start with gpfs0-crimac-scratch/
# savefolder : folder where you want to save all files in 2019/S2019847/ACOUSTIC/GRIDDED/
boto3download('https://s3.hi.no','use_USERID_here', 'use_PASSWORD_here', 'crimac-scratch', 'gpfs0-crimac-scratch/2019/S2019847/ACOUSTIC/GRIDDED/', '/Users/tf/work/crimac/')



# s3 does not have folders but each file has a key
# a filter with the prefix 'gpfs0-crimac-scratch/2019/S2019847/ACOUSTIC/GRIDDED/' will give you a list of all files that has this prefix
# and is the same as all files that are in this "folder" and all of its subfolders



# these are the s3 folders at HI. s3folder
# 'gpfs0-crimac-scratch/2019/S2019847/ACOUSTIC/GRIDDED/'
# 'gpfs0-crimac-scratch/2018/S2018823/ACOUSTIC/GRIDDED/'
# 'gpfs0-crimac-scratch/2017/S2017843/ACOUSTIC/GRIDDED/'
# 'gpfs0-crimac-scratch/2016/S2016837/ACOUSTIC/GRIDDED/'
# 'gpfs0-crimac-scratch/2015/S2015837/ACOUSTIC/GRIDDED/'
# 'gpfs0-crimac-scratch/2014/S2014807/ACOUSTIC/GRIDDED/'
# 'gpfs0-crimac-scratch/2013/S2013842/ACOUSTIC/GRIDDED/'
# 'gpfs0-crimac-scratch/2012/S2012837/ACOUSTIC/GRIDDED/'
