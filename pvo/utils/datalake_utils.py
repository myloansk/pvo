#os is for Miscellaneous operating system interfaces
#datetime is used to manipulate datetime values
import os
from datetime import datetime

def get_dir_content(pPath):
    """
    get_dir_content:
        For a folder in the data lake, get the list of files it contains, including all subfolders. 
        Return Full File Name as well as Last Modified Date time as a generator object. 
        Output requires conversion into list for consumption.
    """
    #This for loop will check all directories and files inside the provided path
    #For each file it contains, return a 2-D array with the file name and the last modified date time
    #The consuming code will need to convert the generater object this returns to a list to consume it
    #The yield function is used to ensure the entire directory contents is scanned. If you used return it would stop after the first object encountered. 
    for dir_path in dbutils.fs.ls(pPath):
        if dir_path.isFile():
            #os.stat gets statistics on a path. st_mtime gets the most recent content modification date time
            yield [dir_path.path, datetime.fromtimestamp(os.stat('/' + dir_path.path.replace(':','')).st_mtime)]
        elif dir_path.isDir() and pPath != dir_path.path:
            #if the path is a directory, call the function on it again to check its contents
            yield from get_dir_content(dir_path.path)

def get_latest_modified_file_from_directory(pDirectory):
    """
    get_latest_modified_file_from_directory:
        For a given path to a directory in the data lake, return the file that was last modified. 
        Uses the get_dir_content function as well.
        Input path format expectation: '/mnt/datalake_rawdata'
            You can add sub directories as well, as long as you use a registered mount point
        Performance: With 588 files, it returns in less than 10 seconds on the lowest cluster size. 
    """
    #Call get_dir_content to get a list of all files in this directory and the last modified date time of each
    vDirectoryContentsList = list(get_dir_content(pDirectory))

    #Convert the list returned from get_dir_content into a dataframe so we can manipulate the data easily. Provide it with column headings. 
    #You can alternatively sort the list by LastModifiedDateTime and get the top record as well. 
    df = spark.createDataFrame(vDirectoryContentsList,['FullFilePath', 'LastModifiedDateTime'])

    #Get the latest modified date time scalar value
    maxLatestModifiedDateTime = df.agg({"LastModifiedDateTime": "max"}).collect()[0][0]

    #Filter the data frame to the record with the latest modified date time value retrieved
    df_filtered = df.filter(df.LastModifiedDateTime == maxLatestModifiedDateTime)
    
    #return the file name that was last modifed in the given directory
    return df_filtered.first()['FullFilePath']