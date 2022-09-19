```python
import pandas as pd
import dropbox 
from dropbox.exceptions import AuthError
from pathlib import Path
import time
import os
import updown
import csv
import math
import numpy as np
import random
import nibabel as nib
import matplotlib.pyplot as plt
import datetime

```

## PNL Lab - Data Engineer Coding Assignment
### Helper functions


```python
from dropbox.auth import AuthError


DROPBOX_TOKEN = 'sl.BPmLXPvB6S7GmhSpdI2S53AKnB0PJB2xWqxgXTi7KAUrm8DncWQN0IB1GJzysJjqucSJSS6JNxAzRzLfkrq_7rF81sXRzwOq-b7vrgHfljW1B1_m2fGpfTdMDTFjoUcSnS5qsW9a'

# Connect to the Dropbox API
def dropbox_connection():
    """Establish connection to Dropbox"""
    try:
        dbx = dropbox.Dropbox(DROPBOX_TOKEN)
    except AuthError as e:
        print("Error: Problem connecting using the given token.")
    return dbx

def getCSV(res) -> pd:
    """Function to extract Pandas dataframe from API call"""
    enrollmentData = res.content.decode().splitlines() # Organize data
    fileColumns = enrollmentData[0].split(',')
    enrollmentData = [x.split(',') for x in enrollmentData[1:len(enrollmentData)]] # Skip the first row with column names
    p = pd.DataFrame(enrollmentData)
    p.columns = fileColumns
    return(p)
def show_slices(slices):
   """Function to display row of image slices"""
   fig, axes = plt.subplots(1, len(slices))
   for i, slice in enumerate(slices):
       axes[i].imshow(slice.T, cmap="gray", origin="lower")
    
    
```

#### Task 1: API to download a file from Dropbox


```python
# Establish connection
dbx = dropbox_connection()
```


```python
# Task 1: Download
dbx.check_and_refresh_access_token()
# Make API call
metadata, res = dbx.files_download(path='/recruitment_project/enroll_data.csv')
# Extract pandas dataframe 
enrollmentData = getCSV(res)
enrollmentData.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>site ID</th>
      <th>date of consent</th>
      <th>cohort</th>
      <th>birth date</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>BWH</td>
      <td>1/1/2020</td>
      <td>CHR</td>
      <td>1990-01-01</td>
    </tr>
    <tr>
      <th>1</th>
      <td>BWH</td>
      <td>1/2/2020</td>
      <td>CHR</td>
      <td>1989-01-02</td>
    </tr>
    <tr>
      <th>2</th>
      <td>BWH</td>
      <td>1/2/2020</td>
      <td>HC</td>
      <td>1998-01-03</td>
    </tr>
    <tr>
      <th>3</th>
      <td>BWH</td>
      <td>1/2/2020</td>
      <td>HC</td>
      <td>1987-01-04</td>
    </tr>
    <tr>
      <th>4</th>
      <td>BWH</td>
      <td>1/2/2020</td>
      <td>CHR</td>
      <td>1986-01-05</td>
    </tr>
  </tbody>
</table>
</div>



#### Task 1: Anonymize


```python

## Convert dates into appropriate columns
enrollmentData['date of consent'] = pd.to_datetime(enrollmentData['date of consent'])
enrollmentData['birth date'] = pd.to_datetime(enrollmentData['birth date'])
enrollmentData['age'] = np.floor((enrollmentData['date of consent'] - enrollmentData['birth date'])/np.timedelta64(1,"Y"))

# initilize a column for holding the offset in days
enrollmentData['days_offset'] = np.random.randint(35000, 50000, len(enrollmentData))
enrollmentData['date_of_consent_anon'] = enrollmentData['date of consent'] - enrollmentData['days_offset'].astype('timedelta64[D]')
enrollmentData.head()

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>site ID</th>
      <th>date of consent</th>
      <th>cohort</th>
      <th>birth date</th>
      <th>age</th>
      <th>days_offset</th>
      <th>date_of_consent_anon</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>BWH</td>
      <td>2020-01-01</td>
      <td>CHR</td>
      <td>1990-01-01</td>
      <td>29.0</td>
      <td>41915</td>
      <td>1905-03-30</td>
    </tr>
    <tr>
      <th>1</th>
      <td>BWH</td>
      <td>2020-01-02</td>
      <td>CHR</td>
      <td>1989-01-02</td>
      <td>30.0</td>
      <td>42562</td>
      <td>1903-06-23</td>
    </tr>
    <tr>
      <th>2</th>
      <td>BWH</td>
      <td>2020-01-02</td>
      <td>HC</td>
      <td>1998-01-03</td>
      <td>21.0</td>
      <td>46836</td>
      <td>1891-10-09</td>
    </tr>
    <tr>
      <th>3</th>
      <td>BWH</td>
      <td>2020-01-02</td>
      <td>HC</td>
      <td>1987-01-04</td>
      <td>32.0</td>
      <td>42054</td>
      <td>1904-11-12</td>
    </tr>
    <tr>
      <th>4</th>
      <td>BWH</td>
      <td>2020-01-02</td>
      <td>CHR</td>
      <td>1986-01-05</td>
      <td>33.0</td>
      <td>36960</td>
      <td>1918-10-24</td>
    </tr>
  </tbody>
</table>
</div>




```python
# Save enroll_data_anon_SY.csv

enrollmentData_anon = enrollmentData.drop(columns=['date of consent', 'birth date','days_offset']).rename(columns = {'date_of_consent_anon':'date of consent'})
enrollmentData_anon.to_csv('enroll_data_anon_SY.csv')
enrollmentData_anon.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>site ID</th>
      <th>cohort</th>
      <th>age</th>
      <th>date of consent</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>BWH</td>
      <td>CHR</td>
      <td>29.0</td>
      <td>1905-03-30</td>
    </tr>
    <tr>
      <th>1</th>
      <td>BWH</td>
      <td>CHR</td>
      <td>30.0</td>
      <td>1903-06-23</td>
    </tr>
    <tr>
      <th>2</th>
      <td>BWH</td>
      <td>HC</td>
      <td>21.0</td>
      <td>1891-10-09</td>
    </tr>
    <tr>
      <th>3</th>
      <td>BWH</td>
      <td>HC</td>
      <td>32.0</td>
      <td>1904-11-12</td>
    </tr>
    <tr>
      <th>4</th>
      <td>BWH</td>
      <td>CHR</td>
      <td>33.0</td>
      <td>1918-10-24</td>
    </tr>
  </tbody>
</table>
</div>




```python
# Save enroll_data_offset_SY.csv
enrollmentData_offset = enrollmentData['days_offset']
enrollmentData_offset.to_csv('enroll_data_offset_SY.csv', columns = ['days_offset'])
enrollmentData_offset.head()
```




    0    41915
    1    42562
    2    46836
    3    42054
    4    36960
    Name: days_offset, dtype: int64



#### Task 1: Upload


```python

# Upload anonymized enrollment data
updown.upload(dbx, 'enroll_data_anon_SY.csv', '/recruitment_project','','enroll_data_anon_SY.csv', overwrite = True)
# Upload offsets
updown.upload(dbx, 'enroll_data_offset_SY.csv', '/recruitment_project','','enroll_data_offset_SY.csv', overwrite = True)

```

    Total elapsed time for upload 233009 bytes: 0.966
    uploaded as b'enroll_data_anon_SY.csv'
    Total elapsed time for upload 86309 bytes: 0.642
    uploaded as b'enroll_data_offset_SY.csv'





    FileMetadata(client_modified=datetime.datetime(2022, 9, 19, 3, 4, 57), content_hash='8866a622fd5305019358657e5177517585a43f91275483f17159a22c6e648c96', export_info=NOT_SET, file_lock_info=NOT_SET, has_explicit_shared_members=NOT_SET, id='id:9wk-zqcqukAAAAAAAAABRw', is_downloadable=True, media_info=NOT_SET, name='enroll_data_offset_SY.csv', parent_shared_folder_id='9340197792', path_display='/recruitment_project/enroll_data_offset_SY.csv', path_lower='/recruitment_project/enroll_data_offset_sy.csv', preview_url=NOT_SET, property_groups=NOT_SET, rev='015e8ff3435eda3000000022cb81ba0', server_modified=datetime.datetime(2022, 9, 19, 3, 21, 13), sharing_info=FileSharingInfo(modified_by='dbid:AAAGGJWE6E8XzEVUTRJVRFMJByg6qL9w-Us', parent_shared_folder_id='9340197792', read_only=False), size=86309, symlink_info=NOT_SET)



### Task 2 

#### Task 2, Workflow 1: Registration using ANTs

 See shell interface to quickly run the antsRegistration command: https://github.com/ANTsX/ANTs/blob/master/Scripts/newAntsExample.sh

The command used for registration:

`ants_registration.sh atlas-T1w.nii.gz given-T1w.nii.gz fastfortesting`



```python
# Verify size of the T1 image. Further verfication was done by overlaying the
# atlas labels on top of the registered image using freeview.
T1Image_path: Path = Path('../scans/atlas-T1w_fixed_given-T1w_moving_setting_is_fastfortesting_warped.nii.gz')
atlasLabels_path: Path = Path('../scans/atlas-integer-labels.nii.gz')

T1_img = nib.load(T1Image_path)
T1_img_data = T1_img.get_fdata()

label_img = nib.load(atlasLabels_path)
label_img_data = label_img.get_fdata()
print(f'Size of T1 image volume: {T1_img_data.shape}')
print(f'Size of the atlas label volume: {label_img_data.shape}')

```

    Size of T1 image volume: (182, 218, 182)
    Size of the atlas label volume: (182, 218, 182)


#### Task 2, Workflow 2: Compute volumes for each label


```python
df = pd.DataFrame(columns=['LabelInteger', 'Volume'])
# df
labels = np.unique(label_img_data).astype(int)

for label in labels:
    label_mask = label_img_data==label
    multipliedMatrix = label_mask*T1_img_data
    unique, counts = np.unique(multipliedMatrix!=0, return_counts = True)
    df.loc[len(df)] = [label, counts[1]]
    
df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>LabelInteger</th>
      <th>Volume</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>6741512</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>15644</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>1500</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>8851</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>13711</td>
    </tr>
  </tbody>
</table>
</div>



#### Task 2, Workflow 3 and 4: Match labels and save to CSV


```python
# Extracting freesurfer labels:
fs_labels = pd.read_csv('freesurfer_ROI_labels.csv', header = 1, names = ['raw','LabelInteger', 'Name', 'R', "G", "B", 'A']).drop(columns = ['raw', 'R', 'G', 'B', 'A'])

# Left join with labels
ROI_volume = pd.merge(df, fs_labels, how = 'left', on = 'LabelInteger')

# Write to CSV file
ROI_volume.to_csv('ROI_volume.csv', na_rep = "None", index = False)
ROI_volume.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>LabelInteger</th>
      <th>Volume</th>
      <th>Name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>6741512</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>15644</td>
      <td>Left-Cerebral-Exterior</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>1500</td>
      <td>Left-Cerebral-White-Matter</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>8851</td>
      <td>Left-Cerebral-Cortex</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>13711</td>
      <td>Left-Lateral-Ventricle</td>
    </tr>
  </tbody>
</table>
</div>



####  Task 2, Workflow 5: Upload to dropbox


```python
updown.upload(dbx, 'ROI_volume.csv', '/recruitment_project','','ROI_volume_SY.csv', overwrite = True)
```

    Total elapsed time for upload 1204 bytes: 0.866
    uploaded as b'ROI_volume_SY.csv'





    FileMetadata(client_modified=datetime.datetime(2022, 9, 19, 2, 32, 21), content_hash='0b6e67849f0599ccd2b995aa810ba4b8b95a60f9437b53c44d57c09ce661b794', export_info=NOT_SET, file_lock_info=NOT_SET, has_explicit_shared_members=NOT_SET, id='id:9wk-zqcqukAAAAAAAAABSA', is_downloadable=True, media_info=NOT_SET, name='ROI_volume_SY.csv', parent_shared_folder_id='9340197792', path_display='/recruitment_project/ROI_volume_SY.csv', path_lower='/recruitment_project/roi_volume_sy.csv', preview_url=NOT_SET, property_groups=NOT_SET, rev='015e907c79d2cec000000022cb81ba0', server_modified=datetime.datetime(2022, 9, 19, 13, 35, 6), sharing_info=FileSharingInfo(modified_by='dbid:AAAGGJWE6E8XzEVUTRJVRFMJByg6qL9w-Us', parent_shared_folder_id='9340197792', read_only=False), size=1204, symlink_info=NOT_SET)


