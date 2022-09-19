#!/usr/bin/env python
# coding: utf-8

# In[128]:


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


# ## PNL Lab - Data Engineer Coding Assignment
# ### Helper functions

# In[142]:


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
    
    


# #### Task 1: API to download a file from Dropbox

# In[144]:


# Establish connection
dbx = dropbox_connection()


# In[101]:


# Task 1: Download
dbx.check_and_refresh_access_token()
# Make API call
metadata, res = dbx.files_download(path='/recruitment_project/enroll_data.csv')
# Extract pandas dataframe 
enrollmentData = getCSV(res)
enrollmentData.head()


# #### Task 1: Anonymize

# In[110]:


## Convert dates into appropriate columns
enrollmentData['date of consent'] = pd.to_datetime(enrollmentData['date of consent'])
enrollmentData['birth date'] = pd.to_datetime(enrollmentData['birth date'])
enrollmentData['age'] = np.floor((enrollmentData['date of consent'] - enrollmentData['birth date'])/np.timedelta64(1,"Y"))

# initilize a column for holding the offset in days
enrollmentData['days_offset'] = np.random.randint(35000, 50000, len(enrollmentData))
enrollmentData['date_of_consent_anon'] = enrollmentData['date of consent'] - enrollmentData['days_offset'].astype('timedelta64[D]')
enrollmentData.head()


# In[121]:


# Save enroll_data_anon_SY.csv

enrollmentData_anon = enrollmentData.drop(columns=['date of consent', 'birth date','days_offset']).rename(columns = {'date_of_consent_anon':'date of consent'})
enrollmentData_anon.to_csv('enroll_data_anon_SY.csv')
enrollmentData_anon.head()


# In[124]:


# Save enroll_data_offset_SY.csv
enrollmentData_offset = enrollmentData['days_offset']
enrollmentData_offset.to_csv('enroll_data_offset_SY.csv', columns = ['days_offset'])
enrollmentData_offset.head()


# #### Task 1: Upload

# In[139]:


# Upload anonymized enrollment data
updown.upload(dbx, 'enroll_data_anon_SY.csv', '/recruitment_project','','enroll_data_anon_SY.csv', overwrite = True)
# Upload offsets
updown.upload(dbx, 'enroll_data_offset_SY.csv', '/recruitment_project','','enroll_data_offset_SY.csv', overwrite = True)


# ### Task 2 

# #### Task 2, Workflow 1: Registration using ANTs
# 
#  See shell interface to quickly run the antsRegistration command: https://github.com/ANTsX/ANTs/blob/master/Scripts/newAntsExample.sh
# 
# The command used for registration:
# 
# `ants_registration.sh atlas-T1w.nii.gz given-T1w.nii.gz fastfortesting`
# 

# In[93]:


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


# #### Task 2, Workflow 2: Compute volumes for each label

# In[90]:


df = pd.DataFrame(columns=['LabelInteger', 'Volume'])
# df
labels = np.unique(label_img_data).astype(int)

for label in labels:
    label_mask = label_img_data==label
    multipliedMatrix = label_mask*T1_img_data
    unique, counts = np.unique(multipliedMatrix!=0, return_counts = True)
    df.loc[len(df)] = [label, counts[1]]
    
df.head()


# #### Task 2, Workflow 3 and 4: Match labels and save to CSV

# In[91]:


# Extracting freesurfer labels:
fs_labels = pd.read_csv('freesurfer_ROI_labels.csv', header = 1, names = ['raw','LabelInteger', 'Name', 'R', "G", "B", 'A']).drop(columns = ['raw', 'R', 'G', 'B', 'A'])

# Left join with labels
ROI_volume = pd.merge(df, fs_labels, how = 'left', on = 'LabelInteger')

# Write to CSV file
ROI_volume.to_csv('ROI_volume.csv', na_rep = "None", index = False)
ROI_volume.head()


# ####  Task 2, Workflow 5: Upload to dropbox

# In[145]:


updown.upload(dbx, 'ROI_volume.csv', '/recruitment_project','','ROI_volume_SY.csv', overwrite = True)

