# Data Engineer Coding Assignment


## Organisation
- Solutions to the coding assignment is in the jupyter notebook titled `PNL_test_1.ipynb`. 
    - The notebook has detailed comments on each task and sub-task. 
    - Incase there are issues with opening the note book, I have exported the file as a markdown, HTLM and a python file with the same name.
- `updown.py` contains wrapper functions for the Dropbox API and can be ignored. 

## Overview of Workflow

The `PNL_test_1.ipynb` jupyter notebook performs the following operations:

- Task 1:
    - API to download data from Dropbox. This is authenticated using an access token from  a personal application in dropbox.com/developers.
    - Anonymize: 
        - The date of consent is disguised using a "day offset" that is randomly generated.  The offsets are stored in enroll_data_offset_SY.csv. 
        - The date of birth is disguised by calculating the age.
    - Upload files: Both csv files (anonymized enrollment data and the offsets) are uploaded back to the dropbox folder.

- Task 2:
    - Register the T1 weighted image to the T1w-atlas file using the ANTs registration tool.
    - Compute volume for each label. 
        - Volume is calculated as follows. 
            - For each integer label, create a binary mask. 
            - Multiply the binary mask with the registered T1 image and calculate the number voxels that are non-zero. This number is defined as the volume. The units of the volume is determined by the size of the volxes in the atlas of interest. 
    - Match integer labels to the freesurfer ROI names.
    - Upload the ROI volume calculations. 