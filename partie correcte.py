#!/usr/bin/env python
# coding: utf-8

# In[1]:


from nilearn import datasets

haxby_dataset = datasets.fetch_haxby()
fmri_filename = haxby_dataset.func[0]
print(f"First subject functional nifti images (4D) are at: {fmri_filename}")


# In[2]:


from nilearn import plotting
from nilearn.image import mean_imgfrom nilearn import datasets

haxby_dataset = datasets.fetch_haxby()
fmri_filename = haxby_dataset.func[0]
print(f"First subject functional nifti images (4D) are at: {fmri_filename}")

plotting.view_img(mean_img(fmri_filename), threshold=None)


# In[3]:


mask_filename = haxby_dataset.mask_vt[0]
# 0 : le premier sujet
# Let's visualize it, using the subject's anatomical image as a
# background
plotting.plot_roi(mask_filename, bg_img=haxby_dataset.anat[0], cmap="Paired")


# In[4]:


import pandas as pd

# Load behavioral information
behavioral = pd.read_csv(haxby_dataset.session_target[0], delimiter=" ")
print(behavioral)


# In[5]:


conditions = behavioral["labels"]
print(conditions)


# In[6]:


condition_mask = conditions.isin(["face", "cat"])
from nilearn.image import index_img

fmri_niimgs = index_img(fmri_filename, condition_mask)
conditions = conditions[condition_mask]
# Convert to numpy array
conditions = conditions.values
print(conditions.shape)


# In[ ]:




