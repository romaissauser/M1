import os
import sys
import nibabel
import numpy
import nilearn
import nilearn.plotting
from nilearn.input_data import NiftiMasker
from nilearn.input_data import NiftiLabelsMasker, NiftiMapsMasker
from nilearn import image
from nilearn import datasets
import pylab as plt

class MRI(object):

    def __init__(self, filename, result_path):

        self.data_path = os.path.dirname(filename)
        self.filename = filename
        self.result_path = result_path
        assert os.path.exists(self.filename)
        self._nifti_filename = None
        self._data = None
        self._brain_mask = None
        self._mask_indices = None
        self._low_pass = 0.08
        self._high_pass = 0.009
        self._t_r = self.data.header.get_zooms()[3]
        self.confound_columns = ['a_comp_cor_00', 'a_comp_cor_01', 'a_comp_cor_02', 'a_comp_cor_03', 'a_comp_cor_04', 
        'a_comp_cor_05', 'cosine00', 'cosine01', 'cosine02', 'cosine03', 'cosine04', 'cosine05', 'trans_x', 
        'trans_y', 'trans_z', 'rot_x', 'rot_y', 'rot_z']
        if self.is_preprocessed:
            self._masker = NiftiMasker(mask_img=self.brain_mask, standardize=True, mask_strategy='epi')

            # self.dataset = datasets.fetch_atlas_msdl()
            # self.labels = self.dataset.labels
            # self.atlas_filename = self.dataset.maps
            # self._parcelizer = NiftiMapsMasker(maps_img=self.atlas_filename, standardize=True,
            #                    memory='nilearn_cache', verbose=5, mask_img=self.brain_mask)

            self.dataset = datasets.fetch_atlas_harvard_oxford('cort-maxprob-thr25-2mm')
            self.atlas_filename = self.dataset.filename
            self.labels = self.dataset.labels[1:]
            
            #self.dataset = datasets.fetch_atlas_juelich('maxprob-thr25-2mm')
            #self.labels = self.dataset.labels

            self._parcelizer = NiftiLabelsMasker(labels_img=self.atlas_filename, standardize=True,
                               memory='nilearn_cache', verbose=5, mask_img=self.brain_mask)
            
    @property
    def duration(self):
        return self.data.shape[-1]

    @property
    def shape(self):
        return self.data.shape

    @property
    def data(self):
        if self._data is None:
            self._data = nibabel.load(self.filename)
        return self._data

    def _get_file(self, pattern):
        files = os.listdir(self.result_path)
        for file in files:
            if file.find(pattern) > -1:
                return os.path.join(self.result_path, file)

    @property
    def is_preprocessed(self):
        files = os.listdir(self.result_path)    
        for file in files:
            if file.find('MNI152') > -1:
                return True
        print("Files should be preprocessed via fmriprep first!")
        return False

    @property
    def confounds(self):
        import pandas as pd
        if self.is_preprocessed:
            return pd.read_csv(self._get_file('desc-confounds_timeseries.tsv'), delimiter='\t')
        else:
            return None

    @property
    def is_compressed(self):
        return os.path.splitext(self.filename)[1] == '.gz'

    @property
    def shape(self):
        return self.data.shape

    @property
    def nb_volumes(self):
        if len(self.data.shape) == 3:
            return 1
        else:
            return self.data.shape[-1]

    @property
    def transformation(self):
        if self.is_preprocessed:
            return self._get_file('from-T1w_to-MNI152NLin2009cAsym_mode-image_xfm.h5')
        else:
            return None

    @property
    def nifti_filename(self):
        if self._nifti_filename is None:
            self._nifti_filename = os.path.splitext(self.filename)[0]
            nibabel.save(self.data, self._nifti_filename)
        return self._nifti_filename

    @property
    def brain_mask(self):
        if self.is_preprocessed:
            if self._brain_mask is None:
                self._brain_mask = nibabel.load(self._get_file('brain_mask.nii.gz'))
            return self._brain_mask
        else:
            return None

    @property
    def mask_indices(self):
        if self._mask_indices is None:
            self._mask_indices = numpy.where(self.brain_mask.get_fdata() > 0)
        return self._mask_indices

    @property
    def preprocessed(self):
        if self.is_preprocessed:
            return nibabel.load(self._get_file('preproc_bold.nii.gz'))
        else:
            return None

    @property
    def cleaned(self):
        if self.is_preprocessed:
            confound_matrix = self.confounds[self.confound_columns].values
            return image.clean_img(self.preprocessed, confounds=confound_matrix, 
                detrend=True, low_pass=self._low_pass, high_pass=self._high_pass, t_r=self._t_r)
        else:
            return None

    @property
    def masked_normalized_values(self):
        return self._masker.fit_transform(self.preprocessed)

    @property
    def masked_normalized_cleaned_values(self):
        return self._masker.fit_transform(self.cleaned)

    @property
    def masked_normalized_cleaned_parceled_values(self):
        return self._parcelizer.fit_transform(self.cleaned)

    def clean_nifti(self):
        if self._nifti_filename is not None:
            if os.path.exists(self._nifti_filename):
                os.remove(self._nifti_filename)
                self._nifti_filename = None

    def export(self, output_file, parcellation=True, compression=True):
        if not parcellation:
            if not compression:
                numpy.savez(output_file, self.masked_normalized_cleaned_values)
            else:
                numpy.savez_compressed(output_file, self.masked_normalized_cleaned_values)
        else:
            if not compression:
                numpy.savez(output_file, self.masked_normalized_cleaned_values, self.masked_normalized_cleaned_parceled_values)
            else:
                numpy.savez_compressed(output_file, self.masked_normalized_cleaned_values, self.masked_normalized_cleaned_parceled_values)

    def get_selection_voxels(self, position, radius=10, mask=None):

        if mask is None:
            x, y, z = self.mask_indices
        else:
            x, y, z = mask

        coordinates = numpy.array(image.coord_transform(x, y, z, self.preprocessed.affine))
        coordinates = numpy.unique(coordinates, axis=1)
        distances = numpy.linalg.norm(numpy.array(coordinates).T - position, axis=1)
        idx = numpy.where(distances < radius)[0]

        return coordinates[:, idx], idx, distances[idx]

    def get_correlation_matrix(self, kind='correlation', time_series=None):

        if time_series is None:
            time_series = self.masked_normalized_cleaned_parceled_values

        from nilearn.connectome import ConnectivityMeasure
        correlation_measure = ConnectivityMeasure(kind=kind)
        return correlation_measure.fit_transform([time_series])[0]

    def view_connectivity(self, correlation_matrix=None, output=None, vmax=None, vmin=None):


        if correlation_matrix is None:
            correlation_matrix = self.get_correlation_matrix()

        correlation_matrix = numpy.nan_to_num(correlation_matrix)
        # Plot the correlation matrix
        import numpy as np
        numpy.fill_diagonal(correlation_matrix, 0)

        if vmax is None:
            vmax = correlation_matrix.max()

        if vmin is None:
            vmin = correlation_matrix.min()

        nilearn.plotting.plot_matrix(correlation_matrix, figure=(10, 8), labels=self.labels,
                            vmax=vmax, vmin=vmin)

        if output is not None:
            plt.savefig(output)
            plt.close()
        else:
            plt.show()

        return correlation_matrix

    def view_atlas(self, output=None):

        nilearn.plotting.plot_roi(self.atlas_filename, title="Harvard Oxford atlas")

        if output is not None:
            plt.savefig(output)
            plt.close()
        else:
            plt.show()

    def view_connectome(self, connectome=None, edge_threshold=None, output=None):

        if connectome is None:
            connectome = self.get_connectome()

        coordinates = nilearn.plotting.find_parcellation_cut_coords(labels_img=self.atlas_filename)
        nilearn.plotting.plot_connectome(connectome, coordinates, edge_threshold=edge_threshold, colorbar=True, node_color='k')

        if output is not None:
            plt.savefig(output)
            plt.close()
        else:
            plt.show()

class ANAT(MRI):

    def __init__(self, data_path, result_path):

        MRI.__init__(self, data_path, result_path)


    def set_session_path(self, path):
        self.session_path = path

    def _get_session_file(self, pattern):
        files = os.listdir(self.session_path)
        for file in files:
            if file.find(pattern) > -1:
                return os.path.join(self.session_path, file)

    @property
    def local_transformation(self):
        if self.is_preprocessed:
            return self._get_session_file('from-orig_to-T1w_mode-image_xfm.txt')
        else:
            return None

    @property
    def preprocessed(self):
        if self.is_preprocessed:
            return nibabel.load(self._get_file('preproc_T1w.nii.gz'))
        else:
            return None
