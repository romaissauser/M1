import csv
import os
import numpy
import shutil
import nibabel
import time
import json
import mri

GLOBAL_PATH = os.path.abspath('.')
DATA_PATH = os.path.join(GLOBAL_PATH, "raw_data")
BIDS_PATH = os.path.join(GLOBAL_PATH, "bids")


def gvalue(csv_row, keys):

    if not isinstance(keys, list):
        keys = [keys]

    results = []

    for key in keys:
        if key in mapping.keys():
            converter = mapping[key][0]
            default_value = mapping[key][1]
        else:
            converter = float
            default_value = None

        try:
            if converter is not float:
                data = converter(csv_row[key])
            else:
                data = converter(csv_row[key].replace(',','.'))
        except Exception:
            data = default_value
            #print("Something wrong with key", key)

        results += [data]

    if len(results) == 1:
        return results[0]
    else:
        return results


class DataConverter(object):

    def __init__(self, input_folder, target_folder=None, extension=None):
        self.input_folder = input_folder
        self.all_files = [os.path.join(self.input_folder, i) for i in os.listdir(self.input_folder)]

        if extension is not None:
            self._filter_files(extension)

        self.target_folder = None
        self.has_files = len(self.all_files) > 0

        if self.has_files:
            self.current_dir = os.path.dirname(self.all_files[0])

            for file in self.all_files:
                assert os.path.exists(file)

            self.target_folder = target_folder
            if self.target_folder is None:
                self.output_folder = self.current_dir
            else:
                self.base_folder = os.path.dirname(self.current_dir)
                self.output_folder = os.path.join(self.base_folder, self.target_folder)
                if not os.path.exists(self.output_folder):
                    os.makedirs(self.output_folder)

        self._new_files = []
        self._old_files = []

    def _filter_files(self, extension):
        self.all_files = [i for i in self.all_files if os.path.splitext(i)[1].lower() == extension]

    def convert(self, pattern, extension):
        pass

    def move(self):
        if self.has_files > 0:
            all_files = os.listdir(self.current_dir)
            if self.output_folder is not None:
                for file in all_files:
                    print(file, self.output_folder)
                    shutil.move(os.path.join(self.current_dir, file), self.output_folder)

    def clean(self):
        self._clean()
        if self.target_folder is not None:
            if len(os.listdir(self.current_dir)) == 0:
                shutil.rmtree(self.current_dir)
        elif not self.has_files:
            shutil.rmtree(self.input_folder)

    @property
    def new_files(self):
        return self._new_files

    @property
    def old_files(self):
        return self._old_files



class Subject(object):

    def __init__(self, csv_row):
        self.key = csv_row['Name']
        self.data = csv_row
        self.data_path = os.path.join(DATA_PATH, self.key)

    @property
    def export_keys(self):
        return self.get_dict().keys()    

    def set_new_key(self, key):
        self.key = key

    def get_dict(self):
        return {'key' : self.key}

class RECtoNIFTIConverter(DataConverter):

    def __init__(self, input_folder, target_folder):
        DataConverter.__init__(self, input_folder, target_folder, '.rec')

    def convert(self, pattern, extension):
        for file in self.all_files:
            name = os.path.basename(file)
            nifti_file = os.path.join(pattern + '_' + extension)
            os.system('dcm2niix -b y -z y -i n -f "%s" -o "%s" "%s"' %(nifti_file, self.output_folder, file))
            self._new_files += [os.path.join(self.output_folder, nifti_file + '.nii.gz')]
            self._old_files += [file]

    def _clean(self):
        for rec_file in self.old_files:
            os.remove(rec_file)
            file_no_extension = os.path.splitext(rec_file)[0]
            for ext in ['.par', '.PAR']:
                par_file = file_no_extension + ext
                if os.path.exists(par_file):
                    os.remove(par_file)

class DICOMtoNIFTIConverter(DataConverter):

    def __init__(self, input_folder, target_folder):
        DataConverter.__init__(self, input_folder, target_folder)

    def convert(self, pattern, extension):
        nifti_file = os.path.join(pattern + '_' + extension)
        os.system('dcm2niix -b y -z y -i n -f "%s" -o "%s" "%s"' %(nifti_file, self.output_folder, self.input_folder))
        self._new_files += [os.path.join(self.output_folder, nifti_file + '.nii.gz')]
        #self._old_files += [file]

    def _clean(self):
        for file in os.listdir(self.input_folder):
            file_no_extension, ext = os.path.splitext(file)
            if ext == '.dcm':
                os.remove(os.path.join(self.input_folder, file))
    

class DataBaseReader(object):

    def __init__(self, database, converters):

        self.database = database
        self.converters = converters

        csvfile = open(os.path.abspath(self.database), newline='')
        reader = csv.DictReader(csvfile, delimiter='\t')
        self.dialect = csv.excel
        self.dialect.delimiter = '\t'
        self.subjects = {}

        for i in reader:
            key = Subject(i).key
            self.subjects[key] = [Subject(i)]

    def __getitem__(self, i):
        return list(self.subjects.values())[i][0]

    def __len__(self):
        return len(self.subjects)        

    def write_bids_description(self, name):

        filename = os.path.join(BIDS_PATH, 'dataset_description.json')
        import bids
        f = open(filename, 'w')
        f.write(
'''
{
    "BIDSVersion": "%s",
    "Name": "%s"
}
''' %(bids.__version__, name))
        f.close()

    def write_bids_ignore(self):

        filename = os.path.join(BIDS_PATH, '.bidsignore')
        f = open(filename, 'w')
        f.write(
'''
extra_data/
*_ADC.nii.gz
''')
        f.close()

    def write_bids_participants(self):
        participants_rows = []
        for count in range(len(self)):
            subject = self[count]
            sub_key = 'sub-%02d' %(count + 1)
            subject.set_new_key(sub_key)
            participants_rows += [subject.get_dict()]
        filename = os.path.join(BIDS_PATH, 'participants.tsv')
        fd = open(filename, 'w')
        csv_writer = csv.DictWriter(fd, subject.export_keys, dialect=self.dialect)
        csv_writer.writeheader()
        csv_writer.writerows(participants_rows)

    def convert_to_bids(self, name):
        if not os.path.exists(BIDS_PATH):
            os.makedirs(BIDS_PATH)

        self.write_bids_description(name)
        self.write_bids_ignore()

        for count in range(len(self)):

            subject = self[count]

            if os.path.exists(subject.data_path):
                
                sub_key = 'sub-%02d' %(count + 1)
                bids_folder = os.path.join(BIDS_PATH, sub_key)

                if not os.path.exists(bids_folder):
                    shutil.copytree(subject.data_path, bids_folder)
                    
                    #shutil.rmtree(os.path.join(bids_folder, 'run 5'))
                    #print('rm %s/*.xlsx' %bids_folder)
                    os.system('rm %s/*.xlsx' %bids_folder)
                    subject.set_new_key(sub_key)
                    
                    filename = os.path.join(bids_folder, '%s_scans.tsv' %(sub_key))
                    fd = open(filename, 'w')
                    
                    csv_writer = csv.DictWriter(fd, ['filename'], dialect=self.dialect)

                    folders = os.listdir(bids_folder)
                    rows = []

                    for run_id in range(1, 6):
                        path = os.path.join(bids_folder, f'run {run_id}')
                        if os.path.isdir(path):

                            pattern = '%s' %(sub_key)

                            files = os.listdir(path)

                            if len(files) == 2:
                                type_converter = self.converters['fmri'][2]
                                task_name = f'task-morph_run-{run_id}_bold'
                                target_folder = self.converters['fmri'][0]
                                print(path, target_folder)
                                converter = type_converter(path, target_folder)
                                converter.convert(pattern, task_name)
                                converter.clean()
                            else:
                                type_converter = DICOMtoNIFTIConverter
                                task_name = f'task-morph_run-{run_id}_bold'
                                target_folder = self.converters['fmri'][0]
                                print(path, target_folder)
                                converter = type_converter(path, target_folder)
                                converter.convert(pattern, task_name)
                                converter.clean()

                            all_files = converter.new_files

                            for file in all_files:
                                relative_path = os.path.join(os.path.basename(os.path.dirname(file)), os.path.basename(file))
                                rows += [{'filename' : relative_path}]

                    for folder in ['anat']:
                        path = os.path.join(bids_folder, folder)
                        if os.path.isdir(path):

                            pattern = '%s' %(sub_key)

                            files = os.listdir(path)

                            if len(files) == 2:
                                type_converter = self.converters[folder][2]
                                task_name = self.converters[folder][1]
                                target_folder = self.converters[folder][0]
                                print(path, target_folder)
                                converter = type_converter(path, target_folder)
                                converter.convert(pattern, task_name)
                                converter.clean()
                            else:
                                type_converter = DICOMtoNIFTIConverter
                                task_name = self.converters[folder][1]
                                target_folder = self.converters[folder][0]
                                print(path, target_folder)
                                converter = type_converter(path, target_folder)
                                converter.convert(pattern, task_name)
                                converter.clean()

                            all_files = converter.new_files

                            for file in all_files:
                                relative_path = os.path.join(os.path.basename(os.path.dirname(file)), os.path.basename(file))
                                rows += [{'filename' : relative_path}]

                    csv_writer.writeheader()
                    csv_writer.writerows(rows)
                    fd.close()

        self.write_bids_participants()



class BIDSDatabase(object):

    def __init__(self, bids_path, result_path, index=False):
        import bids
        self.bids_path = bids_path
        self.sql_data = os.path.join(os.path.dirname(self.bids_path), 'database.bids')
        if not os.path.exists(self.sql_data):
            self.bids = bids.BIDSLayout(self.bids_path, validate=False, database_path=self.sql_data)
        else:
            if index:
                self.bids = bids.BIDSLayout(self.bids_path, validate=False, database_path=self.sql_data, reset_database=True)
            else:
                self.bids = bids.BIDSLayout(self.bids_path, validate=False, database_path=self.sql_data)
        self.result_path = result_path
        self.participants = self.bids.get_file('participants.tsv').get_df()

    def __len__(self):
        return self.nb_subjects

    @property
    def nb_subjects(self):
        return len(self.participants)

    def _get_result_path(self, subject, data_type):
        assert data_type in ['func', 'anat', 'extra_data']
        subject = self._get_subject_key(subject, 'sub-')
        path = os.path.join(self.result_path, subject, data_type)
        
        if not os.path.exists(path):
            os.makedirs(path)
        return path

    def slice_subjects(self, filters=None, as_string=False):
        res = self.participants
        if filters is not None:
            cond = None
            for key, value in filters.items():
                if cond is None:
                    cond = getattr(self.participants, key) == value
                else:
                    cond *= getattr(self.participants, key) == value
            res = res[cond]

        if as_string:
            data = list(bids_data.participants.key[cond].values)
            result = [i.replace('sub-', '') for i in data]
        else:
            result = res.index
        return result

    def _get_subject_key(self, subject, prefix=None):
        if type(subject) == int:
            subject = '%02d' %subject

        if prefix is not None:
            subject = prefix + subject
        return subject

    def _get_anat_path(self, subject):
        subject = self._get_subject_key(subject, 'sub-')
        path = os.path.join(self.result_path, subject, 'anat')
        
        if not os.path.exists(path):
            os.makedirs(path)
        return path

    def _get_all_partial(self, data, subjects=None, verbose=False, filters=None):
        result = {}
        
        if subjects is None:
            subjects = range(1, self.nb_subjects+1)

        if filters is not None:
            subjects = self.slice_subjects(filters)

        for subject in subjects:
            subject = self._get_subject_key(subject)
            if data == 'func':
                result[subject] = self.get_func(subject, verbose)
            elif data == 'anat':
                result[subject] = self.get_anat(subject, verbose)
        return result

    def _get_filenames(self, data, subject):
        subject = self._get_subject_key(subject)
        filenames = self.bids.get(return_type='filename', subject=subject, datatype=data, extension='nii.gz')
        return filenames


    def get_func(self, subject, verbose=False):
        if verbose:
            print('Loading bold sequence for subject', subject)

        result_path = self._get_result_path(subject, 'func')
        
        files = self._get_filenames('func', subject)
        res = []
        for file in files:
            try:
                res += [mri.MRI(file, result_path)]
            except Exception:
                res += [None]
        return res

    def get_all_func(self, subjects=None, verbose=False, filters=None):
        return self._get_all_partial('func', subjects, verbose, filters)


    def launch_fmriprep(self, use_aroma=False, nprocs=72, subset=None, work_folder=None):
        if work_folder is None:
            work_folder = os.path.join(os.path.dirname(self.bids_path), 'work')

        command = 'fmriprep-docker %s %s participant -w %s --nthreads %d --verbose --notrack' %(self.bids_path, self.result_path, work_folder, nprocs)

        print(command)
        all_funcs = self.bids.get(datatype='func')
        all_subjects = []
        for func in all_funcs:
            if func.subject not in all_subjects:
                all_subjects += [func.subject]

        if subset == 'odd':
            command += ' --participant-label '
            command += ' '.join(all_subjects[::2])
        elif subset == 'not-odd':
            command += ' --participant-label '
            command += ' '.join(all_subjects[1::2])
        elif subset == 'all':
            command += ' --participant-label '
            command += ' '.join(all_subjects)

        if use_aroma:
            command += ' --use-aroma'
        os.system(command)

    def launch_xcp_d(self, nprocs=72, subset=None, work_folder=None):
        if work_folder is None:
            work_folder = os.path.join(os.path.dirname(self.bids_path), 'work')

        command = 'xcp_d %s %s participant -w %s --nthreads %d --verbose --despike' %(self.bids_path, self.result_path, work_folder, nprocs)

        print(command)
        all_funcs = self.bids.get(datatype='func')
        all_subjects = []
        for func in all_funcs:
            if func.subject not in all_subjects:
                all_subjects += [func.subject]

        if subset == 'odd':
            command += ' --participant-label '
            command += ' '.join(all_subjects[::2])
        elif subset == 'not-odd':
            command += ' --participant-label '
            command += ' '.join(all_subjects[1::2])
        elif subset == 'all':
            command += ' --participant-label '
            command += ' '.join(all_subjects)

        os.system(command)
