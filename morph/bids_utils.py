import csv
import os
import numpy
import shutil
import nibabel
import time
import json

GLOBAL_PATH = os.path.abspath('.')
DATA_PATH = os.path.join(GLOBAL_PATH, "data/raw")
BIDS_PATH = os.path.join(GLOBAL_PATH, "data/bids")


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
    

class DataBaseReader(object):

    def __init__(self, database, converters):

        self.database = database
        self.converters = converters

        csvfile = open(os.path.abspath(self.database), newline='')
        reader = csv.DictReader(csvfile)
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
        print(participants_rows)
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
                    shutil.move(os.path.join(bids_folder, 'pre'), os.path.join(bids_folder, 'ses-pre'))
                    shutil.move(os.path.join(bids_folder, 'post'), os.path.join(bids_folder, 'ses-post'))

                    subject.set_new_key(sub_key)
                    
                    filename = os.path.join(bids_folder, '%s_sessions.tsv' %sub_key)
                    fd = open(filename, 'w')
                    
                    csv_writer = csv.DictWriter(fd, ['session', 'label'], dialect=self.dialect)
                    
                    rows = [{'session' : 'ses-pre', 'label' : 'pre'},
                            {'session' : 'ses-post', 'label' : 'post'}]

                    csv_writer.writeheader()
                    csv_writer.writerows(rows)
                    fd.close()

                    for session in ['ses-pre', 'ses-post']:
                        session_path = os.path.join(bids_folder, session)
                        filename = os.path.join(session_path, '%s_%s_scans.tsv' %(sub_key, session))
                        fd = open(filename, 'w')
                    
                        csv_writer = csv.DictWriter(fd, ['filename'], dialect=self.dialect)

                        folders = os.listdir(session_path)
                        rows = []

                        for folder in folders:
                            path = os.path.join(session_path, folder)
                            if os.path.isdir(path):

                                pattern = '%s_%s' %(sub_key, session)

                                type_converter = self.converters[folder][2]
                                task_name = self.converters[folder][1]
                                target_folder = self.converters[folder][0]

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