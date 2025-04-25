# ----------------------------------------------------------------------------
# Copyright (c) 2016-2025, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------
import unittest
import tempfile
import os
import zipfile
import pathlib

from qiime2.core.testing.type import FourInts
from qiime2.core.testing.util import ArchiveTestingMixin
import qiime2.core.archive as archive
from qiime2.core.archive.format.v7_0 import ArchiveFormat
from qiime2.core.archive.format.util import artifact_version
from qiime2.core.annotate import Note
from qiime2.sdk import Artifact


class TestArtifactVersion(unittest.TestCase, ArchiveTestingMixin):
    def setUp(self):
        prefix = "qiime2-test-temp-"
        self.temp_dir = tempfile.TemporaryDirectory(prefix=prefix)
        self.provenance_capture = archive.ImportProvenanceCapture()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_nonexistent_archive_format(self):
        with self.assertRaisesRegex(ValueError, 'Version foo not supported'):
            with artifact_version('foo'):
                pass

    # ARCHIVE V0
    def test_write_v0_archive(self):
        fp = os.path.join(self.temp_dir.name, 'artifact_v0.qza')

        with artifact_version(0):
            artifact = Artifact._from_view(FourInts, [-1, 42, 0, 43], list,
                                           self.provenance_capture)
            artifact.save(fp)

        root_dir = str(artifact.uuid)
        # There should be no provenance
        expected = {
            'VERSION',
            'metadata.yaml',
            'data/file1.txt',
            'data/file2.txt',
            'data/nested/file3.txt',
            'data/nested/file4.txt',
        }
        self.assertArchiveMembers(fp, root_dir, expected)

        with zipfile.ZipFile(fp, mode='r') as zf:
            version = zf.read(os.path.join(root_dir, 'VERSION'))
        self.assertRegex(str(version), '^.*archive: 0.*$')

    # ARCHIVE V4
    def test_write_v4_archive(self):
        fp = os.path.join(self.temp_dir.name, 'artifact_v4.qza')

        with artifact_version(4):
            artifact = Artifact._from_view(FourInts, [-1, 42, 0, 43], list,
                                           self.provenance_capture)
            artifact.save(fp)

        root_dir = str(artifact.uuid)
        expected = {
            'VERSION',
            'metadata.yaml',
            'data/file1.txt',
            'data/file2.txt',
            'data/nested/file3.txt',
            'data/nested/file4.txt',
            'provenance/metadata.yaml',
            'provenance/VERSION',
            'provenance/citations.bib',
            'provenance/action/action.yaml',
        }
        self.assertArchiveMembers(fp, root_dir, expected)

        with zipfile.ZipFile(fp, mode='r') as zf:
            version = zf.read(os.path.join(root_dir, 'VERSION'))
        self.assertRegex(str(version), '^.*archive: 4.*$')

    # ARCHIVE V7.0
    def test_write_v7_0_archive_with_note_annotation(self):
        fp = os.path.join(self.temp_dir.name, 'artifact_v7_0.qza')

        with artifact_version(7.0):
            artifact = Artifact._from_view(FourInts, [-1, 42, 0, 43], list,
                                           self.provenance_capture)
            note = Note(name='mynote', text='my special text')
            artifact.add_annotation(note)
            artifact.save(fp)

        root_dir = str(artifact.uuid)
        expected = {
            'VERSION',
            'metadata.yaml',
            'checksums.sha512',
            'data/file1.txt',
            'data/file2.txt',
            'data/nested/file3.txt',
            'data/nested/file4.txt',
            'provenance/metadata.yaml',
            'provenance/VERSION',
            'provenance/citations.bib',
            'provenance/conda-env.yaml',
            'provenance/action/action.yaml',
            f'annotations/{note.id}/metadata.yaml',
            f'annotations/{note.id}/note.txt',
            f'annotations/{note.id}/checksums.sha512'
        }
        self.assertArchiveMembers(fp, root_dir, expected)

        with zipfile.ZipFile(fp, mode='r') as zf:
            version = zf.read(os.path.join(root_dir, 'VERSION'))
            root_metadata = zf.read(os.path.join(root_dir, 'metadata.yaml'))
            prov_metadata = \
                zf.read(os.path.join(root_dir, 'provenance', 'metadata.yaml'))
            conda_env = \
                zf.read(os.path.join(root_dir, 'provenance', 'conda-env.yaml'))
            annotation_metadata = \
                zf.read(os.path.join(root_dir, 'annotations', f'{note.id}',
                                     'metadata.yaml')).decode('utf-8')
            note_contents = \
                zf.read(os.path.join(root_dir, 'annotations',
                                     f'{note.id}', 'note.txt'))
            checksums = \
                zf.read(os.path.join(root_dir, 'annotations',
                                     f'{note.id}', 'checksums.sha512'))

        # check that version is what we expect
        self.assertRegex(str(version), '^.*archive: 7.0.*$')
        # check that root md contains data-size attr
        # and that root & prov md are identical
        self.assertRegex(str(root_metadata), '^.*data-size: .*B.*$')
        self.assertEqual(str(root_metadata), str(prov_metadata))
        # check that conda env exists & isn't empty
        self.assertRegex(str(conda_env), '^.*dependencies:.*- .*$')
        # check that annotation md contains what we expect
        self.assertRegex(str(annotation_metadata),
                         rf'id: {note.id}\nname: mynote\ntype: Note')
        # check that the contents of the note is what we expect
        self.assertRegex(str(note_contents), 'my special text')
        # check that the checksums file contains the two files we expect
        self.assertRegex(str(checksums), '^.*metadata.yaml.*note.txt.*$')

    # testing data directory size helpers
    # total file size calculation within a provided directory
    def test_calculate_directory_size_util(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = pathlib.Path(tmpdir)

            f1 = tmpdir / 'a.txt'
            f2 = tmpdir / 'subdir' / 'b.txt'
            f2.parent.mkdir()

            f1.write_text('abc')        # 3 bytes
            f2.write_text('12345')      # 5 bytes

            result = ArchiveFormat._calculate_directory_size(tmpdir)
            self.assertEqual(result, 8)

    # conversion from bytes to human readable output (MiB, KiB, etc)
    def test_human_readable_size_util(self):
        cases = [
            (0, "0.0 B"),
            (500, "500.0 B"),
            (1023, "1023.0 B"),
            (1024, "1.0 KiB"),
            (1536, "1.5 KiB"),
            (1024**2, "1.0 MiB"),
        ]

        for num, expected in cases:
            result = ArchiveFormat._human_readable_size(num)
            self.assertEqual(result, expected)

    def test_annotation_actions_on_older_archive_version(self):
        note = Note(name='mynote', text='my special text')

        with artifact_version(4):
            artifact = Artifact._from_view(FourInts, [-1, 42, 0, 43], list,
                                           self.provenance_capture)

        # add_annotation
        with self.assertRaisesRegex(
            ValueError, 'Artifact or Visualization being used is'
                        ' associated with a QIIME 2 archive format of < 7.0.'
        ):
            artifact.add_annotation(note)

        # remove_annotation
        with self.assertRaisesRegex(
            ValueError, 'Artifact or Visualization being used is'
                        ' associated with a QIIME 2 archive format of < 7.0.'
        ):
            artifact.remove_annotation('foo')

        # get_annotation
        with self.assertRaisesRegex(
            ValueError, 'Artifact or Visualization being used is'
                        ' associated with a QIIME 2 archive format of < 7.0.'
        ):
            artifact.get_annotation('foo')

        # iter_annotations
        with self.assertRaisesRegex(
            ValueError, 'Artifact or Visualization being used is'
                        ' associated with a QIIME 2 archive format of < 7.0.'
        ):
            for annotation in artifact.iter_annotations():
                print(annotation.name)
