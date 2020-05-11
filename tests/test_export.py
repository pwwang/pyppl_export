import pytest
from diot import Diot, OrderedDiot
from pyppl import Proc
from pyppl.job import Job
from pyppl.utils import fs
from pyppl.logger import logger
from pyppl_export import job_done, logger_init, setup as pe_setup, proc_prerun, job_prebuild
from pyppl.config import config

def setup_module(module):
    pe_setup(config)
    logger_init(logger)

@pytest.fixture
def job0(tmp_path, request):
    job = Job(0, Proc(
        id='pJob0_%s' % request.node.nodeid,
        workdir=tmp_path/'pJob',
        dirsig=True,
        config=Diot(echo_jobs=0, types='stderr')
    ))

    # pretend it's running
    job.proc.runtime_config = {'dirsig': True}

    fs.mkdir(job.dir)
    (job.dir / 'job.script').write_text('')
    return job

def test_export(job0, tmp_path, caplog):
    job0.proc.config.export_dir = ''
    job_done(job0, 'succeeded')
    assert 'Exported' not in caplog.text

    job0.proc.config.export_dir = tmp_path / 'test_export'
    proc_prerun(job0.proc)

    job0.proc.config.export_part = []
    job_done(job0, 'succeeded')
    assert 'Exported' not in caplog.text

    # export everything
    outfile1 = job0.dir / 'output' / 'test_export_outfile.txt'
    outfile1.parent.mkdir(exist_ok = True)
    outfile1.write_text('')
    job0.__attrs_property_cached__['output'] = OrderedDiot(
        outfile = ('file', outfile1)
    )
    job0.proc.config.export_how = 'copy'
    job0.proc.config.export_ow = True
    job_done(job0, 'succeeded')
    assert fs.exists(job0.proc.config.export_dir / outfile1.name)
    assert not fs.islink(outfile1)
    assert not fs.samefile(outfile1, job0.proc.config.export_dir / outfile1.name)
    assert ('Exported: %s' % (job0.proc.config.export_dir / outfile1.name)) in caplog.text

    job0.proc.config.export_how = 'move'
    job_done(job0, 'succeeded')
    assert fs.exists(job0.proc.config.export_dir / outfile1.name)
    assert fs.islink(outfile1)
    assert fs.samefile(outfile1, job0.proc.config.export_dir / outfile1.name)
    assert ('Exported: %s' % (job0.proc.config.export_dir / outfile1.name)) in caplog.text

    # outfile is a link, then copy the file
    job_done(job0, 'succeeded')
    assert fs.exists(job0.proc.config.export_dir / outfile1.name)
    assert not fs.islink(job0.proc.config.export_dir / outfile1.name)
    assert fs.islink(outfile1)
    assert fs.samefile(outfile1, job0.proc.config.export_dir / outfile1.name)

    job0.proc.config.export_how = 'link'
    job_done(job0, 'succeeded')
    assert fs.exists(job0.proc.config.export_dir / outfile1.name)
    assert fs.islink(job0.proc.config.export_dir / outfile1.name)
    assert not fs.islink(outfile1)
    assert fs.samefile(outfile1, job0.proc.config.export_dir / outfile1.name)

    job0.proc.config.export_how = 'gzip'
    job_done(job0, 'succeeded')
    assert fs.exists(job0.proc.config.export_dir / (outfile1.name + '.gz'))

    job0.proc.config.export_part = ['outfile']
    fs.remove(job0.proc.config.export_dir / (outfile1.name + '.gz'))
    job_done(job0, 'succeeded')
    assert fs.exists(job0.proc.config.export_dir / (outfile1.name + '.gz'))

    job0.proc.config.export_part = ['*.txt']
    fs.remove(job0.proc.config.export_dir / (outfile1.name + '.gz'))
    job_done(job0, 'succeeded')
    assert fs.exists(job0.proc.config.export_dir / (outfile1.name + '.gz'))


def test_prebuild(job0, tmp_path, caplog):
    job0.proc.config.export_dir = False
    assert not job0.is_cached()

    job0.proc.cache = 'export'
    job0.proc.config.export_dir = 'export'
    job0.proc.config.export_how = 'link'
    job_prebuild(job0)
    assert not job0.is_cached()
    assert 'Job is not export-cached using symlink export.' in caplog.text
    caplog.clear()

    job0.proc.config.export_how = 'copy'
    job0.proc.config.export_part = [('outfile')]
    job_prebuild(job0)
    assert not job0.is_cached()
    assert 'Job is not export-cached using partial export.' in caplog.text
    caplog.clear()

    job0.proc.config.export_part = None
    job0.proc.config.export_dir = ''
    job_prebuild(job0)
    assert not job0.is_cached()
    caplog.clear()

    job0.proc.config.export_dir = tmp_path / 'test_is_cached_exdir'
    job0.proc.config.export_dir.mkdir()
    outfile1 = tmp_path / 'test_is_cached_outfile1.txt'
    outfile1.write_text('')
    outfile2 = tmp_path / 'test_is_cached_outfile_not_exists.txt'
    outdir1 = tmp_path / 'test_is_cached_outdir1'
    outdir1.mkdir()
    fs.gzip(outfile1, job0.proc.config.export_dir / (outfile1.name + '.gz'))
    fs.gzip(outdir1, job0.proc.config.export_dir / (outdir1.name + '.tgz'))
    job0.__attrs_property_cached__['output'] = OrderedDiot(
        outfile = ('file', outfile1),
        outdir = ('dir', outdir1),
        out = ('var', 'abc')
    )
    # overwriting existing
    (job0.dir / 'output').mkdir()
    (job0.dir / 'output' / outfile1.name).write_text('')
    job0.proc.config.export_how = 'gzip'
    job_prebuild(job0)
    assert job0.is_cached()
    assert 'Overwrite file for export-caching:' in caplog.text
    assert job0.is_cached()
    caplog.clear()

    fs.remove(job0.proc.config.export_dir / (outfile1.name + '.gz'))
    job_prebuild(job0)
    assert 'Job is not export-cached since exported file not exists:' in caplog.text
    caplog.clear()

    job0.__attrs_property_cached__['output'] = OrderedDiot(
        outfile = ('file', outfile1)
    )
    job0.proc.config.export_how = 'move'
    job_prebuild(job0)
    assert 'Job is not export-cached since exported file not exists:' in caplog.text

    fs.link(outfile1, job0.proc.config.export_dir / outfile1.name)
    job_prebuild(job0)
    assert job0.is_cached()
    caplog.clear()

    # overwriting existing
    fs.remove(job0.proc.config.export_dir / outfile1.name)
    (job0.proc.config.export_dir / outfile1.name).write_text('')
    job_prebuild(job0)
    assert job0.is_cached()
    assert 'Overwrite file for export-caching: ' in caplog.text
