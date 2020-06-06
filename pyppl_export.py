"""Export outputs generated by PyPPL pipelines
Features:
1. export whole output to a directory
2. export partial output to a directory
3. resume process from export directory
"""
from functools import partial
from pathlib import Path
from pyppl.plugin import hookimpl
from pyppl.config import config
from pyppl.utils import always_list, fs
from pyppl._proc import OUT_VARTYPE, OUT_DIRTYPE

__version__ = "0.0.5"

EX_GZIP = ('gzip', 'gz')
EX_COPY = ('copy', 'cp')
EX_MOVE = ('move', 'mv')
EX_LINK = ('link', 'symlink', 'symbol')

config.config.export_dir = None
config.config.export_how = EX_MOVE[0]
config.config.export_part = ''
config.config.export_ow = True

@hookimpl
def logger_init(logger):
    """Add log levels"""
    logger.add_level('EXPORT')
    logger.add_sublevel('CACHED_FROM_EXPORT', -1)
    logger.add_sublevel('EXPORT_CACHE_USING_SYMLINK', -1)
    logger.add_sublevel('EXPORT_CACHE_USING_EXPARTIAL', -1)
    logger.add_sublevel('EXPORT_CACHE_EXFILE_NOTEXISTS', -1)
    logger.add_sublevel('EXPORT_CACHE_OUTFILE_EXISTS', -1)


def export_part_converter(part, proc):
    """Export_part converter"""
    parts = [] if not part else always_list(part)
    for i, prt in enumerate(parts):
        parts[i] = proc.template(prt, **proc.envs)
    return parts


@hookimpl
def proc_init(proc):
    """Add configs"""
    proc.add_config('export_dir',
                    converter=lambda exdir: None if not exdir else Path(exdir))
    proc.add_config('export_how')
    proc.add_config('export_part',
                    converter=partial(export_part_converter, proc=proc))
    proc.add_config('export_ow', default=True)


@hookimpl
def proc_prerun(proc):
    """Create export directory"""
    if proc.config.export_dir:
        proc.config.export_dir.mkdir(exist_ok=True, parents=True)


@hookimpl
def job_done(job, status):  # pylint: disable=too-many-branches
    """Export the output if job succeeded"""
    if status == 'failed' or not job.proc.config.export_dir:
        return

    # output files to export
    files2ex = []
    # no partial export
    if (not job.proc.config.export_part
            or (len(job.proc.config.export_part) == 1
                and not job.proc.config.export_part[0].render(job.data))):

        files2ex.extend(
            Path(outdata)
            for outtype, outdata in job.output.values()
            if outtype not in OUT_VARTYPE
        )
    else:
        for expart in job.proc.config.export_part:
            expart = expart.render(job.data)
            if expart in job.output:
                files2ex.append(Path(job.output[expart][1]))
            else:
                files2ex.extend(job.dir.joinpath('output').glob(expart))

    files2ex = set(files2ex)
    for file2ex in files2ex:
        # don't export if file2ex does not exist
        # it might be a dead link
        # then job should fail
        if not file2ex.exists():
            return
        # exported file
        exfile = job.proc.config.export_dir.joinpath(file2ex.name)
        if job.proc.config.export_how in EX_GZIP:
            exfile = exfile.with_suffix(exfile.suffix + '.tgz') \
                if fs.isdir(file2ex) \
                else exfile.with_suffix(exfile.suffix + '.gz')
        # If job is cached and exported file exists, skip exporting
        if status == 'cached' and exfile.exists():
            continue

        with fs.lock(file2ex, exfile):
            if job.proc.config.export_how in EX_GZIP:
                fs.gzip(file2ex, exfile, overwrite=job.proc.config.export_ow)
            elif job.proc.config.export_how in EX_COPY:
                fs.copy(file2ex, exfile, overwrite=job.proc.config.export_ow)
            elif job.proc.config.export_how in EX_LINK:
                fs.link(file2ex, exfile, overwrite=job.proc.config.export_ow)
            else:  # move
                if fs.islink(file2ex):
                    fs.copy(file2ex,
                            exfile,
                            overwrite=job.proc.config.export_ow)
                else:
                    fs.move(file2ex,
                            exfile,
                            overwrite=job.proc.config.export_ow)
                    fs.link(exfile.resolve(), file2ex)

        job.logger('Exported: %s' % exfile, level='EXPORT', plugin='export')


@hookimpl
def job_prebuild(job):
    """See if we can extract output from export directory"""
    if job.proc.cache != 'export' or not job.proc.config.export_dir:
        return

    if job.proc.config.export_how in EX_LINK:
        job.logger("Job is not export-cached using symlink export.",
                   slevel="EXPORT_CACHE_USING_SYMLINK",
                   level="warning",
                   plugin="export")
        return
    if job.proc.config.export_part and \
        job.proc.config.export_part[0].render(job.data):

        job.logger("Job is not export-cached using partial export.",
                   slevel="EXPORT_CACHE_USING_EXPARTIAL",
                   level="warning",
                   plugin="export")
        return

    for outtype, outdata in job.output.values():
        if outtype in OUT_VARTYPE:
            continue
        exfile = job.proc.config.export_dir / outdata.name

        if job.proc.config.export_how in EX_GZIP:
            exfile = (exfile.with_suffix(exfile.suffix + '.tgz')
                      if fs.isdir(outdata) or outtype in OUT_DIRTYPE
                      else exfile.with_suffix(exfile.suffix + '.gz'))
            with fs.lock(exfile, outdata):
                if not fs.exists(exfile):
                    job.logger(
                        "Job is not export-cached since exported "
                        "file not exists: %s" % exfile,
                        slevel="EXPORT_CACHE_EXFILE_NOTEXISTS",
                        level="debug",
                        plugin="export"
                    )
                    return

                if fs.exists(outdata):
                    job.logger('Overwrite file for export-caching: %s' %
                               outdata,
                               slevel="EXPORT_CACHE_OUTFILE_EXISTS",
                               level="warning",
                               plugin="export")
                fs.gunzip(exfile, outdata)
        else:  # exhow not gzip
            with fs.lock(exfile, outdata):
                if not fs.exists(exfile):
                    job.logger(
                        "Job is not export-cached since "
                        "exported file not exists: %s" % exfile,
                        slevel="EXPORT_CACHE_EXFILE_NOTEXISTS",
                        level="debug",
                        plugin="export"
                    )
                    return
                if fs.samefile(exfile, outdata):
                    continue
                if fs.exists(outdata):
                    job.logger("Overwrite file for "
                               "export-caching: %s" % outdata,
                               slevel="EXPORT_CACHE_OUTFILE_EXISTS",
                               level="warning",
                               plugin="export")
                fs.link(exfile.resolve(), outdata)
    job.rc = 0
    job.cache()
