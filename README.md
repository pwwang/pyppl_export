# pyppl_export

Export outputs generated by PyPPL pipelines:

1. export whole output to a directory
2. export partial output to a directory
3. resume process from export directory

```python
# export all output
PyPPL(config_export_dir = './export')

# export output with key outfile1
# pXXX.output = 'outfile1:file:..., outfile2:file:...'
PyPPL(config_export_dir = './export', config_export_part = 'outfile1')

# export output with .txt files only
PyPPL(config_export_dir = './export', config_export_part = '*.txt')

# use export directory to make cache job cached
PyPPL(config_export_dir = './export', cache = 'export')
```
