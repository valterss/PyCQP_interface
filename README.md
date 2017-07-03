# My PyCQP_interface

Due to problems installing the package [cwb-python](https://pypi.python.org/pypi/cwb-python/) in Mac OS and some usage problems thereafter, I decided to take the `PyCQP_interface.py` and modify it for my own needs.

This package works with Python >= 3.

Requirements:

- `six`

## Install

I recommend to install this package in a virtual environment.

With `pip` directly from GitHub:

```shell
pip install -e git+https://github.com/chozelinek/PyCQP_interface.git#egg=pycqp-interface
```

## Usage

```python
import PyCQP_interface
# name of the corpus to Query
corpus_name = "EP-EN-TT"
# path to your registry
registry_dir = "~/CORPORA/registry"
# start a CQP session
cqp=PyCQP_interface.CQP(bin='cqp', options='-c -r ' + registry_dir)
# select the corpus
cqp.Exec(corpus_name)
# get all tokens in corpus
cqp.Query("[]")
# get size in tokens
cqp.Exec("size Last")
# end session
cqp.Terminate()
```
