# PROTON

[![Build Status](https://travis-ci.com/obsmax/proton.svg?branch=master)](https://travis-ci.com/obsmax/proton)

Parallelization helper for python based on python-multiprocessing

* Threads input/output exchanged using python generators  
* provide tools to handle the exceptions raised by the threads
* Allow controling the affinity/priority of the threads (linux systems)
* Provides on time execution statistics
* Provides thread safe tools for random numbers applications
* ...

## Install
```bash
# cd <to/installation/path>
git clone https://github.com/obsmax/proton
conda create -n neutron python=3.7 --yes
conda activate neutron
cd proton
pip install -e .
```

## Usage examples

* examples/example_000.py  
The simplest possible usage: run 10 jobs in parallel  
  
* examples/example_001.py  
play with the number of virtual threads, affinity or priority  
  
* examples/example_002.py  
handle fatal / non-fatal exceptions  

* examples/example_003.py  
pass the worker to the target function  

* examples/example_004.py  
Use a callable class for target   

* examples/example_005.py  
Use locks

* examples/example_006.py  
Parallelize a cumulative processing
