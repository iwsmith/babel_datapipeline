# babel_datapipeline
Data processing pipeline for Babel

Add the following line to .bashrc or .bash_profile:
`export PYTHONPATH="${PYTHONPATH}:[[path to repo]]/babel_datapipeline”`

Current command to run:
`luigi --module luigi_pipeline [parser task] --local-scheduler --date [yyyy-mm-dd]`