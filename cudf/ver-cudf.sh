#!/bin/bash
set -e

conda activate cudf
python -c 'import cudf; open("cudf/VERSION","w").write(cudf.__version__.split("+", 1)[0]); open("cudf/REVISION","w").write("");' > /dev/null
conda deactivate
