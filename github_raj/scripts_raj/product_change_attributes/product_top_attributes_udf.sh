#!/bin/bash
set -e
(>&2 echo "Begin of script")
python product_top_attributes_udf.py $1
(>&2 echo "End of script")