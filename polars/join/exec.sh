#!/bin/bash
set -e

# code snippet from _helpers/ used to determine right join table names.
data_name=${SRC_DATANAME}
x_n="$(echo $data_name | cut -d '_' -f 2)"
x_n_lhs="$(echo $x_n | cut -d 'e' -f 1)"
if [ "$x_n_lhs" -ne 1 ]; then
  echo "data_name $data_name must have '1' base in exponential notation for number of rows" >&2 && eit 1
fi
x_n_rhs="$(echo $x_n | cut -d "e" -f 2)"
if [ "$x_n_rhs" -lt 6 ]; then
  echo "data_name $data_name must have exponent greater or equal to '6' in exponential notation for number of rows" >&2 && exit 1
fi

RUSTFLAGS='-C target-cpu=native' cargo run +nighlty --release ${data_name/NA/"$x_n_lhs"e"$(($x_n_rhs-6))"} ${data_name/NA/"$x_n_lhs"e"$(($x_n_rhs-3))"} ${data_name/NA/"$x_n_lhs"e"$x_n_rhs"}

