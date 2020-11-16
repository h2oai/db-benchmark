# join task RHS tables for LHS data name
join_to_tbls() {
  data_name=$1
  x_n="$(echo $data_name | cut -d '_' -f 2)"
  x_n_lhs="$(echo $x_n | cut -d 'e' -f 1)"
  if [ "$x_n_lhs" -ne 1 ]; then
    echo "data_name $data_name must have '1' base in exponential notation for number of rows" >&2 && eit 1
  fi
  x_n_rhs="$(echo $x_n | cut -d "e" -f 2)"
  if [ "$x_n_rhs" -lt 6 ]; then
    echo "data_name $data_name must have exponent greater or equal to '6' in exponential notation for number of rows" >&2 && exit 1
  fi
  echo ${data_name/NA/"$x_n_lhs"e"$(($x_n_rhs-6))"} ${data_name/NA/"$x_n_lhs"e"$(($x_n_rhs-3))"} ${data_name/NA/"$x_n_lhs"e"$x_n_rhs"}
}
