# Converts a "csv" file delimited with ASCII 31 (\u001f) to someting excel can import
# Excel requires printable delimiter, but it can be multiple characters long, so we pick somethinh
# hoepfully not ocurring in the data :)


# Use: ./delim_to_excel FILENAME < IN_FILE > OUT_FILE
DELIM="<<>>::<<>>"

echo "delimiter (copy without quotes when importing to excel): '$DELIM'" 
sed $'s@\u001f@$DELIM@g\'
