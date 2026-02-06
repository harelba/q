
import os
from test.utils import b

EXAMPLES = os.path.abspath(os.path.join(os.getcwd(), 'examples'))


uneven_ls_output = b("""drwxr-xr-x   2 root     root      4096 Jun 11  2012 /selinux
drwxr-xr-x   2 root     root      4096 Apr 19  2013 /mnt
drwxr-xr-x   2 root     root      4096 Apr 24  2013 /srv
drwx------   2 root     root     16384 Jun 21  2013 /lost+found
lrwxrwxrwx   1 root     root        33 Jun 21  2013 /initrd.img.old -> /boot/initrd.img-3.8.0-19-generic
drwxr-xr-x   2 root     root      4096 Jun 21  2013 /cdrom
drwxr-xr-x   3 root     root      4096 Jun 21  2013 /home
lrwxrwxrwx   1 root     root        29 Jun 21  2013 /vmlinuz -> boot/vmlinuz-3.8.0-19-generic
lrwxrwxrwx   1 root     root        32 Jun 21  2013 /initrd.img -> boot/initrd.img-3.8.0-19-generic
""")


find_output = b("""8257537   32 drwxrwxrwt 218 root     root        28672 Mar  1 11:00 /tmp
8299123    4 drwxrwxr-x   2 harel    harel        4096 Feb 27 10:06 /tmp/1628a3fd-b9fe-4dd1-bcdc-7eb869fe7461/supervisor/stormdist/testTopology3fad644a-54c0-4def-b19e-77ca97941595-1-1393513576
8263229  964 -rw-rw-r--   1 mapred   mapred      984569 Feb 27 10:06 /tmp/1628a3fd-b9fe-4dd1-bcdc-7eb869fe7461/supervisor/stormdist/testTopology3fad644a-54c0-4def-b19e-77ca97941595-1-1393513576/stormcode.ser
8263230    4 -rw-rw-r--   1 harel    harel        1223 Feb 27 10:06 /tmp/1628a3fd-b9fe-4dd1-bcdc-7eb869fe7461/supervisor/stormdist/testTopology3fad644a-54c0-4def-b19e-77ca97941595-1-1393513576/stormconf.ser
8299113    4 drwxrwxr-x   2 harel    harel        4096 Feb 27 10:16 /tmp/1628a3fd-b9fe-4dd1-bcdc-7eb869fe7461/supervisor/localstate
8263406    4 -rw-rw-r--   1 harel    harel        2002 Feb 27 10:16 /tmp/1628a3fd-b9fe-4dd1-bcdc-7eb869fe7461/supervisor/localstate/1393514168746
8263476    0 -rw-rw-r--   1 harel    harel           0 Feb 27 10:16 /tmp/1628a3fd-b9fe-4dd1-bcdc-7eb869fe7461/supervisor/localstate/1393514168746.version
8263607    0 -rw-rw-r--   1 harel    harel           0 Feb 27 10:16 /tmp/1628a3fd-b9fe-4dd1-bcdc-7eb869fe7461/supervisor/localstate/1393514169735.version
8263533    0 -rw-rw-r--   1 harel    harel           0 Feb 27 10:16 /tmp/1628a3fd-b9fe-4dd1-bcdc-7eb869fe7461/supervisor/localstate/1393514172733.version
8263604    0 -rw-rw-r--   1 harel    harel           0 Feb 27 10:16 /tmp/1628a3fd-b9fe-4dd1-bcdc-7eb869fe7461/supervisor/localstate/1393514175754.version
""")


header_row = b('name,value1,value2')
sample_data_rows = [b('a,1,0'), b('b,2,0'), b('c,,0')]
sample_data_rows_with_empty_string = [b('a,aaa,0'), b('b,bbb,0'), b('c,,0')]
sample_data_no_header = b("\n").join(sample_data_rows) + b("\n")
sample_data_with_empty_string_no_header = b("\n").join(
    sample_data_rows_with_empty_string) + b("\n")
sample_data_with_header = header_row + b("\n") + sample_data_no_header
sample_data_with_missing_header_names = b("name,value1\n") + sample_data_no_header

def generate_sample_data_with_header(header):
    return header + b("\n") + sample_data_no_header

sample_quoted_data = b('''non_quoted regular_double_quoted double_double_quoted escaped_double_quoted multiline_double_double_quoted multiline_escaped_double_quoted
control-value-1 "control-value-2" control-value-3 "control-value-4" control-value-5 "control-value-6"
non-quoted-value "this is a quoted value" "this is a ""double double"" quoted value" "this is an escaped \\"quoted value\\"" "this is a double double quoted ""multiline
  value""." "this is an escaped \\"multiline
  value\\"."
control-value-1 "control-value-2" control-value-3 "control-value-4" control-value-5 "control-value-6"
''')

double_double_quoted_data = b('''regular_double_quoted double_double_quoted
"this is a quoted value" "this is a quoted value with ""double double quotes"""
''')

escaped_double_quoted_data = b('''regular_double_quoted escaped_double_quoted
"this is a quoted value" "this is a quoted value with \\"escaped double quotes\\""
''')

combined_quoted_data = b('''regular_double_quoted double_double_quoted escaped_double_quoted
"this is a quoted value" "this is a quoted value with ""double double quotes""" "this is a quoted value with \\"escaped double quotes\\""
''')

sample_quoted_data2 = b('"quoted data" 23\nunquoted-data 54')

sample_quoted_data2_with_newline = b('"quoted data with\na new line inside it":23\nunquoted-data:54')

one_column_data = b('''data without commas 1
data without commas 2
''')

# Values with leading whitespace
sample_data_rows_with_spaces = [b('a,1,0'), b('   b,   2,0'), b('c,,0')]
sample_data_with_spaces_no_header = b("\n").join(
    sample_data_rows_with_spaces) + b("\n")

header_row_with_spaces = b('name,value 1,value2')
sample_data_with_spaces_with_header = header_row_with_spaces + \
    b("\n") + sample_data_with_spaces_no_header

long_value1 = "23683289372328372328373"
int_value = "2328372328373"
sample_data_with_long_values = "%s\n%s\n%s" % (long_value1,int_value,int_value)

