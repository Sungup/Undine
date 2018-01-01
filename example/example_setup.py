#!/usr/bin/env python3

import os
import uuid

temp_file_dir = '../tmp/temp'
input_file_dir = '../tmp/input'
input_file_name = 'Input'
input_file_ext = 'in'
input_file_items = 10
input_file_lines = 10

# 1. Create temporary directory
#   - tmp/input
os.makedirs(temp_file_dir, mode=0o777, exist_ok=True)
os.makedirs(input_file_dir, mode=0o777, exist_ok=True)

# 2. Make input files
for id_ in range(0, input_file_items):
    filename = "{0}{2}.{1}".format(input_file_name, input_file_ext, id_)

    with open(os.path.join(input_file_dir, filename), 'w') as f_out:
        for _ in range(0, input_file_lines):
            f_out.write('{}\n'.format(str(uuid.uuid4())))
