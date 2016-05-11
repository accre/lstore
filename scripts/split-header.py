#!/usr/bin/env python

import re
import os
import os.path
import sys

for filename in sys.argv[1:]:
    api_strings = []
    old_strings = []
    old_guard = False
    type_strings = []
    with open(filename, 'r') as in_fd:
        for line in in_fd.readlines():
            line = line.rstrip()
            if line.startswith('TBX_API'):
                api_strings.append(line)
            elif line.startswith('typedef struct'):
                type_strings.append(line)
            else:
                old_strings.append(line)
    # Sort functions alphabetically
    api_strings = sorted(api_strings,
                            key=lambda x: ' '.join(x.split()[2:]).replace('*', '').replace('(', ' '))
    type_strings = sorted(type_strings,
                            key=lambda x: ' '.join(x.split()[2:]).replace('*', '').replace('(', ' '))


    with open(os.path.join('tbx', filename), 'w') as new_fd:
        guard_name = "ACCRE_%s_INCLUDED" % filename.replace('.','_').upper()
        file_info = {'guard_name' : guard_name}
        new_fd.write("""// See LICENSE.txt at http://lstore.org 
// (c) 2016 Vanderbilt University, All Rights Reserved
#pragma once
#ifndef %(guard_name)s
#define %(guard_name)s

#ifdef __cplusplus
extern "C" {
#endif

#include "tbx/tbx_visibility.h"

// Types
""" %file_info)
        for type_line in type_strings:
            new_fd.write(type_line + "\n\n")
        new_fd.write(""" // Functions
""" % file_info)
        for api_line in api_strings:
            new_fd.write(api_line + "\n\n")
        new_fd.write("""
#ifdef __cplusplus
}
#endif

#endif
""" % file_info)


    with open(filename, 'w') as old_fd:
        for line in old_strings:
            old_fd.write("%s\n" % line)
        pass
