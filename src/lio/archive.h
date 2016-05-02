/*
   Copyright 2016 Vanderbilt University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
#include "lio/lio_visibility.h"
#define ARCHIVE_TAPE_ATTRIBUTE "user.tapeid"

// concatenate two strings together
LIO_API char* concat(char *str1, char *str2);

// concatenate two paths together with added separator
LIO_API char* path_concat(char *str1, char *str2);
