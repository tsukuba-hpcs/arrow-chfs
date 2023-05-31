# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# cython: language_level = 3

from pyarrow.lib cimport (check_status, pyarrow_wrap_metadata,
                          pyarrow_unwrap_metadata)
from pyarrow.lib import frombytes, tobytes, KeyValueMetadata
from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *
from pyarrow.includes.libarrow_fs cimport *
from pyarrow._fs cimport FileSystem
import os

def initialize_chfs():
    cdef c_string server = os.environ['CHFS_SERVER'].encode('utf-8')
    check_status(CInitializeCHFS(server))

def finalize_chfs():
    check_status(CFinalizeCHFS())

cdef class ConsistentHashFileSystem(FileSystem):
    cdef:
        CConsistentHashFileSystem* chfs
    def __init__(self, use_mmap=False):
        with nogil:
            wrapped = GetResultValue(CConsistentHashFileSystem.Make())
        self.init(<shared_ptr[CFileSystem]> wrapped)
    
    cdef init(self, const shared_ptr[CFileSystem]& wrapped):
        FileSystem.init(self, wrapped)
        self.chfs = <CConsistentHashFileSystem*> wrapped.get()
    
    @classmethod
    def _reconstruct(cls, kwargs):
        return cls(**kwargs)

    def __reduce__(self):
        return (ConsistentHashFileSystem._reconstruct, (dict(),))
    