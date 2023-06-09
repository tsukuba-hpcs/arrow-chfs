// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/filesystem/chfs.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/util_internal.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/value_parsing.h"
#include "arrow/util/windows_fixup.h"

#ifdef __cplusplus
extern "C" {
#include <chfs.h>
}
#endif
#include <cstdlib>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sstream>
#include <queue>


namespace arrow {

using internal::ErrnoFromStatus;
using internal::ParseValue;
using internal::Uri;

namespace fs {

using internal::GetAbstractPathParent;
using internal::MakeAbstractPathRelative;
using internal::RemoveLeadingSlash;

class CHFSInputStream : public arrow::io::InputStream {
  public:
    explicit CHFSInputStream(std::string path): path_(path) {}
  private:
    std::string path_;
};

class CHFSRandomAccessFile : public arrow::io::RandomAccessFile {
 public:
  CHFSRandomAccessFile() {};
};

void StatInfoToFileInfo(const struct stat info, FileInfo* out) {
  if (S_ISREG(info.st_mode)) {
    out->set_type(FileType::File);
    out->set_size(info.st_size);
  } else if (S_ISDIR(info.st_mode)) {
    out->set_type(FileType::Directory);
    out->set_size(kNoSize);
  }
  std::chrono::nanoseconds ns_count(static_cast<int64_t>(info.st_mtim.tv_sec) * 1000000000);
  out->set_mtime(TimePoint(std::chrono::duration_cast<TimePoint::duration>(ns_count)));
}

ConsistentHashFileSystem::ConsistentHashFileSystem(const io::IOContext& io_context)
    : FileSystem(io_context) {
}

ConsistentHashFileSystem::~ConsistentHashFileSystem() {}

bool ConsistentHashFileSystem::Equals(const FileSystem& other) const {
  if (other.type_name() != type_name()) {
    return false;
  }
  return true;
}

Result<FileInfo> ConsistentHashFileSystem::GetFileInfo(const std::string& path) {
  if (path.substr(0, 5) == "chfs:") {
    return Status::Invalid("GetFileInfo must not be passed a URI, got: ", path);
  }
  FileInfo info;
  struct stat sb;
  int ret;
  ret = chfs_stat(path.c_str(), &sb);
  if (ret != 0) {
    info.set_type(FileType::NotFound);
    return info;
  }
  StatInfoToFileInfo(sb, &info);
  return info;
}

struct ReadDirBuffer {
  std::string base_dir;
  std::vector<FileInfo> entries;
  std::queue<std::string> targets;
  std::queue<std::string> next_targets;
};

static int getfileinfo_filler(void *buf, const char *name, const struct stat *st, off_t off) {
  if (name[0] == '.' && (name[1] == '\0' || (name[1] == '.' && name[2] == '\0'))) {
    return 0;
  }
  ReadDirBuffer *rbuf = (ReadDirBuffer *)buf;
  std::string path;
  path += rbuf->base_dir;
  if (rbuf->base_dir != "/") {
    path += "/";
  }
  path += std::string(name);
  if (S_ISDIR(st->st_mode)) {
    FileInfo info(path, FileType::Directory);
    rbuf->entries.push_back(info);
    rbuf->next_targets.push(path);
  } else if (S_ISREG(st->st_mode)) {
    FileInfo info(path, FileType::File);
    rbuf->entries.push_back(info);
  }
  return 0;
}

Result<std::vector<FileInfo>> ConsistentHashFileSystem::GetFileInfo(const FileSelector& select) {
  FileInfo base;
  Status stat = GetFileInfo(select.base_dir).Value(&base);
  if (!stat.ok() || base.type() != FileType::Directory) {
    if (select.allow_not_found) {
      return std::vector<FileInfo>{};
    }
    return Status::Invalid("ConsistentHashFileSystem::GetFileInfo: path is not directory");
  }
  if (!select.recursive) {
    int ret;
    ReadDirBuffer buf;
    buf.base_dir = select.base_dir;
    ret = chfs_readdir(select.base_dir.c_str(), &buf, getfileinfo_filler);
    if (ret != 0) {
      return Status::Invalid("ConsistentHashFileSystem::GetFileInfo: chfs_readdir failed");
    }
    return buf.entries;
  }
  ReadDirBuffer buf;
  buf.next_targets.push(select.base_dir);
  for (int32_t depth = 0; depth < select.max_recursion && !buf.next_targets.empty(); depth++) {
    std::swap(buf.next_targets, buf.targets);
    while (!buf.targets.empty()) {
      std::string target = buf.targets.front();
      buf.targets.pop();
      int ret;
      buf.base_dir = target;
      ret = chfs_readdir(target.c_str(), &buf, getfileinfo_filler);
      if (ret != 0) {
        return Status::Invalid("ConsistentHashFileSystem::GetFileInfo: chfs_readdir failed");
      }
    }
  }
  return buf.entries;
}

Status ConsistentHashFileSystem::CreateDir(const std::string& path, bool recursive) {
  if (path.empty()) {
    return Status::Invalid("ConsistentHashFileSystem::CreateDir: path is empty");
  }

  if (!recursive) {
    FileInfo info;
    Status stat = GetFileInfo(path).Value(&info);
    if (!stat.ok()) {
      return Status::Invalid("GetFileInfo failed");
    }
    if (info.type() == FileType::File || info.type() == FileType::Unknown) {
      return Status::Invalid("path is not directory");
    }
    if (info.type() == FileType::Directory) {
      return Status::OK();
    }
    int ret;
    ret = chfs_mkdir(path.c_str(), S_IRWXU);
    if (ret == 0) {
      return Status::OK();
    }
    return Status::Invalid("CreateDir failed");
  }
  std::stringstream ss{path};
  std::string item;
  std::string prefix;
  std::getline(ss, item, '/');
  if (!item.empty()) {
    prefix += item;
    FileInfo info;
    Status stat = GetFileInfo(prefix).Value(&info);
    if (!stat.ok()) {
      return Status::Invalid("GetFileInfo failed");
    }
    if (info.type() == FileType::File || info.type() == FileType::Unknown) {
      return Status::Invalid("path is not directory");
    }
    if (info.type() == FileType::NotFound) {
      int ret;
      ret = chfs_mkdir(prefix.c_str(), S_IRWXU);
      if (ret != 0) {
        return Status::Invalid("CreateDir failed");
      }
    }
  }
  prefix += '/';
  while (std::getline(ss, item, '/')) {
    if (item.empty()) {
      return Status::Invalid("CreateDir: empty path");
    }
    prefix += item;
    FileInfo info;
    Status stat = GetFileInfo(prefix).Value(&info);
    if (!stat.ok()) {
      return Status::Invalid("GetFileInfo failed");
    }
    if (info.type() == FileType::File || info.type() == FileType::Unknown) {
      return Status::Invalid("path is not directory");
    }
    if (info.type() == FileType::NotFound) {
      int ret;
      ret = chfs_mkdir(prefix.c_str(), S_IRWXU);
      if (ret != 0) {
        return Status::Invalid("CreateDir failed");
      }
    }
    prefix += '/';
  }
  return Status::OK();
}

Status ConsistentHashFileSystem::DeleteDir(const std::string& path) {
  int ret;
  ret = chfs_rmdir(path.c_str());
  if (ret == 0) {
    return Status::OK();
  }
  return Status::Invalid("DeleteDir failed");
}

Status ConsistentHashFileSystem::DeleteDirContents(const std::string& path, bool missing_dir_ok) {
  int ret;
  ret = chfs_unlink(path.c_str());
  if (ret == 0) {
    return Status::OK();
  }
  return Status::Invalid("DeleteDirContents failed");
}

Status ConsistentHashFileSystem::DeleteRootDirContents() {
  return Status::Invalid("DeleteRootDirContents not implemented");
}

Status ConsistentHashFileSystem::DeleteFile(const std::string& path) {
  int ret;
  ret = chfs_unlink(path.c_str());
  if (ret == 0) {
    return Status::OK();
  }
  return Status::Invalid("DeleteFile failed");
}

Status ConsistentHashFileSystem::Move(const std::string& src, const std::string& dest) {
  int src_fd, dest_fd;
  src_fd = chfs_open(src.c_str(), O_RDWR);
  if (src_fd < 0) {
    return Status::Invalid("open failed");
  }
  dest_fd = chfs_create(dest.c_str(), O_RDWR, S_IRWXU);
  if (dest_fd < 0) {
    return Status::Invalid("create failed");
  }
  int bs = 65536;
  const char* bs_str = std::getenv("CHFS_CHUNK_SIZE");
  if (bs_str != NULL) {
    bs = std::stoi(bs_str);
  }
  int tsize;
  std::vector<char> buffer(bs);
  do {
    tsize = chfs_read(src_fd, &buffer[0], bs);
    if (tsize < 0) {
      return Status::Invalid("move: chfs_read failed");
    }
    tsize = chfs_write(dest_fd, &buffer[0], tsize);
    if (tsize < 0) {
      return Status::Invalid("move: chfs_write failed");
    }
  } while (tsize == bs);
  if (chfs_close(src_fd) < 0) {
    return Status::Invalid("move: chfs_close(src_fd) failed");
  }
  if (chfs_close(dest_fd) < 0) {
    return Status::Invalid("move: chfs_close(dest_fd) failed");
  }
  if (chfs_unlink(src.c_str()) < 0) {
    return Status::Invalid("move: chfs_unlink failed");
  }
  return Status::OK();
}

Status ConsistentHashFileSystem::CopyFile(const std::string& src, const std::string& dest) {
  return Status::Invalid("CopyFile not implemented");
}

class ARROW_EXPORT CHFSFile : public arrow::io::ReadWriteFileInterface {
public:
  int fd;
  bool chfs_closed;
  io::IOContext io_context_;
  CHFSFile(int fd, const io::IOContext& io_context): fd(fd), chfs_closed(false), io_context_(io_context) {
  }
  Status WriteAt(int64_t position, const void* data, int64_t nbytes) {
    int ret;
    ret = chfs_pwrite(fd, data, nbytes, position);
    if (ret < 0) {
      return Status::Invalid("WriteAt failed");
    }
    return Status::OK();
  }
  Result<int64_t> GetSize() {
    int64_t now, end;
    now = chfs_seek(fd, 0, SEEK_CUR);
    if (now < 0) {
      return Status::Invalid("SEEK_CUR failed");
    }
    end = chfs_seek(fd, 0, SEEK_END);
    if (end < 0) {
      return Status::Invalid("SEEK_END failed");
    }
    now = chfs_seek(fd, now, SEEK_SET);
    if (now < 0) {
      return Status::Invalid("SEEK_SET failed");
    }
    return end;
  }
  Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) {
    int ret;
    ret = chfs_pread(fd, out, nbytes, position);
    if (ret < 0) {
      return Status::Invalid("ReadAt failed");
    }
    return ret;
  }
  Result<std::shared_ptr<Buffer>> ReadAt(int64_t position, int64_t nbytes) {
    ARROW_ASSIGN_OR_RAISE(auto buf, arrow::AllocateResizableBuffer(nbytes, io_context_.pool()));
    int ret;
    ret = chfs_pread(fd, buf->mutable_data(), nbytes, position);
    if (ret < 0) {
      return Status::Invalid("ReadAt failed");
    }
    RETURN_NOT_OK(buf->Resize(ret));
    return std::move(buf);
  }
  Status Advance(int64_t nbytes) {
    int ret;
    ret = chfs_seek(fd, nbytes, SEEK_CUR);
    if (ret < 0) {
      return Status::Invalid("Advance failed");
    }
    return Status::OK();
  }
  Result<std::string_view> Peek(int64_t nbytes) {
    return Status::Invalid("Peek not implemented");
  }
  Result<int64_t> Read(int64_t nbytes, void* out) {
    int ret;
    ret = chfs_read(fd, out, nbytes);
    if (ret < 0) {
      return Status::Invalid("Read failed");
    }
    return ret;
  }
  Result<std::shared_ptr<Buffer>> Read(int64_t nbytes) {
    ARROW_ASSIGN_OR_RAISE(auto buf, arrow::AllocateResizableBuffer(nbytes, io_context_.pool()));
    int ret;
    ret = chfs_read(fd, buf->mutable_data(), nbytes);
    if (ret < 0) {
      return Status::Invalid("Read failed");
    }
    RETURN_NOT_OK(buf->Resize(ret));
    return buf;
  }
  Status Write(const void* data, int64_t nbytes) {
    int ret;
    ret = chfs_write(fd, data, nbytes);
    if (ret < 0) {
      return Status::Invalid("Write failed");
    }
    return Status::OK();
  }
  Status Write(const std::shared_ptr<Buffer>& data) {
    int ret;
    ret = chfs_write(fd, data->data(), data->size());
    if (ret < 0) {
      return Status::Invalid("Write failed");
    }
    return Status::OK();
  }
  Status Flush() {
    int ret;
    ret = chfs_fsync(fd);
    if (ret < 0) {
      return Status::Invalid("Flush failed");
    }
    return Status::OK();
  }
  Status Write(std::string_view data) {
    return Status::Invalid("Write not implemented");
  }
  Status Seek(int64_t position) {
    int ret;
    ret = chfs_seek(fd, position, SEEK_SET);
    if (ret < 0) {
      return Status::Invalid("Seek failed");
    }
    return Status::OK();
  }
  Status Close() {
    int ret;
    ret = chfs_close(fd);
    if (ret < 0) {
      return Status::Invalid("Close failed");
    }
    chfs_closed = true;
    return Status::OK();
  }
  Result<int64_t> Tell() const {
    int64_t ret;
    ret = chfs_seek(fd, 0, SEEK_CUR);
    if (ret < 0) {
      return Status::Invalid("Tell failed");
    }
    return ret;
  }
  bool closed() const {
    return chfs_closed;
  }
};

Result<std::shared_ptr<io::InputStream>> ConsistentHashFileSystem::OpenInputStream(
      const std::string& path) {
  int fd;
  fd = chfs_open(path.c_str(), O_RDWR);
  if (fd < 0) {
    return Status::Invalid("open failed");
  }
  std::shared_ptr<io::InputStream> file = std::shared_ptr<CHFSFile>(new CHFSFile(fd, io_context_));
  return file;
}

Result<std::shared_ptr<io::RandomAccessFile>> ConsistentHashFileSystem::OpenInputFile(
      const std::string& path) {
  int fd;
  fd = chfs_open(path.c_str(), O_RDWR);
  if (fd < 0) {
    return Status::Invalid("open failed");
  }
  std::shared_ptr<io::RandomAccessFile> file = std::shared_ptr<CHFSFile>(new CHFSFile(fd, io_context_));
  return file;
}

Result<std::shared_ptr<io::OutputStream>> ConsistentHashFileSystem::OpenOutputStream(
      const std::string& path,
      const std::shared_ptr<const KeyValueMetadata>& metadata) {
  int fd;
  fd = chfs_create(path.c_str(), O_RDWR, S_IRWXU);
  if (fd < 0) {
    return Status::Invalid("create failed");
  }
  std::shared_ptr<io::OutputStream> file = std::shared_ptr<CHFSFile>(new CHFSFile(fd, io_context_));
  return file;
}

Result<std::shared_ptr<io::OutputStream>> ConsistentHashFileSystem::OpenAppendStream(
      const std::string& path,
      const std::shared_ptr<const KeyValueMetadata>& metadata) {
  int fd;
  fd = chfs_open(path.c_str(), O_RDWR);
  if (fd < 0) {
    return Status::Invalid("open failed");
  }
  std::shared_ptr<io::OutputStream> file = std::shared_ptr<CHFSFile>(new CHFSFile(fd, io_context_));
  return file;
}

Result<std::shared_ptr<ConsistentHashFileSystem>> ConsistentHashFileSystem::Make(
    const io::IOContext& io_context) {
  std::shared_ptr<ConsistentHashFileSystem> ptr(new ConsistentHashFileSystem(io_context));
  return ptr;
}

Status InitializeCHFS(std::string server) {
  int ret;
  ret = chfs_init(server.c_str());
  if (ret == 0) {
    return Status::OK();
  }
  return Status::Invalid("InitializeCHFS failed");
}

Status FinalizeCHFS() {
  int ret;
  ret = chfs_term();
  if (ret == 0) {
    return Status::OK();
  }
  return Status::Invalid("FinalizeCHFS failed");
}

}
}
