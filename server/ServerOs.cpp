// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "ServerOs.h"

#include <cerrno>
#include <cstdio>
#include <cstring>

#if defined(_WIN32)
    #include <windows.h>
    #include <bcrypt.h>
    #include <direct.h>
    #include <fcntl.h>
    #include <io.h>
    #include <sys/stat.h>
#else
    #include <dirent.h>
    #include <fcntl.h>
    #include <sys/stat.h>
    #include <unistd.h>
    #if defined(__linux__)
        #include <sys/random.h>
    #endif
#endif

namespace server_os {

std::string lastError() {
#if defined(_WIN32)
    const DWORD code = GetLastError();
    if (code != 0) {
        char text[256] = {0};
        FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, nullptr, code, 0, text,
                       sizeof text, nullptr);
        std::string out = text;
        while (!out.empty() && (out.back() == '\n' || out.back() == '\r' || out.back() == '.')) out.pop_back();
        if (!out.empty()) return out;
    }
#endif
    return std::strerror(errno);
}

bool replaceFile(const std::string& from, const std::string& to) {
#if defined(_WIN32)
    // rename() on Windows refuses an existing target; this replaces it, and
    // does not return until it is on the disk.
    return MoveFileExA(from.c_str(), to.c_str(), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH) != 0;
#else
    return std::rename(from.c_str(), to.c_str()) == 0;
#endif
}

bool writeFileAtomically(const std::string& path, const std::string& data, std::string& error, int mode) {
    const std::string temporary = path + ".new";
#if defined(_WIN32)
    (void)mode;
    const int fd = _open(temporary.c_str(), _O_WRONLY | _O_CREAT | _O_TRUNC | _O_BINARY, _S_IREAD | _S_IWRITE);
#else
    std::remove(temporary.c_str());          // so the mode below is the one it gets
    const int fd = ::open(temporary.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, (mode_t)mode);
#endif
    if (fd < 0) {
        error = lastError();
        return false;
    }
    size_t written = 0;
    while (written < data.size()) {
#if defined(_WIN32)
        const int n = _write(fd, data.data() + written, (unsigned)(data.size() - written));
#else
        const ssize_t n = ::write(fd, data.data() + written, data.size() - written);
        if (n < 0 && errno == EINTR) continue;
#endif
        if (n <= 0) break;
        written += (size_t)n;
    }
#if defined(_WIN32)
    bool ok = written == data.size() && _commit(fd) == 0;
    ok = _close(fd) == 0 && ok;
#else
    bool ok = written == data.size() && ::fsync(fd) == 0;
    ok = ::close(fd) == 0 && ok;
#endif
    if (!ok || !replaceFile(temporary, path)) {
        error = lastError();
        std::remove(temporary.c_str());
        return false;
    }
    return true;
}

bool canWrite(const std::string& path, std::string& error) {
    std::FILE* probe = std::fopen(path.c_str(), "wb");
    if (probe == nullptr) {
        error = lastError();
        return false;
    }
    std::fclose(probe);
    std::remove(path.c_str());
    return true;
}

bool makeDirectory(const std::string& path, int mode) {
#if defined(_WIN32)
    (void)mode;
    if (_mkdir(path.c_str()) == 0) return true;
    struct _stat info;
    return _stat(path.c_str(), &info) == 0 && (info.st_mode & _S_IFDIR) != 0;
#else
    if (::mkdir(path.c_str(), (mode_t)mode) == 0) return true;
    struct stat info;
    return errno == EEXIST && ::stat(path.c_str(), &info) == 0 && S_ISDIR(info.st_mode);
#endif
}

std::vector<std::string> listDirectory(const std::string& path) {
    std::vector<std::string> names;
#if defined(_WIN32)
    WIN32_FIND_DATAA found;
    const HANDLE search = FindFirstFileA((path + "\\*").c_str(), &found);
    if (search == INVALID_HANDLE_VALUE) return names;
    do {
        const std::string name = found.cFileName;
        if (name != "." && name != "..") names.push_back(name);
    } while (FindNextFileA(search, &found));
    FindClose(search);
#else
    if (DIR* dir = ::opendir(path.c_str())) {
        while (dirent* entry = ::readdir(dir)) {
            const std::string name = entry->d_name;
            if (name != "." && name != "..") names.push_back(name);
        }
        ::closedir(dir);
    }
#endif
    return names;
}

bool randomBytes(uint8_t* out, size_t length) {
#if defined(_WIN32)
    return BCryptGenRandom(nullptr, out, (ULONG)length, BCRYPT_USE_SYSTEM_PREFERRED_RNG) == 0;
#elif defined(__linux__)
    size_t got = 0;
    while (got < length) {
        const ssize_t n = getrandom(out + got, length - got, 0);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return false;
        got += (size_t)n;
    }
    return true;
#else
    arc4random_buf(out, length);
    return true;
#endif
}

std::tm utcTime(std::time_t when) {
    std::tm out;
#if defined(_WIN32)
    gmtime_s(&out, &when);
#else
    gmtime_r(&when, &out);
#endif
    return out;
}

} // namespace server_os
