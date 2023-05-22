module fastcopy;

import core.stdc.errno;
import std.file : PreserveAttributes, preserveAttributesDefault;

version(CRuntime_GLibc) version = assumeHaveCopyFileRange;

version(assumeHaveCopyFileRange)
{
    import core.sys.posix.sys.types;

    private enum COPY_FILE_RANGE
    {
        UNINITIALIZED,
        NOT_AVAILABLE,
        AVAILABLE,
    }

    private shared COPY_FILE_RANGE hasCopyFileRange = COPY_FILE_RANGE.UNINITIALIZED;

    alias copy_file_range_T = ssize_t function(int, off64_t, int, off64_t, size_t, uint) @nogc nothrow @trusted;
    static copy_file_range_T copy_file_range = null;

    private bool hasCopyFileRangeInGlibc() @nogc nothrow @trusted
    {
        extern (C) void initCopyFileRange() @nogc nothrow @trusted
        {
            import core.sys.posix.dlfcn;
            void* handle = dlopen(null, RTLD_LAZY);
            if (handle !is null)
                copy_file_range = cast(copy_file_range_T) dlsym(handle, "copy_file_range");
        }

        import core.sys.posix.pthread;
        static pthread_once_t initOnce = PTHREAD_ONCE_INIT;
        pthread_once(&initOnce, &initCopyFileRange);
        return copy_file_range !is null;
    }
}


/**
 *  Stolen from std.file.
 */
version (Posix)
private T cenforce(T)(T condition, scope const(char)[] name, scope const(char)* namez,
                      string file = __FILE__, size_t line = __LINE__)
@trusted
{
    if (condition)
        return condition;
    if (!name)
    {
        import core.stdc.string : strlen;
        auto len = namez ? strlen(namez) : 0;
        name = namez[0 .. len].idup;
    }
    import std.file : FileException;
    throw new FileException(name, .errno, file, line);
}

///
void fastcopy(string from, string to, PreserveAttributes preserve = preserveAttributesDefault)
    @trusted
{
    version (assumeHaveCopyFileRange)
    {
        fastcopyImpl(from, to, preserve);
    }
    else
    {
        import std.file : copy;
        copy(from, to, preserve);
    }
}

version(assumeHaveCopyFileRange)
private void fastcopyImpl(scope const(char)[] f, scope const(char)[] t,
                          PreserveAttributes preserve)
@trusted
{
    static import core.stdc.stdio;
    import core.sys.posix.fcntl;
    import core.sys.posix.sys.stat;
    import core.sys.posix.unistd;
    import core.sys.posix.utime;
    import core.atomic;
    import std.conv : octal;
    import std.string : toStringz;

    static COPY_FILE_RANGE p = atomicLoad(*cast(const shared COPY_FILE_RANGE*) &hasCopyFileRange);
    if (p == COPY_FILE_RANGE.UNINITIALIZED)
    {
        p = hasCopyFileRangeInGlibc()
            ? COPY_FILE_RANGE.AVAILABLE
            : COPY_FILE_RANGE.NOT_AVAILABLE;
        atomicStore(*cast(shared COPY_FILE_RANGE*) &hasCopyFileRange, p);
    }
    if (p == COPY_FILE_RANGE.NOT_AVAILABLE)
    {
        import std.file : copy;
        copy(f, t);
        return;
    }

    const(char)* fromz = f.toStringz;
    const(char)* toz = t.toStringz;

    immutable fdr = core.sys.posix.fcntl.open(fromz, O_RDONLY);
    cenforce(fdr != -1, f, fromz);
    scope(exit) core.sys.posix.unistd.close(fdr);

    stat_t statbufr = void;
    cenforce(fstat(fdr, &statbufr) == 0, t, toz);

    immutable fdw = core.sys.posix.fcntl.open(toz, O_CREAT | O_WRONLY, octal!666);
    cenforce(fdw != -1, t, toz);

    stat_t statbufw = void;
    {
        scope(failure) core.sys.posix.unistd.close(fdw);
        cenforce(fstat(fdw, &statbufw) == 0, t, toz);
        if (statbufr.st_dev == statbufw.st_dev && statbufr.st_ino == statbufw.st_ino)
        {
            import std.file : FileException;
            throw new FileException("Source and destination are the same file");
        }
    }

    if (!S_ISREG(statbufr.st_mode) || statbufr.st_size <= 0 ||
        !S_ISREG(statbufw.st_mode) || statbufw.st_size <= 0)
        goto fallbackGenericCopyImpl;

    scope(failure) core.stdc.stdio.remove(toz);
    {
        scope(failure) core.sys.posix.unistd.close(fdw);
        ulong maxLength = ulong.max;
        ulong written = 0;
        while (written < maxLength)
        {
            import std.algorithm : min;
            auto left = min(maxLength - written, size_t.max, 0x40_000_000 /* 1GiB */);
            auto result = copy_file_range(fdr, null, fdw, null, left, 0);
            if (result == -1)
            {
                switch (errno)
                {
                    case EOVERFLOW:
                        goto fallback;
                    case ENOSYS:
                    case EXDEV:
                    case EINVAL:
                    case EPERM:
                    case EOPNOTSUPP:
                    case EBADF:
                        // Try fallback if either:
                        // - Kernel version is < 4.5 (ENOSYS)
                        // - Files are mounted on different fs (EXDEV)
                        // - copy_file_range is broken in various ways on RHEL/CentOS 7 (EOPNOTSUPP)
                        // - copy_file_range file is immutable or syscall is blocked by seccomp (EPERM)
                        // - copy_file_range cannot be used with pipes or device nodes (EINVAL)
                        // - the writer fd was opened with O_APPEND (EBADF)
                        assert(written == 0);
                        goto fallbackGenericCopyImpl;
                    default:
                        import std.format : format;
                        throw new ErrnoException(format!"Copy from %s to %s"(f, t));
                }
            }
            if (result == 0 && written == 0)
            {
                // WORKAROUND: several kernel bugs where copy_file_range will fail to copy any bytes and
                // return 0 insteaf of an error.
                // https://lore.kernel.org/linux-fsdevel/20210126233840.GG4626@dread.disaster.area/T/#m05753578c7f7882f6e9ffe01f981bc223edef2b0
                atomicStore(*cast(shared COPY_FILE_RANGE*) &hasCopyFileRange, COPY_FILE_RANGE.NOT_AVAILABLE);
                goto fallbackGenericCopyImpl;
            }
            if (result == 0)
                break;
            written += result;
        }
    }
    if (preserve)
        cenforce(fchmod(fdw, to!mode_t(statbufr.st_mode)) == 0, f, fromz);

    cenforce(core.sys.posix.unistd.close(fdw) != -1, f, fromz);

    utimbuf utim = void;
    utim.actime = cast(time_t) statbufr.st_atime;
    utim.modtime = cast(time_t) statbufr.st_mtime;

    cenforce(utime(toz, &utim) != -1, f, fromz);
    return;

fallbackGenericCopyImpl:
    {
        scope(failure) core.sys.posix.unistd.close(fdw);
        cenforce(ftruncate(fdw, 0) == 0, t, toz);

        auto BUFSIZ = 4096u * 16;
        auto buf = core.stdc.stdlib.malloc(BUFSIZ);
        if (!buf)
        {
            BUFSIZ = 4096;
            buf = core.stdc.stdlib.malloc(BUFSIZ);
            if (!buf)
            {
                import core.exception : onOutOfMemoryError;
                onOutOfMemoryError();
            }
        }
        scope(exit) core.stdc.stdlib.free(buf);

        while (written < statbufr.st_size)
        {
            immutable toxfer = min(statbufr.st_size - written, BUFSIZE);
            cenforce(
                core.sys.posix.unistd.read(fdr, buf, toxfer) == toxfer
                && core.sys.posix.unistd.write(fdw, buf, toxfer) == toxfer,
                f, fromz);
            written += toxfer;
        }
        if (preserve)
            cenforce(fchmod(fdw, to!mode_t(statbufr.st_mode)) == 0, f, fromz);
    }

    cenforce(core.sys.posix.unistd.close(fdw) != -1, f, fromz);

    utimbuf utim = void;
    utim.actime = cast(time_t) statbufr.st_atime;
    utim.modtime = cast(time_t) statbufr.st_mtime;

    cenforce(utime(toz, &utim) != -1, f, fromz);
}

@trusted unittest
{
    import std.file;

    auto source = deleteme ~ "source";
    auto target = deleteme ~ "target";
    auto targetNonExistent = deleteme ~ "target2";

    scope(exit) source.remove, target.remove, targetNonExistent.remove;

    source.write("source");
    target.write("target");

    assert(target.readText == "target");

    source.fastcopy(target);
    assert(target.readText == "source");

    source.fastcopy(targetNonExistent);
    assert(targetNonExistent.readText == "source");
}
