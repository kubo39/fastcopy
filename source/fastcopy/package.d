module fastcopy;

import core.stdc.errno;

version(linux)
{
    private enum COPY_FILE_RANGE
    {
        UNINITIALIZED,
        NOT_AVAILABLE,
        AVAILABLE,
    }

    private __gshared COPY_FILE_RANGE hasCopyFileRange = COPY_FILE_RANGE.UNINITIALIZED;
    private import core.sys.posix.sys.utsname : utsname;

    private extern (C) int uname(scope utsname* __name) @nogc nothrow;

    private bool initCopyFileRange() @nogc nothrow
    {
        import core.stdc.string : strtok;
        import core.stdc.stdlib : atoi;

        utsname uts;
        uname(&uts);
        char* p = uts.release.ptr;

        auto token = strtok(p, ".");
        int major = atoi(token);
        if (major > 4) return true;
        if (major == 4)
        {
            token = strtok(p, ".");
            if (atoi(token) >= 5) return true;
        }
        return false;
    }

    private extern (C) int syscall(size_t ident, size_t n, size_t arg1, size_t arg2, size_t arg3, size_t arg4, size_t arg5) @nogc nothrow;

    immutable size_t __NR_COPY_FILE_RANGE;
}


/**
 *  Stallen from std.file.
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


void fastcopy(string from, string to)
    @trusted
{
    version (linux)
    {
        import std.string : toStringz;
        auto fromz = from.toStringz();
        auto toz = to.toStringz();
        fastcopyImpl(from, to, fromz, toz);
    }
    else
    {
        import std.file : copy;
        retun copy(from, to);
    }
}

version(linux)
private void fastcopyImpl(scope const(char)[] f, scope const(char)[] t,
                          scope const(char)* fromz, scope const(char)* toz)
@trusted
{
    static import core.stdc.stdio;
    import core.sys.posix.fcntl;
    import core.sys.posix.sys.stat;
    import core.sys.posix.unistd;
    import core.sys.posix.utime;
    import core.atomic;
    import std.conv : octal;

    auto p = atomicLoad(*cast(const shared COPY_FILE_RANGE*) &hasCopyFileRange);
    if (p == COPY_FILE_RANGE.UNINITIALIZED)
    {
        p = initCopyFileRange() ? COPY_FILE_RANGE.AVAILABLE
            : COPY_FILE_RANGE.NOT_AVAILABLE;
        atomicStore(*cast(shared COPY_FILE_RANGE*) &hasCopyFileRange, p);
    }
    if (p == COPY_FILE_RANGE.NOT_AVAILABLE)
    {
        import std.file : copy;
        copy(f, t);
    }

    int copy_file_range(
        int fd_in,
        int* off_in,
        int fd_out,
        int* off_out,
        size_t len,
        uint flags
        ) @nogc nothrow
    {
        return syscall(__NR_COPY_FILE_RANGE,
                       fd_in, cast(size_t) off_in,
                       fd_out, cast(size_t) off_out,
                       len, flags);
    }

    immutable fdr = core.sys.posix.fcntl.open(fromz, O_RDONLY);
    cenforce(fdr != -1, f, fromz);
    scope(exit) core.sys.posix.unistd.close(fdr);

    stat_t statbufr = void;
    cenforce(fstat(fdr, &statbufr) == 0, t, toz);

    immutable fdw = core.sys.posix.fcntl.open(toz,
                                              O_CREAT | O_WRONLY,
                                              octal!666);
    cenforce(fdw != -1, t, toz);
    {
        scope(failure) core.sys.posix.unistd.close(fdw);
        stat_t statbufw = void;
        cenforce(fstat(fdw, &statbufw) == 0, t, toz);
        if (statbufr.st_dev == statbufw.st_dev && statbufr.st_ino == statbufw.st_ino)
        {
            import std.file : FileException;
            throw new FileException("Source and destination are the same file");
        }
    }

    scope(failure) core.stdc.stdio.remove(toz);
    {
        scope(failure) core.sys.posix.unistd.close(fdw);
        ulong len = statbufr.st_size;
        ulong written = 0;
        while (written < len)
        {
            import std.algorithm : min;
            auto left = min(len - written, size_t.max);
            auto result = copy_file_range(fdr, null,
                                          fdw, null,
                                          left, 0);
            written += result;
        }
    }

    cenforce(core.sys.posix.unistd.close(fdw) != -1, f, fromz);

    utimbuf utim = void;
    utim.actime = cast(time_t) statbufr.st_atime;
    utim.modtime = cast(time_t) statbufr.st_mtime;

    cenforce(utime(toz, &utim) != -1, f, fromz);
}

unittest
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
