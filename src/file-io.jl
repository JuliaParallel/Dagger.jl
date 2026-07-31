export save, load
using SparseArrays
import MemPool: GenericFileDevice, FileRef

struct File
    path::String
    chunk::Chunk
end

"""
    File(path::AbstractString;
         serialize::Base.Callable, deserialize::Base.Callable,
         use_io::Bool, mmap::Bool) -> Dagger.File

References data in the file at `path`, using the derialization function `deserialize`.
`use_io` specifies whether `deserialize` takes an `IO` (the default) or takes a
`String` path. `mmap` is experimental, and specifies whether `deserialize` can
use MemPool's `MMWrap` memory mapping functionality (default is `false`).

The file at `path` is not yet loaded when the call to `File` returns; passing
it to a task will cause reading to occur, and the result will be passed to the
task.
"""
function File(path::AbstractString;
              serialize::Base.Callable=serialize,
              deserialize::Base.Callable=deserialize,
              use_io::Bool=true,
              mmap::Bool=false)
    if !isfile(path)
        # FIXME: Once MemPool better propagates errors, this check won't be necessary
        throw(ArgumentError("`Dagger.File` expected file to exist at \"$path\""))
    end
    dir, file = dirname(path), basename(path)
    # if dir is empty, use the current working directory
    dir = isempty(dir) ? pwd() : dir
    Tdevice = GenericFileDevice{serialize, deserialize, use_io, mmap}
    device = Tdevice(dir)
    leaf_tag = MemPool.Tag(Tdevice=>file)
    chunk = Dagger.tochunk(FileRef(path), OSProc(), ProcessScope();
                           restore=true, retain=true, device, leaf_tag)
    return File(path, chunk)
end

"""
    tofile(data, path::AbstractString;
           serialize::Base.Callable, deserialize::Base.Callable,
           use_io::Bool, mmap::Bool) -> Dagger.File

Writes `data` to disk at `path`, using the serialization function `serialize`.
`use_io` specifies whether `serialize` takes an `IO` (the default) or takes a
`String` path. `mmap` is experimental, and specifies whether `serialize` can
use MemPool's `MMWrap` memory mapping functionality (default is `false`).

The returned `File` object can be passed to tasks as an argument, or returned
from tasks as a result.
"""
function tofile(@nospecialize(data), path::AbstractString;
                serialize::Base.Callable=serialize,
                deserialize::Base.Callable=deserialize,
                use_io::Bool=true,
                mmap::Bool=false)
    dir, file = dirname(path), basename(path)
    # if dir is empty, use the current working directory
    dir = isempty(dir) ? pwd() : dir
    Tdevice = GenericFileDevice{serialize, deserialize, use_io, mmap}
    device = Tdevice(dir)
    chunk = Dagger.tochunk(data, OSProc(), ProcessScope();
                           retain=true, device, tag=file)
    return File(path, chunk)
end
Base.fetch(file::File) = fetch(file.chunk)
Base.show(io::IO, file::File) = print(io, "Dagger.File(path=\"$(file.path)\")")

function move(from_proc::Processor, to_proc::Processor, file::File)
    return move(from_proc, to_proc, file.chunk)
end
