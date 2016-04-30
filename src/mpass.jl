export Message,register,send_message,recv_message

const BUFFER_LENGTH = 1000

type Message
    dest_proc::Int
    val::Any
end

const _mailbox = RemoteChannel(()->Channel{Message}(BUFFER_LENGTH), myid())
const _address_book = Dict{Int,RemoteChannel}()

get_address() = _mailbox

function set_address!(address_book)
    for (key, value) in address_book
        _address_book[key] = value
    end
end

""" Run only on the REPL proc """
function register()
    for pid in workers()
        _address_book[pid] = remotecall_fetch(get_address, pid)
    end
    for pid in workers()
        remotecall_fetch(set_address!, pid, _address_book)
    end
end

function send_message(pid, msg)
    put!(_address_book[pid], msg)
end

function recv_message()
    take!(_mailbox)
end
