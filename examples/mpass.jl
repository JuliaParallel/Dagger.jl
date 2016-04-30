addprocs(2)

using ComputeFramework

@everywhere function transmit(id)
    for i in 1:10
        msg = ComputeFramework.Message(id, rand(1:10))
        ComputeFramework.send_message(id, msg)
        println("Proc $id sent $(msg.val)")
    end

    for i in 1:10
        msg = ComputeFramework.recv_message()
        println("Prod $id received $(msg.val)")
    end
end

register()
println(ComputeFramework._address_book)
dests = compute(Distribute(BlockPartition(1), [3,2]))

compute(map(transmit, dests))
