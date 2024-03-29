function throughput_monitor(ctr, x)
    time_start = time_ns()

    Dagger.spawn(ctr, time_start) do ctr, time_start
        # Measure throughput
        elapsed_time_ns = time_ns() - time_start
        elapsed_time_s = elapsed_time_ns / 1e9
        elem_size = sizeof(x)
        throughput = (ctr[] * elem_size) / elapsed_time_s

        # Print measured throughput
        print("\e[1K\e[100D")
        print("Throughput: $(round(throughput; digits=3)) bytes/second")

        # Sleep for a bit
        sleep(0.1)
    end
    function measure_throughput(ctr, x)
        ctr[] += 1
        return x
    end

    return Dagger.@spawn measure_throughput(ctr, x)
end
