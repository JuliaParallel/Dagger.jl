using Test
using Serialization
using MPIRPC
using MPIRPC: MsgHeader, MPIRRID, NULL_RRID, CallMsg, CallWaitMsg, RemoteDoMsg,
              ResultMsg, RPCProgressHaltMsg, encode_frame, decode_frame, MSG_BOUNDARY, ProtocolError,
              is_null

@testset "protocol / framing (no MPI)" begin
    @testset "MPIRRID identity" begin
        a = MPIRRID(Int32(2), UInt64(7))
        b = MPIRRID(Int32(2), UInt64(7))
        c = MPIRRID(Int32(3), UInt64(7))
        @test a == b
        @test hash(a) == hash(b)
        @test a != c
        @test is_null(NULL_RRID)
        @test !is_null(a)
    end

    @testset "MsgHeader defaults" begin
        @test is_null(MsgHeader().response_oid)
        @test is_null(MsgHeader().notify_oid)
        @test MsgHeader(MPIRRID(Int32(1), UInt64(2))).notify_oid == NULL_RRID
    end

    @testset "CallMsg roundtrip preserves Mode and args" begin
        h = MsgHeader(NULL_RRID, MPIRRID(Int32(0), UInt64(99)))
        m = CallMsg{:call_fetch}(+, (1, 2, 3), Pair{Symbol,Any}[:by => 10])
        buf = encode_frame(h, m)
        h2, m2, err = decode_frame(buf)
        @test err === nothing
        @test h2 == h
        @test m2 isa CallMsg{:call_fetch}
        @test m2.args == (1, 2, 3)
        @test m2.kwargs == Pair{Symbol,Any}[:by => 10]
    end

    @testset "ResultMsg roundtrip" begin
        h = MsgHeader(MPIRRID(Int32(0), UInt64(1)), NULL_RRID)
        m = ResultMsg([1.0, 2.0, 3.0])
        buf = encode_frame(h, m)
        h2, m2, err = decode_frame(buf)
        @test err === nothing
        @test h2 == h
        @test m2 isa ResultMsg
        @test m2.value == [1.0, 2.0, 3.0]
    end

    @testset "RemoteDoMsg has null OIDs" begin
        h = MsgHeader(NULL_RRID, NULL_RRID)
        m = RemoteDoMsg(println, ("hi",), Pair{Symbol,Any}[])
        buf = encode_frame(h, m)
        _, m2, err = decode_frame(buf)
        @test err === nothing
        @test m2 isa RemoteDoMsg
    end

    @testset "RPCProgressHaltMsg roundtrip" begin
        buf = encode_frame(MsgHeader(), RPCProgressHaltMsg())
        h2, m2, err = decode_frame(buf)
        @test err === nothing
        @test h2 == MsgHeader()
        @test m2 isa RPCProgressHaltMsg
    end

    @testset "fresh serializer per frame: state does not leak" begin
        # Two frames sharing a Vector{Int} should still round-trip identically;
        # if state leaked across frames, the second decode would resolve a
        # stale back-reference and crash.
        v = collect(1:10)
        b1 = encode_frame(MsgHeader(), ResultMsg(v))
        b2 = encode_frame(MsgHeader(), ResultMsg(v))
        _, m1, _ = decode_frame(b1)
        _, m2, _ = decode_frame(b2)
        @test m1.value == v
        @test m2.value == v
    end

    @testset "MSG_BOUNDARY mismatch is fail-fast" begin
        buf = encode_frame(MsgHeader(), ResultMsg(42))
        buf[end] ⊻= 0xFF  # corrupt last byte of the boundary
        _, _, err = decode_frame(buf)
        @test err isa ProtocolError
    end

    @testset "boundary missing entirely is fail-fast" begin
        buf = encode_frame(MsgHeader(), ResultMsg(42))
        truncated = buf[1:end - length(MSG_BOUNDARY) - 1]
        _, _, err = decode_frame(truncated)
        @test err !== nothing  # may be EOFError or ProtocolError, both acceptable
    end

    @testset "body deserialization error is captured, not thrown" begin
        hbuf = IOBuffer()
        s = Serializer(hbuf)
        serialize(s, MsgHeader())
        hbytes = take!(hbuf)
        garbage = UInt8[0xff for _ in 1:32]
        full = vcat(hbytes, garbage, MSG_BOUNDARY)
        h, m, err = decode_frame(full)
        @test h isa MsgHeader  # header parsed successfully
        @test err !== nothing
        @test m === nothing
    end

    @testset "run_work_thunk print_error mirrors Distributed" begin
        path, io = mktemp()
        close(io)
        open(path, "w") do f
            redirect_stderr(f) do
                v = MPIRPC.run_work_thunk(() -> error("MPIRPC_planned_thunk_failure"), 99; print_error=true)
                @test v isa MPIRemoteException
                @test v.rank == 99
            end
        end
        cap = read(path, String)
        @test occursin("MPIRPC_planned_thunk_failure", cap)
        rm(path)
    end
end
